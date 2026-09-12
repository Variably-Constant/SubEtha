/*
 * Workloads through the C ABI, each the shape of a program that uses the
 * Rust API in production, replayed in C so the ABI is exercised the way
 * its consumers exercise the library: several processes on one backing,
 * frames past the slot, the waiting forms under contention, and long
 * runs. Every function returns the number of problems it found and
 * fills a `subetha_workload_stats` with what it measured; the Rust
 * harness spawns the processes, chooses the counts and records the
 * numbers.
 *
 * The workloads:
 *   rr     - a request/response service: one server process drains a
 *            request ring, several client processes each own a response
 *            ring; requests and responses are frames with a correlation
 *            id, a wide reply is chunked into partial frames.
 *   fleet  - a host and N worker processes on 2N single-producer
 *            single-consumer rings with large frames; the host offers
 *            snapshots without waiting, workers answer with findings, a
 *            zero-length frame is the shutdown sentinel.
 *   race   - T threads on one anonymous ring, every one a producer and a
 *            consumer, publishing small frames and draining to empty.
 *   log    - a chunked line log on a single-producer single-consumer
 *            ring: 64-byte slots carrying a start frame with the total
 *            length and continuation frames, a writer that drains to
 *            empty and sleeps between passes.
 *   bus    - a command ring and one reply ring per asking process, the
 *            reply matched by ring identity, rows past the inline budget.
 *   menu   - one direction over a file-backed single-producer ring with
 *            a begin/chunk/show protocol and a two-second push wait.
 *   pods   - three anonymous single-producer rings whose producers are
 *            serialized by the caller and whose consumer waits without a
 *            deadline.
 *   mind   - two anonymous rings between a foreground and a background
 *            thread carrying fixed-size verdicts and variable notes.
 *   deque  - one file-backed deque per producer with every consumer
 *            stealing from all of them, both sides spinning.
 *   blob   - a content-addressed blob store: a hash map from a content
 *            hash to an arena reference, several writers storing
 *            overlapping sets, readers on the arena read-only.
 *   index  - a content index rebuilt generation by generation under an
 *            owner lease: a vec of records over an arena, reset at a
 *            fresh path, flushed, published through a counter, read
 *            read-only.
 *   mvcc   - a versioned map changed by writers while readers scan it
 *            under a pin from the shared epoch table.
 *   graph  - a graph store on a frame region: chains of edge pages with
 *            a version word per page, writers appending and pruning,
 *            readers walking.
 *   memory - a record store: ids in a strategy-switching set and records
 *            in a hash map, both under one reader-writer lock.
 *   stream - a cluster stream: sealed Sens-O-Matic streams from several
 *            sender processes into one receiver, more than one per
 *            process.
 */

#include "subetha.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
/* Carried by Windows 10 1803 and later; named here so an SDK header
 * without it still builds. */
#ifndef CREATE_WAITABLE_TIMER_HIGH_RESOLUTION
#define CREATE_WAITABLE_TIMER_HIGH_RESOLUTION 0x00000002
#endif
#else
#include <time.h>
#include <unistd.h>
#endif

/* What a workload measured. */
typedef struct subetha_workload_stats {
    uint64_t items;     /* requests served, frames received, lines written */
    uint64_t bytes;     /* payload bytes moved */
    uint64_t refusals;  /* pushes refused for a full ring, offers dropped */
    uint64_t retries;   /* waits that ended in a timeout and were retried */
    uint64_t elapsed_ns;
    uint64_t worst_ns;  /* the slowest round trip or wait */
    uint64_t total_ns;  /* round trips summed, for the mean */
} subetha_workload_stats;

static uint64_t now_ns(void)
{
#if defined(_WIN32)
    LARGE_INTEGER f, t;
    QueryPerformanceFrequency(&f);
    QueryPerformanceCounter(&t);
    return (uint64_t)((double)t.QuadPart * 1e9 / (double)f.QuadPart);
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
#endif
}

/* A wait on a high-resolution waitable timer rather than on Sleep,
 * whose granularity is the scheduler tick: at the default tick a host
 * charges about eleven milliseconds for any wait of a millisecond or
 * less, so a poll asked for in microseconds costs tens of times what
 * it asks, and a loop bounded by a count of polls spans far longer
 * than the count was chosen for. The timer is honored to about half a
 * millisecond. Sleep is the fallback where the flag is unsupported. */
static void sleep_us(uint32_t us)
{
#if defined(_WIN32)
    HANDLE timer = CreateWaitableTimerExW(NULL, NULL, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
    if (timer != NULL) {
        LARGE_INTEGER due;
        /* A negative due time is relative, in hundreds of nanoseconds. */
        due.QuadPart = -((LONGLONG)us * 10);
        if (SetWaitableTimer(timer, &due, 0, NULL, NULL, FALSE)) {
            WaitForSingleObject(timer, INFINITE);
            CloseHandle(timer);
            return;
        }
        CloseHandle(timer);
    }
    Sleep(us / 1000 + (us % 1000 != 0));
#else
    struct timespec ts;
    ts.tv_sec = us / 1000000;
    ts.tv_nsec = (long)(us % 1000000) * 1000;
    nanosleep(&ts, NULL);
#endif
}

/* A pause measured on the monotonic clock rather than slept, since a
 * sleep of a few microseconds rounds up to the scheduler's tick. */
static void spin_us(uint32_t us)
{
    uint64_t until = now_ns() + (uint64_t)us * 1000ull;
    while (now_ns() < until) {
    }
}

static void wl_detail(const char *who, const char *what, int32_t rc)
{
    char detail[512];
    size_t needed = subetha_last_error_detail(detail, sizeof detail);
    fprintf(stderr, "  %s: %s -> %d (%s)%s%s\n", who, what, (int)rc, subetha_strerror(rc),
            needed > 1 ? "; " : "", needed > 1 ? detail : "");
}

static void note_round_trip(subetha_workload_stats *s, uint64_t ns)
{
    s->total_ns += ns;
    if (ns > s->worst_ns) {
        s->worst_ns = ns;
    }
}

/* Locales the ring workloads run in: `name` is a file prefix or a
 * shared-memory name. */
#define WL_LOCALE_FILE 0
#define WL_LOCALE_SHM_SESSION 1
#define WL_LOCALE_SHM_MACHINE 2

static int32_t ring_create(uint32_t locale, const char *name, uint32_t mp, uint32_t mc, uint32_t cap,
                           const subetha_ring_options *opt, subetha_handle *out)
{
    switch (locale) {
    case WL_LOCALE_FILE:
        return subetha_ring_create(name, mp, mc, cap, opt, out);
    case WL_LOCALE_SHM_SESSION:
        return subetha_ring_create_shm(name, mp, mc, cap, SUBETHA_SHM_SESSION, opt, out);
    default:
        return subetha_ring_create_shm(name, mp, mc, cap, SUBETHA_SHM_MACHINE, opt, out);
    }
}

static int32_t ring_open(uint32_t locale, const char *name, uint32_t mp, uint32_t mc, uint32_t cap,
                         const subetha_ring_options *opt, subetha_handle *out)
{
    switch (locale) {
    case WL_LOCALE_FILE:
        return subetha_ring_open(name, mp, mc, cap, opt, out);
    case WL_LOCALE_SHM_SESSION:
        return subetha_ring_open_shm(name, mp, mc, cap, SUBETHA_SHM_SESSION, opt, out);
    default:
        return subetha_ring_open_shm(name, mp, mc, cap, SUBETHA_SHM_MACHINE, opt, out);
    }
}

static int32_t ring_unlink(uint32_t locale, const char *name, uint32_t mp, subetha_unlink_report *report)
{
    switch (locale) {
    case WL_LOCALE_FILE:
        return subetha_ring_unlink(name, mp, report);
    case WL_LOCALE_SHM_SESSION:
        return subetha_ring_unlink_shm(name, SUBETHA_SHM_SESSION, report);
    default:
        return subetha_ring_unlink_shm(name, SUBETHA_SHM_MACHINE, report);
    }
}

static subetha_ring_options ring_options(uint32_t mode, uint64_t frame_block, uint32_t frame_blocks, const char *sddl)
{
    /* Named fields, not positions: the struct gains fields between
     * releases, and a positional list silently slides every value one
     * place along when it does. */
    subetha_ring_options opt = {
        .mode = mode,
        .stamps = SUBETHA_STAMPS_NONE,
        .frame_block = frame_block,
        .frame_blocks = frame_blocks,
        .shm_sddl = sddl,
    };
    if (mode == SUBETHA_MODE_MANAGED) {
        /* The library names no default and refuses managed mode without a
         * cadence, so this value is the harness's choice. SUBETHA_FFI_SCAN_US
         * overrides it, which is what lets a sweep vary the cadence across
         * the multi-process workloads while everything else stays fixed. */
        const char *scan = getenv("SUBETHA_FFI_SCAN_US");
        opt.scan_interval_us = 1000;
        if (scan != NULL && scan[0] != '\0') {
            char *end = NULL;
            unsigned long long v = strtoull(scan, &end, 10);
            if (end == NULL || *end != '\0' || v == 0ULL) {
                fprintf(stderr, "SUBETHA_FFI_SCAN_US must be a non-zero number of microseconds, got \"%s\"\n", scan);
                exit(2);
            }
            opt.scan_interval_us = (uint64_t)v;
        }
    }
    return opt;
}

static void put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static uint32_t get_u32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* ---------------------------------------------------------------- rr --
 * Request frame: [corr u32][client u32][kind u8][payload]. Response
 * frame: [corr u32][status u8][payload], status `FINAL` 0 / `PARTIAL` 1 /
 * `ERR` 2. A `PING` carries a short payload echoed back; a `WIDE` asks for
 * RR_WIDE_REPLY bytes, which the server chunks at RR_CHUNK into partial
 * frames and a final one. Geometry: capacity 1024, four peers hinted on
 * each side, 256 KiB frame blocks, 16 of them. */
#define RR_CAPACITY 1024
#define RR_PEERS 4
#define RR_FRAME_BLOCK (256u * 1024u)
#define RR_FRAME_BLOCKS 16
#define RR_CHUNK (200u * 1024u)
#define RR_WIDE_REPLY (230u * 1024u)
#define RR_KIND_PING 1
#define RR_KIND_WIDE 2
#define RR_FINAL 0
#define RR_PARTIAL 1
#define RR_WAIT_MS 10000

static void rr_name(char *out, size_t cap, const char *base, const char *suffix, uint32_t index)
{
    snprintf(out, cap, "%s_%s%u", base, suffix, (unsigned)index);
}

/* Serve `clients * requests_per_client` requests, then tear everything
 * down and unlink. `sddl` is null or the descriptor for the regions. */
int subetha_workload_rr_serve(uint32_t locale, const char *base, const char *sddl, uint32_t mode, uint32_t clients,
                              uint32_t requests_per_client, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_ring_options opt = ring_options(mode, RR_FRAME_BLOCK, RR_FRAME_BLOCKS, sddl);
    char req_name[512];
    rr_name(req_name, sizeof req_name, base, "req", 0);
    subetha_handle req = SUBETHA_HANDLE_NONE;
    int32_t rc = ring_create(locale, req_name, RR_PEERS, RR_PEERS, RR_CAPACITY, &opt, &req);
    if (rc != SUBETHA_OK) {
        wl_detail("rr server", "create request ring", rc);
        fprintf(stdout, "RR-SERVER-FAILED %d\n", (int)rc);
        fflush(stdout);
        return 1;
    }
    subetha_handle *resp = calloc(clients, sizeof *resp);
    uint32_t *resp_pid = calloc(clients, sizeof *resp_pid);
    if (resp == NULL || resp_pid == NULL) {
        fprintf(stderr, "  rr server: no memory for %u response rings\n", (unsigned)clients);
        subetha_handle_destroy(req);
        free(resp);
        free(resp_pid);
        return 1;
    }
    for (uint32_t i = 0; i < clients; i++) {
        char name[512];
        rr_name(name, sizeof name, base, "resp", i);
        rc = ring_create(locale, name, RR_PEERS, RR_PEERS, RR_CAPACITY, &opt, &resp[i]);
        if (rc != SUBETHA_OK) {
            wl_detail("rr server", "create response ring", rc);
            problems++;
        } else if ((rc = subetha_ring_register_producer(resp[i], &resp_pid[i])) != SUBETHA_OK) {
            wl_detail("rr server", "register on response ring", rc);
            problems++;
        }
    }
    uint32_t cid = 0;
    rc = subetha_ring_register_consumer(req, &cid);
    if (rc != SUBETHA_OK) {
        wl_detail("rr server", "register consumer", rc);
        problems++;
    }
    /* The ready marker: the harness waits for this line before it
     * spawns the clients. */
    fprintf(stdout, "RR-SERVER-READY\n");
    fflush(stdout);

    static uint8_t frame[RR_FRAME_BLOCK];
    static uint8_t reply[RR_FRAME_BLOCK];
    uint64_t expected = (uint64_t)clients * requests_per_client;
    uint64_t start = now_ns();
    while (problems == 0 && out->items < expected) {
        size_t len = 0;
        rc = subetha_ring_recv_frame_wait(req, cid, frame, sizeof frame, &len, RR_WAIT_MS);
        if (rc == SUBETHA_E_TIMEOUT) {
            out->retries++;
            fprintf(stderr, "  rr server: no request for %d ms after %llu of %llu\n", RR_WAIT_MS,
                    (unsigned long long)out->items, (unsigned long long)expected);
            /* The ring trace is per process, and this process is the only
             * place its own history exists: the parent creates no rings and
             * the clients hold different ones. A count of zero here says the
             * server registered a consumer and then saw nothing, which the
             * registration and shape-decision events distinguish from a
             * server that never became drainable. */
            fflush(stderr);
            (void)subetha_test_ring_trace_dump(0, 200);
            fflush(stderr);
            problems++;
            break;
        }
        if (rc != SUBETHA_OK) {
            wl_detail("rr server", "recv_frame_wait", rc);
            problems++;
            break;
        }
        if (len < 9) {
            fprintf(stderr, "  rr server: a %zu-byte request has no header\n", len);
            problems++;
            break;
        }
        uint32_t corr = get_u32(frame);
        uint32_t client = get_u32(frame + 4);
        uint8_t kind = frame[8];
        if (client >= clients) {
            fprintf(stderr, "  rr server: request from client %u of %u\n", (unsigned)client, (unsigned)clients);
            problems++;
            break;
        }
        out->bytes += len;
        if (kind == RR_KIND_PING) {
            put_u32(reply, corr);
            reply[4] = RR_FINAL;
            memcpy(reply + 5, frame + 9, len - 9);
            rc = subetha_ring_send_frame_wait(resp[client], resp_pid[client], reply, 5 + (len - 9), SUBETHA_LAYOUT_AUTO,
                                              RR_WAIT_MS, NULL);
            if (rc != SUBETHA_OK) {
                wl_detail("rr server", "send ping reply", rc);
                problems++;
                break;
            }
            out->bytes += 5 + (len - 9);
        } else if (kind == RR_KIND_WIDE) {
            uint32_t sent = 0;
            while (sent < RR_WIDE_REPLY) {
                uint32_t n = RR_WIDE_REPLY - sent;
                if (n > RR_CHUNK) {
                    n = RR_CHUNK;
                }
                put_u32(reply, corr);
                reply[4] = (sent + n < RR_WIDE_REPLY) ? RR_PARTIAL : RR_FINAL;
                for (uint32_t i = 0; i < n; i++) {
                    reply[5 + i] = (uint8_t)((sent + i) % 251);
                }
                rc = subetha_ring_send_frame_wait(resp[client], resp_pid[client], reply, 5 + n, SUBETHA_LAYOUT_AUTO,
                                                  RR_WAIT_MS, NULL);
                if (rc != SUBETHA_OK) {
                    wl_detail("rr server", "send wide reply chunk", rc);
                    problems++;
                    break;
                }
                out->bytes += 5 + n;
                sent += n;
            }
        } else {
            fprintf(stderr, "  rr server: request kind %u\n", (unsigned)kind);
            problems++;
            break;
        }
        out->items++;
    }
    out->elapsed_ns = now_ns() - start;

    for (uint32_t i = 0; i < clients; i++) {
        if (resp[i] != SUBETHA_HANDLE_NONE && subetha_handle_destroy(resp[i]) != SUBETHA_OK) {
            problems++;
        }
    }
    if (subetha_handle_destroy(req) != SUBETHA_OK) {
        problems++;
    }
    subetha_unlink_report report;
    if (ring_unlink(locale, req_name, RR_PEERS, &report) != SUBETHA_OK || report.failed != 0) {
        problems++;
    }
    for (uint32_t i = 0; i < clients; i++) {
        char name[512];
        rr_name(name, sizeof name, base, "resp", i);
        if (ring_unlink(locale, name, RR_PEERS, &report) != SUBETHA_OK || report.failed != 0) {
            problems++;
        }
    }
    free(resp);
    free(resp_pid);
    return problems;
}

/* One client: `requests` round trips alternating ping and wide, each
 * matched to its correlation id, a mismatch discarded as a leftover of
 * a timed-out call. */
int subetha_workload_rr_client(uint32_t locale, const char *base, const char *sddl, uint32_t mode, uint32_t client,
                               uint32_t requests, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_ring_options opt = ring_options(mode, RR_FRAME_BLOCK, RR_FRAME_BLOCKS, sddl);
    char req_name[512], resp_name[512];
    rr_name(req_name, sizeof req_name, base, "req", 0);
    rr_name(resp_name, sizeof resp_name, base, "resp", client);
    subetha_handle req = SUBETHA_HANDLE_NONE, resp = SUBETHA_HANDLE_NONE;
    int32_t rc = ring_open(locale, req_name, RR_PEERS, RR_PEERS, RR_CAPACITY, &opt, &req);
    if (rc != SUBETHA_OK) {
        wl_detail("rr client", "open request ring", rc);
        return 1;
    }
    rc = ring_open(locale, resp_name, RR_PEERS, RR_PEERS, RR_CAPACITY, &opt, &resp);
    if (rc != SUBETHA_OK) {
        wl_detail("rr client", "open response ring", rc);
        subetha_handle_destroy(req);
        return 1;
    }
    uint32_t pid = 0, cid = 0;
    if ((rc = subetha_ring_register_producer(req, &pid)) != SUBETHA_OK ||
        (rc = subetha_ring_register_consumer(resp, &cid)) != SUBETHA_OK) {
        wl_detail("rr client", "register", rc);
        subetha_handle_destroy(resp);
        subetha_handle_destroy(req);
        return 1;
    }

    static uint8_t request[64];
    static uint8_t frame[RR_FRAME_BLOCK];
    uint32_t corr = client * 1000003u;
    uint64_t start = now_ns();
    for (uint32_t i = 0; i < requests && problems == 0; i++) {
        corr++;
        uint8_t kind = (i % 2 == 0) ? RR_KIND_PING : RR_KIND_WIDE;
        put_u32(request, corr);
        put_u32(request + 4, client);
        request[8] = kind;
        int plen = snprintf((char *)request + 9, sizeof request - 9, "c%u-r%u", (unsigned)client, (unsigned)i);
        size_t req_len = 9 + (size_t)plen;
        uint64_t t0 = now_ns();
        rc = subetha_ring_send_frame_wait(req, pid, request, req_len, SUBETHA_LAYOUT_AUTO, RR_WAIT_MS, NULL);
        if (rc != SUBETHA_OK) {
            wl_detail("rr client", "send request", rc);
            problems++;
            break;
        }
        out->bytes += req_len;
        uint32_t got = 0;
        int done = 0;
        while (!done) {
            size_t len = 0;
            rc = subetha_ring_recv_frame_wait(resp, cid, frame, sizeof frame, &len, RR_WAIT_MS);
            if (rc != SUBETHA_OK) {
                wl_detail("rr client", "recv response", rc);
                /* This client's own response ring, which no other process
                 * holds: whether its producer registered and whether its
                 * consumer slot stayed its own are answerable only here. */
                fflush(stderr);
                (void)subetha_test_ring_trace_dump(0, 200);
                fflush(stderr);
                problems++;
                break;
            }
            if (len < 5) {
                fprintf(stderr, "  rr client: a %zu-byte response has no header\n", len);
                problems++;
                break;
            }
            if (get_u32(frame) != corr) {
                /* A leftover of a call that timed out: not this reply. */
                continue;
            }
            out->bytes += len;
            uint8_t status = frame[4];
            if (kind == RR_KIND_PING) {
                if (status != RR_FINAL || len - 5 != req_len - 9 || memcmp(frame + 5, request + 9, len - 5) != 0) {
                    fprintf(stderr, "  rr client: ping reply differs (status %u, %zu bytes)\n", (unsigned)status, len);
                    problems++;
                }
                done = 1;
            } else {
                for (size_t k = 5; k < len; k++) {
                    if (frame[k] != (uint8_t)((got + (k - 5)) % 251)) {
                        fprintf(stderr, "  rr client: wide reply byte %zu differs\n", got + (k - 5));
                        problems++;
                        break;
                    }
                }
                got += (uint32_t)(len - 5);
                if (status == RR_FINAL) {
                    if (got != RR_WIDE_REPLY) {
                        fprintf(stderr, "  rr client: wide reply is %u bytes, not %u\n", (unsigned)got, RR_WIDE_REPLY);
                        problems++;
                    }
                    done = 1;
                } else if (status != RR_PARTIAL) {
                    fprintf(stderr, "  rr client: status %u\n", (unsigned)status);
                    problems++;
                    done = 1;
                }
            }
        }
        note_round_trip(out, now_ns() - t0);
        out->items++;
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_ring_unregister_producer(req, pid) != SUBETHA_OK || subetha_handle_destroy(resp) != SUBETHA_OK ||
        subetha_handle_destroy(req) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* ------------------------------------------------------------- fleet --
 * Two rings per worker, `{base}_snap_{i}` host to worker and
 * `{base}_find_{i}` worker to host, one producer and one consumer each,
 * capacity 64, 2 MiB frame blocks, four of them. A snapshot is
 * FLEET_SNAPSHOT bytes, always past the inline budget; a finding set is
 * FLEET_FINDING bytes. A zero-length frame on the snap ring is the
 * shutdown sentinel. The host offers without waiting and counts a full
 * ring as a dropped offer; a worker polls with a short wait and gives
 * up after FLEET_IDLE_LIMIT empty polls. */
#define FLEET_CAPACITY 64
#define FLEET_FRAME_BLOCK (2u * 1024u * 1024u)
#define FLEET_FRAME_BLOCKS 4
#define FLEET_SNAPSHOT 4670
#define FLEET_FINDING 320
#define FLEET_POLL_MS 5
#define FLEET_IDLE_LIMIT 6000

/* The host: creates the rings, prints its ready marker, offers `rounds`
 * snapshots to every worker, collects the findings, sends the sentinel
 * and unlinks. */
int subetha_workload_fleet_host(uint32_t locale, const char *base, uint32_t mode, uint32_t workers, uint32_t rounds,
                                subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_ring_options opt = ring_options(mode, FLEET_FRAME_BLOCK, FLEET_FRAME_BLOCKS, NULL);
    subetha_handle *snap = calloc(workers, sizeof *snap);
    subetha_handle *find = calloc(workers, sizeof *find);
    uint32_t *snap_pid = calloc(workers, sizeof *snap_pid);
    uint32_t *find_cid = calloc(workers, sizeof *find_cid);
    if (snap == NULL || find == NULL || snap_pid == NULL || find_cid == NULL) {
        fprintf(stderr, "  fleet host: no memory for %u workers\n", (unsigned)workers);
        free(snap);
        free(find);
        free(snap_pid);
        free(find_cid);
        return 1;
    }
    for (uint32_t i = 0; i < workers; i++) {
        char name[512];
        rr_name(name, sizeof name, base, "snap", i);
        int32_t rc = ring_create(locale, name, 1, 1, FLEET_CAPACITY, &opt, &snap[i]);
        if (rc == SUBETHA_OK) {
            rc = subetha_ring_register_producer(snap[i], &snap_pid[i]);
        }
        if (rc == SUBETHA_OK) {
            rr_name(name, sizeof name, base, "find", i);
            rc = ring_create(locale, name, 1, 1, FLEET_CAPACITY, &opt, &find[i]);
        }
        if (rc == SUBETHA_OK) {
            rc = subetha_ring_register_consumer(find[i], &find_cid[i]);
        }
        if (rc != SUBETHA_OK) {
            wl_detail("fleet host", "set up a worker's rings", rc);
            fprintf(stdout, "FLEET-HOST-FAILED %d\n", (int)rc);
            fflush(stdout);
            problems++;
        }
    }
    if (problems != 0) {
        for (uint32_t i = 0; i < workers; i++) {
            if (snap[i] != SUBETHA_HANDLE_NONE) {
                subetha_handle_destroy(snap[i]);
            }
            if (find[i] != SUBETHA_HANDLE_NONE) {
                subetha_handle_destroy(find[i]);
            }
        }
        free(snap);
        free(find);
        free(snap_pid);
        free(find_cid);
        return problems;
    }
    fprintf(stdout, "FLEET-HOST-READY\n");
    fflush(stdout);

    static uint8_t snapshot[FLEET_SNAPSHOT];
    static uint8_t finding[FLEET_FRAME_BLOCK];
    uint64_t start = now_ns();
    uint64_t findings = 0;
    /* A round: one offer to every worker, then the finding set each
     * accepted offer earns, so the region's four blocks are never
     * outrun and every round is a measured exchange. An offer a full
     * ring refuses is counted and not retried. */
    for (uint32_t r = 0; r < rounds && problems == 0; r++) {
        for (size_t k = 0; k < sizeof snapshot; k++) {
            snapshot[k] = (uint8_t)((r + k) % 241);
        }
        for (uint32_t i = 0; i < workers && problems == 0; i++) {
            uint64_t t0 = now_ns();
            int32_t rc = subetha_ring_send_frame(snap[i], snap_pid[i], snapshot, sizeof snapshot, SUBETHA_LAYOUT_AUTO, NULL);
            if (rc == SUBETHA_E_RING_FULL) {
                out->refusals++;
                continue;
            }
            if (rc != SUBETHA_OK) {
                wl_detail("fleet host", "offer a snapshot", rc);
                problems++;
                break;
            }
            out->items++;
            out->bytes += sizeof snapshot;
            size_t len = 0;
            rc = subetha_ring_recv_frame_wait(find[i], find_cid[i], finding, sizeof finding, &len, RR_WAIT_MS);
            if (rc != SUBETHA_OK) {
                wl_detail("fleet host", "collect a finding set", rc);
                problems++;
                break;
            }
            if (len != FLEET_FINDING) {
                fprintf(stderr, "  fleet host: a %zu-byte finding set\n", len);
                problems++;
                break;
            }
            findings++;
            out->bytes += len;
            note_round_trip(out, now_ns() - t0);
        }
    }
    for (uint32_t i = 0; i < workers; i++) {
        int32_t rc = subetha_ring_send_frame_wait(snap[i], snap_pid[i], snapshot, 0, SUBETHA_LAYOUT_AUTO, RR_WAIT_MS, NULL);
        if (rc != SUBETHA_OK) {
            wl_detail("fleet host", "send the sentinel", rc);
            problems++;
        }
    }
    out->elapsed_ns = now_ns() - start;
    if (findings != out->items) {
        fprintf(stderr, "  fleet host: %llu finding sets for %llu accepted offers\n", (unsigned long long)findings,
                (unsigned long long)out->items);
        problems++;
    }
    /* The workers' exit is observed by the harness; the rings are
     * released after it reports them gone. */
    fprintf(stdout, "FLEET-HOST-DONE\n");
    fflush(stdout);
    for (uint32_t i = 0; i < workers; i++) {
        if (snap[i] != SUBETHA_HANDLE_NONE && subetha_handle_destroy(snap[i]) != SUBETHA_OK) {
            problems++;
        }
        if (find[i] != SUBETHA_HANDLE_NONE && subetha_handle_destroy(find[i]) != SUBETHA_OK) {
            problems++;
        }
    }
    free(snap);
    free(find);
    free(snap_pid);
    free(find_cid);
    return problems;
}

/* Unlink a fleet's rings, called by the harness once the workers have
 * exited. */
int subetha_workload_fleet_unlink(uint32_t locale, const char *base, uint32_t workers)
{
    int problems = 0;
    subetha_unlink_report report;
    for (uint32_t i = 0; i < workers; i++) {
        char name[512];
        rr_name(name, sizeof name, base, "snap", i);
        if (ring_unlink(locale, name, 1, &report) != SUBETHA_OK || report.failed != 0) {
            problems++;
        }
        rr_name(name, sizeof name, base, "find", i);
        if (ring_unlink(locale, name, 1, &report) != SUBETHA_OK || report.failed != 0) {
            problems++;
        }
    }
    return problems;
}

/* A worker: opens its two rings, answers every snapshot with one
 * finding set, exits on the sentinel or after the idle limit. */
int subetha_workload_fleet_worker(uint32_t locale, const char *base, uint32_t mode, uint32_t index,
                                  subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_ring_options opt = ring_options(mode, FLEET_FRAME_BLOCK, FLEET_FRAME_BLOCKS, NULL);
    char name[512];
    subetha_handle snap = SUBETHA_HANDLE_NONE, find = SUBETHA_HANDLE_NONE;
    rr_name(name, sizeof name, base, "snap", index);
    int32_t rc = ring_open(locale, name, 1, 1, FLEET_CAPACITY, &opt, &snap);
    if (rc == SUBETHA_OK) {
        rr_name(name, sizeof name, base, "find", index);
        rc = ring_open(locale, name, 1, 1, FLEET_CAPACITY, &opt, &find);
    }
    uint32_t cid = 0, pid = 0;
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_register_consumer(snap, &cid);
    }
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_register_producer(find, &pid);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("fleet worker", "open the rings", rc);
        if (find != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(find);
        }
        if (snap != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(snap);
        }
        return 1;
    }
    static uint8_t snapshot[FLEET_FRAME_BLOCK];
    static uint8_t finding[FLEET_FINDING];
    uint32_t idle = 0;
    uint64_t start = now_ns();
    for (;;) {
        size_t len = 0;
        rc = subetha_ring_recv_frame_wait(snap, cid, snapshot, sizeof snapshot, &len, FLEET_POLL_MS);
        if (rc == SUBETHA_E_TIMEOUT) {
            out->retries++;
            if (++idle >= FLEET_IDLE_LIMIT) {
                fprintf(stderr, "  fleet worker %u: idle for %u polls, leaving\n", (unsigned)index, FLEET_IDLE_LIMIT);
                problems++;
                break;
            }
            continue;
        }
        if (rc != SUBETHA_OK) {
            wl_detail("fleet worker", "recv a snapshot", rc);
            problems++;
            break;
        }
        idle = 0;
        if (len == 0) {
            break; /* the sentinel */
        }
        if (len != FLEET_SNAPSHOT) {
            fprintf(stderr, "  fleet worker %u: a %zu-byte snapshot\n", (unsigned)index, len);
            problems++;
            break;
        }
        out->items++;
        out->bytes += len;
        for (size_t k = 0; k < sizeof finding; k++) {
            finding[k] = (uint8_t)(snapshot[k % len] ^ (uint8_t)index);
        }
        rc = subetha_ring_send_frame_wait(find, pid, finding, sizeof finding, SUBETHA_LAYOUT_AUTO, RR_WAIT_MS, NULL);
        if (rc != SUBETHA_OK) {
            wl_detail("fleet worker", "send a finding set", rc);
            problems++;
            break;
        }
        out->bytes += sizeof finding;
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(find) != SUBETHA_OK || subetha_handle_destroy(snap) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* -------------------------------------------------------------- race --
 * One anonymous ring the harness created with T producers and T
 * consumers; explorer `index` registers both roles, publishes a migrant
 * per round as a frame of 10 + 3 * steps bytes (level, length, three
 * bytes per step), and drains everything the others published, stopping
 * at empty. A full ring drops the publish. The migrant's third step byte
 * names the explorer rather than the producer slot it publishes from: a
 * slot recycles the moment its holder leaves, so two explorers can
 * publish from one slot in turn. `slot` receives the producer slot this
 * explorer published from. */
int subetha_workload_race_explorer(subetha_handle ring, uint32_t index, uint32_t rounds, uint32_t steps,
                                   uint8_t *seen, size_t seen_len, uint32_t *slot,
                                   subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint32_t pid = 0, cid = 0;
    int32_t rc = subetha_ring_register_producer(ring, &pid);
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_register_consumer(ring, &cid);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("race explorer", "register", rc);
        return 1;
    }
    *slot = pid;
    size_t migrant_len = 10 + (size_t)steps * 3;
    uint8_t *migrant = malloc(migrant_len);
    /* Every explorer runs this on its own thread and receives into this
     * buffer, so it belongs to the call rather than the program: a shared
     * one lets a frame be overwritten while another thread is reading it. */
    uint8_t *adopt = malloc(SUBETHA_RING_FRAME_DEFAULT_BLOCK);
    if (migrant == NULL || adopt == NULL) {
        fprintf(stderr, "  race explorer: no memory for a %zu-byte migrant\n", migrant_len);
        free(migrant);
        free(adopt);
        return 1;
    }
    uint64_t start = now_ns();
    for (uint32_t r = 0; r < rounds && problems == 0; r++) {
        uint64_t level = r;
        for (int b = 0; b < 8; b++) {
            migrant[b] = (uint8_t)(level >> (8 * b));
        }
        migrant[8] = (uint8_t)steps;
        migrant[9] = (uint8_t)(steps >> 8);
        for (uint32_t s = 0; s < steps; s++) {
            migrant[10 + s * 3] = (uint8_t)s;
            migrant[11 + s * 3] = (uint8_t)(s >> 8);
            migrant[12 + s * 3] = (uint8_t)index;
        }
        rc = subetha_ring_send_frame(ring, pid, migrant, migrant_len, SUBETHA_LAYOUT_AUTO, NULL);
        if (rc == SUBETHA_E_RING_FULL) {
            out->refusals++;
        } else if (rc != SUBETHA_OK) {
            wl_detail("race explorer", "publish", rc);
            problems++;
            break;
        } else {
            out->bytes += migrant_len;
        }
        for (;;) {
            size_t len = 0;
            rc = subetha_ring_recv_frame(ring, cid, adopt, SUBETHA_RING_FRAME_DEFAULT_BLOCK, &len);
            if (rc == SUBETHA_E_RING_EMPTY) {
                break;
            }
            if (rc != SUBETHA_OK) {
                wl_detail("race explorer", "adopt", rc);
                problems++;
                break;
            }
            if (len != migrant_len) {
                fprintf(stderr, "  race explorer: a %zu-byte migrant, not %zu\n", len, migrant_len);
                problems++;
                break;
            }
            /* Mark the migrant adopted. A migrant goes to exactly one
             * consumer, so each byte has a single writer and the map
             * needs no atomics. The round is the first eight bytes and
             * the explorer is at byte 12, both written by the loop
             * above. */
            if (seen != NULL) {
                uint64_t adopted_round = 0;
                for (int b = 0; b < 8; b++) {
                    adopted_round |= (uint64_t)adopt[b] << (8 * b);
                }
                size_t entry = (size_t)adopt[12] * rounds + (size_t)adopted_round;
                if (entry < seen_len) {
                    seen[entry] = 1;
                }
            }
            out->items++;
        }
    }
    out->elapsed_ns = now_ns() - start;
    free(migrant);
    free(adopt);
    if (subetha_ring_unregister_consumer(ring, cid) != SUBETHA_OK || subetha_ring_unregister_producer(ring, pid) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* --------------------------------------------------------------- log --
 * A line is chunked into 64-byte slots: a start slot [0x01][len u16
 * LE][61 bytes] and continuation slots [0x02][63 bytes]. The start slot
 * is pushed without waiting and the line dropped when the ring is full;
 * a continuation waits LOG_CONT_WAIT_MS and abandons the line on a
 * timeout. The writer drains to empty with try_pop, sleeps
 * LOG_BUSY_SLEEP_US after a pass that moved lines and LOG_IDLE_SLEEP_US
 * after one that did not, and stops at the end slot [0x03] the harness
 * pushes once every producer has returned. Silence is not the end: a
 * producer starved of its core for a second and more is silent, and a
 * writer that took that for the end left a ring of whole lines behind. */
#define LOG_START 0x01
#define LOG_CONT 0x02
#define LOG_END 0x03
#define LOG_START_DATA 61
#define LOG_CONT_DATA 63
#define LOG_CONT_WAIT_MS 1
#define LOG_END_WAIT_MS 60000
#define LOG_BUSY_SLEEP_US 4000
#define LOG_IDLE_SLEEP_US 200000

/* Push `lines` lines of `line_bytes` bytes from index `first`, pausing
 * `pace_us` between lines the way a logging thread does between
 * messages; the line text carries its index so the writer can check
 * it. `out->items` counts lines pushed whole, `out->refusals` lines
 * dropped at the start slot, `out->retries` lines abandoned at a
 * continuation. */
int subetha_workload_log_producer(subetha_handle ring, uint32_t first, uint32_t lines, uint32_t line_bytes, uint32_t pace_us,
                                  subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    if (line_bytes < 12 || line_bytes > 65535) {
        fprintf(stderr, "  log producer: line_bytes %u is not between 12 and 65535\n", (unsigned)line_bytes);
        return 1;
    }
    uint8_t *line = malloc(line_bytes);
    if (line == NULL) {
        return 1;
    }
    uint8_t slot[SUBETHA_RING_SLOT_BYTES];
    uint64_t start = now_ns();
    for (uint32_t i = first; i < first + lines && problems == 0; i++) {
        if (pace_us != 0 && i != first) {
            spin_us(pace_us);
        }
        int n = snprintf((char *)line, line_bytes, "line %010u ", (unsigned)i);
        for (uint32_t k = (uint32_t)n; k < line_bytes; k++) {
            line[k] = (uint8_t)('a' + (i + k) % 26);
        }
        slot[0] = LOG_START;
        slot[1] = (uint8_t)line_bytes;
        slot[2] = (uint8_t)(line_bytes >> 8);
        uint32_t head = line_bytes < LOG_START_DATA ? line_bytes : LOG_START_DATA;
        memcpy(slot + 3, line, head);
        int32_t rc = subetha_spsc_try_push(ring, slot, 3 + head);
        if (rc == SUBETHA_E_RING_FULL) {
            out->refusals++;
            continue;
        }
        if (rc != SUBETHA_OK) {
            wl_detail("log producer", "push a start slot", rc);
            problems++;
            break;
        }
        uint32_t sent = head;
        int abandoned = 0;
        while (sent < line_bytes) {
            uint32_t n2 = line_bytes - sent;
            if (n2 > LOG_CONT_DATA) {
                n2 = LOG_CONT_DATA;
            }
            slot[0] = LOG_CONT;
            memcpy(slot + 1, line + sent, n2);
            rc = subetha_spsc_push_wait(ring, slot, 1 + n2, LOG_CONT_WAIT_MS);
            if (rc == SUBETHA_E_TIMEOUT) {
                out->retries++;
                abandoned = 1;
                break;
            }
            if (rc != SUBETHA_OK) {
                wl_detail("log producer", "push a continuation", rc);
                problems++;
                break;
            }
            sent += n2;
        }
        if (!abandoned && problems == 0) {
            out->items++;
            out->bytes += line_bytes;
        }
    }
    out->elapsed_ns = now_ns() - start;
    free(line);
    return problems;
}

/* Mark the end of the log, once every producer has returned: the writer
 * drains everything before this slot and then stops. */
int subetha_workload_log_end(subetha_handle ring)
{
    uint8_t slot[1] = {LOG_END};
    int32_t rc = subetha_spsc_push_wait(ring, slot, sizeof slot, LOG_END_WAIT_MS);
    if (rc != SUBETHA_OK) {
        wl_detail("log end", "push the end slot", rc);
        return 1;
    }
    return 0;
}

/* Drain the ring into lines until the end slot. `out->items` counts
 * complete lines whose text matches their index, `out->refusals` lines
 * that were cut short by a new start slot, `out->retries` slots that
 * arrived outside any line, `out->total_ns` passes that found nothing. */
int subetha_workload_log_writer(subetha_handle ring, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint8_t slot[SUBETHA_RING_SLOT_BYTES];
    uint8_t line[65536];
    uint32_t want = 0, have = 0;
    int in_line = 0;
    int ended = 0;
    uint64_t start = now_ns();
    while (!ended && problems == 0) {
        int moved = 0;
        for (;;) {
            size_t len = 0;
            int32_t rc = subetha_spsc_try_pop(ring, slot, sizeof slot, &len);
            if (rc == SUBETHA_E_RING_EMPTY) {
                break;
            }
            if (rc != SUBETHA_OK) {
                wl_detail("log writer", "try_pop", rc);
                problems++;
                break;
            }
            moved = 1;
            if (slot[0] == LOG_END) {
                ended = 1;
                break;
            }
            if (slot[0] == LOG_START) {
                if (in_line) {
                    out->refusals++;
                }
                want = (uint32_t)slot[1] | ((uint32_t)slot[2] << 8);
                have = want < LOG_START_DATA ? want : LOG_START_DATA;
                memcpy(line, slot + 3, have);
                in_line = 1;
            } else if (slot[0] == LOG_CONT && in_line) {
                uint32_t n = want - have;
                if (n > LOG_CONT_DATA) {
                    n = LOG_CONT_DATA;
                }
                memcpy(line + have, slot + 1, n);
                have += n;
            } else {
                out->retries++;
                continue;
            }
            if (in_line && have == want) {
                line[want] = 0;
                const char *digits = (const char *)line + 5;
                char *end = NULL;
                unsigned long parsed = (want > 5 && memcmp(line, "line ", 5) == 0) ? strtoul(digits, &end, 10) : 0;
                unsigned index = (unsigned)parsed;
                if (end == NULL || end == digits || *end != ' ') {
                    fprintf(stderr, "  log writer: a line without its index\n");
                    problems++;
                } else {
                    int ok = 1;
                    char head[24];
                    int n = snprintf(head, sizeof head, "line %010u ", index);
                    for (uint32_t k = (uint32_t)n; k < want; k++) {
                        if (line[k] != (uint8_t)('a' + (index + k) % 26)) {
                            ok = 0;
                            break;
                        }
                    }
                    if (!ok) {
                        fprintf(stderr, "  log writer: line %u differs from what was pushed\n", index);
                        problems++;
                    }
                }
                out->items++;
                out->bytes += want;
                in_line = 0;
            }
        }
        if (ended) {
            break;
        }
        if (moved) {
            sleep_us(LOG_BUSY_SLEEP_US);
        } else {
            out->total_ns++;
            sleep_us(LOG_IDLE_SLEEP_US);
        }
    }
    out->elapsed_ns = now_ns() - start;
    return problems;
}

/* --------------------------------------------------------------- bus --
 * A command ring `{prefix}_cmd`, file-backed, one producer and one
 * consumer, capacity 256, frames. An asking process creates its own
 * reply ring `{prefix}_reply_{nonce}` and sends [nonce u64][needle] on
 * the command ring; the shell opens that reply ring, sends BUS_ROWS row
 * frames of about BUS_ROW_BYTES bytes and a done frame, and closes it.
 * The shell serves `clients * queries` requests. */
#define BUS_CAPACITY 256
#define BUS_ROWS 64
#define BUS_ROW_BYTES 170
#define BUS_WAIT_MS 20000
#define BUS_DONE 0xFF

static void bus_reply_name(char *out, size_t cap, const char *prefix, uint64_t nonce)
{
    snprintf(out, cap, "%s_reply_%016llx", prefix, (unsigned long long)nonce);
}

int subetha_workload_bus_shell(const char *prefix, uint32_t mode, uint32_t clients, uint32_t queries,
                               subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_ring_options opt = ring_options(mode, 0, 0, NULL);
    char cmd_name[512];
    snprintf(cmd_name, sizeof cmd_name, "%s_cmd", prefix);
    subetha_handle cmd = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_ring_create(cmd_name, 1, 1, BUS_CAPACITY, &opt, &cmd);
    uint32_t cid = 0;
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_register_consumer(cmd, &cid);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("bus shell", "create the command ring", rc);
        return 1;
    }
    fprintf(stdout, "BUS-SHELL-READY\n");
    fflush(stdout);
    static uint8_t frame[SUBETHA_RING_FRAME_DEFAULT_BLOCK];
    static uint8_t row[BUS_ROW_BYTES + 8];
    uint64_t expected = (uint64_t)clients * queries;
    uint64_t start = now_ns();
    while (out->items < expected && problems == 0) {
        size_t len = 0;
        rc = subetha_ring_recv_frame_wait(cmd, cid, frame, sizeof frame, &len, BUS_WAIT_MS);
        if (rc != SUBETHA_OK) {
            wl_detail("bus shell", "recv a query", rc);
            problems++;
            break;
        }
        if (len < 9) {
            fprintf(stderr, "  bus shell: a %zu-byte query\n", len);
            problems++;
            break;
        }
        uint64_t nonce = 0;
        for (int b = 0; b < 8; b++) {
            nonce |= (uint64_t)frame[b] << (8 * b);
        }
        out->bytes += len;
        char reply_name[512];
        bus_reply_name(reply_name, sizeof reply_name, prefix, nonce);
        subetha_handle reply = SUBETHA_HANDLE_NONE;
        uint32_t pid = 0;
        rc = subetha_ring_open(reply_name, 1, 1, BUS_CAPACITY, &opt, &reply);
        if (rc == SUBETHA_OK) {
            rc = subetha_ring_register_producer(reply, &pid);
        }
        if (rc != SUBETHA_OK) {
            wl_detail("bus shell", "open the reply ring", rc);
            problems++;
            break;
        }
        for (uint32_t r = 0; r < BUS_ROWS && problems == 0; r++) {
            int n = snprintf((char *)row, sizeof row, "row %u of %.*s: ", (unsigned)r, (int)(len - 8), (const char *)frame + 8);
            for (int k = n; k < BUS_ROW_BYTES; k++) {
                row[k] = (uint8_t)('/' + (k % 3 == 0 ? 0 : 'a' - '/' + (k % 26)));
            }
            rc = subetha_ring_send_frame_wait(reply, pid, row, BUS_ROW_BYTES, SUBETHA_LAYOUT_AUTO, BUS_WAIT_MS, NULL);
            if (rc != SUBETHA_OK) {
                wl_detail("bus shell", "send a row", rc);
                problems++;
            }
            out->bytes += BUS_ROW_BYTES;
        }
        uint8_t done = BUS_DONE;
        if (problems == 0) {
            rc = subetha_ring_send_frame_wait(reply, pid, &done, 1, SUBETHA_LAYOUT_AUTO, BUS_WAIT_MS, NULL);
            if (rc != SUBETHA_OK) {
                wl_detail("bus shell", "send done", rc);
                problems++;
            }
        }
        if (subetha_ring_unregister_producer(reply, pid) != SUBETHA_OK || subetha_handle_destroy(reply) != SUBETHA_OK) {
            problems++;
        }
        out->items++;
    }
    out->elapsed_ns = now_ns() - start;
    fprintf(stdout, "BUS-SHELL-DONE\n");
    fflush(stdout);
    if (subetha_handle_destroy(cmd) != SUBETHA_OK) {
        problems++;
    }
    subetha_unlink_report report;
    if (subetha_ring_unlink(cmd_name, 1, &report) != SUBETHA_OK || report.failed != 0) {
        problems++;
    }
    return problems;
}

/* An asking process: its own reply ring, `queries` queries each answered
 * by BUS_ROWS rows and a done frame; the reply ring is unlinked at the
 * end. */
int subetha_workload_bus_client(const char *prefix, uint32_t mode, uint32_t client, uint32_t queries,
                                subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_ring_options opt = ring_options(mode, 0, 0, NULL);
    char cmd_name[512], reply_name[512];
    snprintf(cmd_name, sizeof cmd_name, "%s_cmd", prefix);
    uint64_t nonce = ((uint64_t)client << 40) ^ (now_ns() & 0xffffffffffull);
    bus_reply_name(reply_name, sizeof reply_name, prefix, nonce);
    subetha_handle cmd = SUBETHA_HANDLE_NONE, reply = SUBETHA_HANDLE_NONE;
    uint32_t pid = 0, cid = 0;
    int32_t rc = subetha_ring_create(reply_name, 1, 1, BUS_CAPACITY, &opt, &reply);
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_register_consumer(reply, &cid);
    }
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_open(cmd_name, 1, 1, BUS_CAPACITY, &opt, &cmd);
    }
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_register_producer(cmd, &pid);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("bus client", "set up", rc);
        if (cmd != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(cmd);
        }
        if (reply != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(reply);
        }
        return 1;
    }
    static uint8_t query[128];
    static uint8_t frame[SUBETHA_RING_FRAME_DEFAULT_BLOCK];
    uint64_t start = now_ns();
    for (uint32_t q = 0; q < queries && problems == 0; q++) {
        for (int b = 0; b < 8; b++) {
            query[b] = (uint8_t)(nonce >> (8 * b));
        }
        int n = snprintf((char *)query + 8, sizeof query - 8, "needle-%u-%u-past-the-inline-budget-of-fifty-one-bytes-and-then-some",
                         (unsigned)client, (unsigned)q);
        uint64_t t0 = now_ns();
        rc = subetha_ring_send_frame_wait(cmd, pid, query, 8 + (size_t)n, SUBETHA_LAYOUT_AUTO, BUS_WAIT_MS, NULL);
        if (rc != SUBETHA_OK) {
            wl_detail("bus client", "send a query", rc);
            problems++;
            break;
        }
        out->bytes += 8 + (size_t)n;
        uint32_t rows = 0;
        for (;;) {
            size_t len = 0;
            rc = subetha_ring_recv_frame_wait(reply, cid, frame, sizeof frame, &len, BUS_WAIT_MS);
            if (rc != SUBETHA_OK) {
                wl_detail("bus client", "recv a row", rc);
                problems++;
                break;
            }
            out->bytes += len;
            if (len == 1 && frame[0] == BUS_DONE) {
                break;
            }
            if (len != BUS_ROW_BYTES) {
                fprintf(stderr, "  bus client: a %zu-byte row\n", len);
                problems++;
                break;
            }
            rows++;
        }
        if (problems == 0 && rows != BUS_ROWS) {
            fprintf(stderr, "  bus client: %u rows, not %u\n", (unsigned)rows, BUS_ROWS);
            problems++;
        }
        note_round_trip(out, now_ns() - t0);
        out->items++;
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_ring_unregister_producer(cmd, pid) != SUBETHA_OK || subetha_handle_destroy(cmd) != SUBETHA_OK ||
        subetha_handle_destroy(reply) != SUBETHA_OK) {
        problems++;
    }
    subetha_unlink_report report;
    if (subetha_ring_unlink(reply_name, 1, &report) != SUBETHA_OK || report.failed != 0) {
        problems++;
    }
    return problems;
}

/* -------------------------------------------------------------- menu --
 * One direction over a file-backed single-producer single-consumer
 * ring of 256 slots: BEGIN [1][x i32][y i32], CHUNK [2][len][<= 62
 * bytes] repeated over a path of about MENU_PATH_BYTES bytes, SHOW [3].
 * The consumer opens the ring with MENU_OPEN_TRIES attempts
 * MENU_OPEN_SLEEP_US apart and pops without waiting; the producer waits
 * that whole budget per push, since a consumer still inside it is a
 * consumer that may yet arrive. */
#define MENU_CAPACITY 256
#define MENU_BEGIN 1
#define MENU_CHUNK 2
#define MENU_SHOW 3
#define MENU_CHUNK_MAX 62
#define MENU_PATH_BYTES 230
#define MENU_OPEN_TRIES 50
#define MENU_OPEN_SLEEP_US 100000
#define MENU_PUSH_WAIT_MS ((MENU_OPEN_TRIES * MENU_OPEN_SLEEP_US) / 1000)

int subetha_workload_menu_shell(const char *path, uint32_t mode, uint32_t menus, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_handle ring = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_spsc_create(path, MENU_CAPACITY, mode, &ring);
    if (rc != SUBETHA_OK) {
        wl_detail("menu shell", "create", rc);
        return 1;
    }
    fprintf(stdout, "MENU-SHELL-READY\n");
    fflush(stdout);
    uint8_t slot[SUBETHA_RING_SLOT_BYTES];
    char file[MENU_PATH_BYTES + 1];
    uint64_t start = now_ns();
    for (uint32_t m = 0; m < menus && problems == 0; m++) {
        int n = snprintf(file, sizeof file, "/home/example/Downloads/menu-%u-", (unsigned)m);
        for (int k = n; k < MENU_PATH_BYTES - 4; k++) {
            file[k] = 'a';
        }
        memcpy(file + MENU_PATH_BYTES - 4, ".zip", 4);
        slot[0] = MENU_BEGIN;
        put_u32(slot + 1, (uint32_t)(100 + m));
        put_u32(slot + 5, (uint32_t)(200 + m));
        rc = subetha_spsc_push_wait(ring, slot, 9, MENU_PUSH_WAIT_MS);
        for (int off = 0; off < MENU_PATH_BYTES && rc == SUBETHA_OK; off += MENU_CHUNK_MAX) {
            int n2 = MENU_PATH_BYTES - off;
            if (n2 > MENU_CHUNK_MAX) {
                n2 = MENU_CHUNK_MAX;
            }
            slot[0] = MENU_CHUNK;
            slot[1] = (uint8_t)n2;
            memcpy(slot + 2, file + off, (size_t)n2);
            rc = subetha_spsc_push_wait(ring, slot, 2 + (size_t)n2, MENU_PUSH_WAIT_MS);
            out->bytes += (uint64_t)n2;
        }
        if (rc == SUBETHA_OK) {
            slot[0] = MENU_SHOW;
            rc = subetha_spsc_push_wait(ring, slot, 1, MENU_PUSH_WAIT_MS);
        }
        if (rc != SUBETHA_OK) {
            wl_detail("menu shell", "push", rc);
            problems++;
            break;
        }
        out->items++;
    }
    out->elapsed_ns = now_ns() - start;
    fprintf(stdout, "MENU-SHELL-DONE\n");
    fflush(stdout);
    if (subetha_handle_destroy(ring) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Unlink a menu ring after both processes have left. A mapping a
 * process just exited from can still be closing, so a refused removal
 * is retried MENU_OPEN_TRIES times MENU_OPEN_SLEEP_US apart; what was
 * removed, missing and refused is printed when the last try fails. */
int subetha_workload_menu_unlink(const char *path)
{
    subetha_unlink_report report = {0, 0, 0};
    int32_t rc = SUBETHA_OK;
    for (int attempt = 0; attempt < MENU_OPEN_TRIES; attempt++) {
        rc = subetha_spsc_unlink(path, &report);
        if (rc == SUBETHA_OK && report.failed == 0) {
            return 0;
        }
        sleep_us(MENU_OPEN_SLEEP_US);
    }
    wl_detail("menu unlink", "spsc_unlink", rc);
    fprintf(stderr, "  menu unlink: removed %llu, missing %llu, refused %llu\n", (unsigned long long)report.removed,
            (unsigned long long)report.missing, (unsigned long long)report.failed);
    return 1;
}

int subetha_workload_menu_broker(const char *path, uint32_t mode, uint32_t menus, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_handle ring = SUBETHA_HANDLE_NONE;
    int32_t rc = SUBETHA_E_RING_IO;
    for (int attempt = 0; attempt < MENU_OPEN_TRIES; attempt++) {
        rc = subetha_spsc_open(path, MENU_CAPACITY, mode, &ring);
        if (rc == SUBETHA_OK) {
            break;
        }
        out->retries++;
        sleep_us(MENU_OPEN_SLEEP_US);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("menu broker", "open", rc);
        return 1;
    }
    uint8_t slot[SUBETHA_RING_SLOT_BYTES];
    char file[MENU_PATH_BYTES + 64];
    size_t have = 0;
    int in_menu = 0;
    uint32_t idle = 0;
    uint64_t start = now_ns();
    while (out->items < menus && problems == 0) {
        size_t len = 0;
        rc = subetha_spsc_try_pop(ring, slot, sizeof slot, &len);
        if (rc == SUBETHA_E_RING_EMPTY) {
            if (++idle > MENU_OPEN_TRIES * 100) {
                fprintf(stderr, "  menu broker: nothing for %u polls after %llu menus\n", MENU_OPEN_TRIES * 100,
                        (unsigned long long)out->items);
                problems++;
                break;
            }
            sleep_us(1000);
            continue;
        }
        if (rc != SUBETHA_OK) {
            wl_detail("menu broker", "try_pop", rc);
            problems++;
            break;
        }
        idle = 0;
        if (slot[0] == MENU_BEGIN) {
            in_menu = 1;
            have = 0;
        } else if (slot[0] == MENU_CHUNK && in_menu) {
            size_t n = slot[1];
            if (have + n > sizeof file) {
                fprintf(stderr, "  menu broker: a path past %zu bytes\n", sizeof file);
                problems++;
                break;
            }
            memcpy(file + have, slot + 2, n);
            have += n;
            out->bytes += n;
        } else if (slot[0] == MENU_SHOW && in_menu) {
            if (have != MENU_PATH_BYTES || memcmp(file + MENU_PATH_BYTES - 4, ".zip", 4) != 0) {
                fprintf(stderr, "  menu broker: a %zu-byte path\n", have);
                problems++;
                break;
            }
            out->items++;
            in_menu = 0;
        } else {
            fprintf(stderr, "  menu broker: slot tag %u outside a menu\n", (unsigned)slot[0]);
            problems++;
            break;
        }
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(ring) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* -------------------------------------------------------------- pods --
 * Anonymous single-producer rings of 256 slots the harness created;
 * the producers are serialized by the harness. A producer pushes
 * `items` messages of up to 64 bytes with a PODS_PUSH_WAIT_MS wait and
 * drops one on a timeout, then a sentinel it never drops. A consumer
 * runs until it has seen `producers` sentinels, in one of three styles:
 * 0 waits without a deadline, 1 pops without waiting and sleeps a
 * millisecond when empty, 2 waits PODS_POP_WAIT_MS at a time. */
#define PODS_PUSH_WAIT_MS 50
#define PODS_POP_WAIT_MS 50
#define PODS_SENTINEL 0xFE

int subetha_workload_pods_producer(subetha_handle ring, uint32_t producer, uint32_t first, uint32_t items,
                                   uint32_t with_sentinel, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint8_t msg[SUBETHA_RING_SLOT_BYTES];
    uint64_t start = now_ns();
    for (uint32_t i = first; i < first + items; i++) {
        msg[0] = (uint8_t)producer;
        put_u32(msg + 1, i);
        size_t len = 5 + (i % 40);
        for (size_t k = 5; k < len; k++) {
            msg[k] = (uint8_t)k;
        }
        int32_t rc = subetha_spsc_push_wait(ring, msg, len, PODS_PUSH_WAIT_MS);
        if (rc == SUBETHA_E_TIMEOUT) {
            out->refusals++;
        } else if (rc != SUBETHA_OK) {
            wl_detail("pods producer", "push", rc);
            problems++;
            break;
        } else {
            out->items++;
            out->bytes += len;
        }
    }
    if (with_sentinel) {
        msg[0] = PODS_SENTINEL;
        msg[1] = (uint8_t)producer;
        int32_t rc = subetha_spsc_push_wait(ring, msg, 2, RR_WAIT_MS);
        if (rc != SUBETHA_OK) {
            wl_detail("pods producer", "push the sentinel", rc);
            problems++;
        }
    }
    out->elapsed_ns = now_ns() - start;
    return problems;
}

int subetha_workload_pods_consumer(subetha_handle ring, uint32_t style, uint32_t producers, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint8_t msg[SUBETHA_RING_SLOT_BYTES];
    uint32_t sentinels = 0;
    uint64_t start = now_ns();
    while (sentinels < producers && problems == 0) {
        size_t len = 0;
        int32_t rc;
        if (style == 0) {
            rc = subetha_spsc_pop_wait(ring, msg, sizeof msg, &len, SUBETHA_WAIT_FOREVER);
        } else if (style == 1) {
            rc = subetha_spsc_try_pop(ring, msg, sizeof msg, &len);
            if (rc == SUBETHA_E_RING_EMPTY) {
                sleep_us(1000);
                continue;
            }
        } else {
            rc = subetha_spsc_pop_wait(ring, msg, sizeof msg, &len, PODS_POP_WAIT_MS);
            if (rc == SUBETHA_E_TIMEOUT) {
                out->retries++;
                continue;
            }
        }
        if (rc != SUBETHA_OK) {
            wl_detail("pods consumer", "pop", rc);
            problems++;
            break;
        }
        if (msg[0] == PODS_SENTINEL) {
            sentinels++;
        } else {
            out->items++;
            out->bytes += len;
        }
    }
    out->elapsed_ns = now_ns() - start;
    return problems;
}

/* -------------------------------------------------------------- mind --
 * Two anonymous rings the harness created with one producer and one
 * consumer each: `raw` carries snapshots (tag 0, MIND_SNAPSHOT_MIN to
 * MIND_SNAPSHOT_MAX bytes) and verdicts (tag 1, 18 bytes) from the
 * foreground to the background, `promo` carries promotions (25 to 280
 * bytes) back. The background drains to empty and sleeps a millisecond
 * when nothing arrived; a zero-length frame on `raw` ends it. */
#define MIND_TAG_SNAPSHOT 0
#define MIND_TAG_VERDICT 1
#define MIND_VERDICT_BYTES 18
#define MIND_SNAPSHOT_MIN 100
#define MIND_SNAPSHOT_MAX 2000
#define MIND_PROMOTION_MIN 25
#define MIND_PROMOTION_MAX 280

int subetha_workload_mind_conscious(subetha_handle raw, subetha_handle promo, uint32_t turns, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint32_t pid = 0, cid = 0;
    int32_t rc = subetha_ring_register_producer(raw, &pid);
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_register_consumer(promo, &cid);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("mind conscious", "register", rc);
        return 1;
    }
    static uint8_t snapshot[MIND_SNAPSHOT_MAX];
    static uint8_t promotion[SUBETHA_RING_FRAME_DEFAULT_BLOCK];
    uint8_t verdict[MIND_VERDICT_BYTES];
    uint64_t promotions = 0;
    uint64_t start = now_ns();
    for (uint32_t t = 0; t < turns && problems == 0; t++) {
        size_t len = MIND_SNAPSHOT_MIN + (t * 37) % (MIND_SNAPSHOT_MAX - MIND_SNAPSHOT_MIN);
        snapshot[0] = MIND_TAG_SNAPSHOT;
        put_u32(snapshot + 1, t);
        for (size_t k = 5; k < len; k++) {
            snapshot[k] = (uint8_t)(t + k);
        }
        rc = subetha_ring_send_frame_wait(raw, pid, snapshot, len, SUBETHA_LAYOUT_AUTO, RR_WAIT_MS, NULL);
        if (rc != SUBETHA_OK) {
            wl_detail("mind conscious", "send a snapshot", rc);
            problems++;
            break;
        }
        out->items++;
        out->bytes += len;
        for (;;) {
            size_t plen = 0;
            rc = subetha_ring_recv_frame(promo, cid, promotion, sizeof promotion, &plen);
            if (rc == SUBETHA_E_RING_EMPTY) {
                break;
            }
            if (rc != SUBETHA_OK) {
                wl_detail("mind conscious", "recv a promotion", rc);
                problems++;
                break;
            }
            if (plen < MIND_PROMOTION_MIN || plen > MIND_PROMOTION_MAX) {
                fprintf(stderr, "  mind conscious: a %zu-byte promotion\n", plen);
                problems++;
                break;
            }
            promotions++;
            out->bytes += plen;
            verdict[0] = MIND_TAG_VERDICT;
            memcpy(verdict + 1, promotion + 1, 8);
            put_u32(verdict + 9, 7);
            verdict[13] = 1;
            memset(verdict + 14, 0, 4);
            rc = subetha_ring_send_frame_wait(raw, pid, verdict, sizeof verdict, SUBETHA_LAYOUT_AUTO, RR_WAIT_MS, NULL);
            if (rc != SUBETHA_OK) {
                wl_detail("mind conscious", "send a verdict", rc);
                problems++;
                break;
            }
            out->bytes += sizeof verdict;
        }
    }
    /* Drain the promotions still in flight, each still earning its
     * verdict, then end the background. */
    uint64_t deadline = now_ns() + (uint64_t)RR_WAIT_MS * 1000000ull;
    while (promotions < out->items && problems == 0 && now_ns() < deadline) {
        size_t plen = 0;
        rc = subetha_ring_recv_frame_wait(promo, cid, promotion, sizeof promotion, &plen, 1);
        if (rc == SUBETHA_OK) {
            promotions++;
            out->bytes += plen;
            verdict[0] = MIND_TAG_VERDICT;
            memcpy(verdict + 1, promotion + 1, 8);
            put_u32(verdict + 9, 7);
            verdict[13] = 1;
            memset(verdict + 14, 0, 4);
            rc = subetha_ring_send_frame_wait(raw, pid, verdict, sizeof verdict, SUBETHA_LAYOUT_AUTO, RR_WAIT_MS, NULL);
            if (rc != SUBETHA_OK) {
                wl_detail("mind conscious", "send a late verdict", rc);
                problems++;
            }
            out->bytes += sizeof verdict;
        } else if (rc != SUBETHA_E_TIMEOUT) {
            wl_detail("mind conscious", "drain promotions", rc);
            problems++;
        }
    }
    if (promotions != out->items) {
        fprintf(stderr, "  mind conscious: %llu promotions for %llu snapshots\n", (unsigned long long)promotions,
                (unsigned long long)out->items);
        problems++;
    }
    rc = subetha_ring_send_frame_wait(raw, pid, snapshot, 0, SUBETHA_LAYOUT_AUTO, RR_WAIT_MS, NULL);
    if (rc != SUBETHA_OK) {
        wl_detail("mind conscious", "send the end", rc);
        problems++;
    }
    out->elapsed_ns = now_ns() - start;
    out->total_ns = promotions;
    return problems;
}

int subetha_workload_mind_subconscious(subetha_handle raw, subetha_handle promo, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint32_t pid = 0, cid = 0;
    int32_t rc = subetha_ring_register_consumer(raw, &cid);
    if (rc == SUBETHA_OK) {
        rc = subetha_ring_register_producer(promo, &pid);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("mind subconscious", "register", rc);
        return 1;
    }
    static uint8_t frame[SUBETHA_RING_FRAME_DEFAULT_BLOCK];
    static uint8_t promotion[MIND_PROMOTION_MAX];
    uint64_t verdicts = 0;
    uint64_t start = now_ns();
    int running = 1;
    while (running && problems == 0) {
        int arrived = 0;
        for (;;) {
            size_t len = 0;
            rc = subetha_ring_recv_frame(raw, cid, frame, sizeof frame, &len);
            if (rc == SUBETHA_E_RING_EMPTY) {
                break;
            }
            if (rc != SUBETHA_OK) {
                wl_detail("mind subconscious", "recv", rc);
                problems++;
                break;
            }
            arrived = 1;
            if (len == 0) {
                running = 0;
                break;
            }
            if (frame[0] == MIND_TAG_SNAPSHOT) {
                out->items++;
                out->bytes += len;
                size_t plen = MIND_PROMOTION_MIN + (out->items * 13) % (MIND_PROMOTION_MAX - MIND_PROMOTION_MIN);
                promotion[0] = 2;
                memcpy(promotion + 1, frame + 1, 4);
                memset(promotion + 5, 0, 4);
                for (size_t k = 9; k < plen; k++) {
                    promotion[k] = (uint8_t)('n' + k % 13);
                }
                rc = subetha_ring_send_frame_wait(promo, pid, promotion, plen, SUBETHA_LAYOUT_AUTO, RR_WAIT_MS, NULL);
                if (rc != SUBETHA_OK) {
                    wl_detail("mind subconscious", "send a promotion", rc);
                    problems++;
                    break;
                }
                out->bytes += plen;
            } else if (frame[0] == MIND_TAG_VERDICT && len == MIND_VERDICT_BYTES) {
                verdicts++;
            } else {
                fprintf(stderr, "  mind subconscious: a %zu-byte frame with tag %u\n", len, (unsigned)frame[0]);
                problems++;
                break;
            }
        }
        if (!arrived) {
            sleep_us(1000);
        }
    }
    out->elapsed_ns = now_ns() - start;
    out->total_ns = verdicts;
    return problems;
}

/* ------------------------------------------------------------- deque --
 * One file-backed deque per producer, 16384 slots of eight bytes, every
 * consumer stealing from all of them; both sides spin. A producer
 * pushes `items` packed values and retries a full deque without
 * sleeping; a consumer steals round-robin until every deque has been
 * pushed `items` times and is empty. */
#define DEQUE_CAPACITY 16384

static const subetha_element_layout deque_layout = {8, 8, 0x0064006900730070ULL};

int subetha_workload_deque_create(const char *path, uint32_t mode, subetha_handle *out)
{
    subetha_ring_options opt = ring_options(mode, 0, 0, NULL);
    int32_t rc = subetha_deque_create(path, DEQUE_CAPACITY, &deque_layout, &opt, out);
    if (rc != SUBETHA_OK) {
        wl_detail("deque producer", "create", rc);
        return 1;
    }
    return 0;
}

int subetha_workload_deque_open(const char *path, uint32_t mode, subetha_handle *out)
{
    subetha_ring_options opt = ring_options(mode, 0, 0, NULL);
    int32_t rc = subetha_deque_open_thief(path, &deque_layout, &opt, out);
    if (rc != SUBETHA_OK) {
        wl_detail("deque consumer", "open", rc);
        return 1;
    }
    return 0;
}

int subetha_workload_deque_produce(subetha_handle deque, uint32_t producer, uint32_t items, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint8_t value[8];
    uint64_t start = now_ns();
    for (uint32_t i = 0; i < items && problems == 0; i++) {
        uint64_t packed = ((uint64_t)producer << 32) | i;
        for (int b = 0; b < 8; b++) {
            value[b] = (uint8_t)(packed >> (8 * b));
        }
        for (;;) {
            int32_t rc = subetha_deque_try_push(deque, value, sizeof value);
            if (rc == SUBETHA_OK) {
                break;
            }
            if (rc != SUBETHA_E_RING_FULL) {
                wl_detail("deque producer", "push", rc);
                problems++;
                break;
            }
            out->refusals++;
        }
        out->items++;
        out->bytes += sizeof value;
    }
    out->elapsed_ns = now_ns() - start;
    return problems;
}

int subetha_workload_deque_consume(const subetha_handle *thieves, uint32_t count, uint32_t items_per_producer,
                                   subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint8_t value[8];
    uint64_t start = now_ns();
    for (;;) {
        int stole = 0;
        for (uint32_t d = 0; d < count; d++) {
            size_t len = 0;
            int32_t rc = subetha_deque_try_steal(thieves[d], value, sizeof value, &len);
            if (rc == SUBETHA_OK) {
                uint64_t packed = 0;
                for (int b = 0; b < 8; b++) {
                    packed |= (uint64_t)value[b] << (8 * b);
                }
                if ((uint32_t)packed >= items_per_producer) {
                    fprintf(stderr, "  deque consumer: index %u past %u\n", (unsigned)(uint32_t)packed,
                            (unsigned)items_per_producer);
                    problems++;
                }
                out->items++;
                out->bytes += sizeof value;
                stole = 1;
            } else if (rc != SUBETHA_E_RING_EMPTY) {
                wl_detail("deque consumer", "steal", rc);
                problems++;
            }
        }
        if (problems != 0) {
            break;
        }
        if (!stole) {
            out->retries++;
            int done = 1;
            for (uint32_t d = 0; d < count; d++) {
                subetha_deque_stats stats;
                if (subetha_deque_read_stats(thieves[d], &stats) != SUBETHA_OK) {
                    problems++;
                    done = 0;
                    break;
                }
                if (stats.bottom != (int64_t)items_per_producer || stats.approx_len != 0) {
                    done = 0;
                    break;
                }
            }
            if (done || problems != 0) {
                break;
            }
        }
    }
    out->elapsed_ns = now_ns() - start;
    return problems;
}

/* ------------------------------------------------------- shared helpers --
 * The store-shaped workloads below derive every byte they write from a
 * seed, so a reader in another process can check what it finds without
 * being told what was written. */

static uint64_t fnv1a64(const uint8_t *p, size_t len)
{
    uint64_t h = 0xcbf29ce484222325ull;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 0x100000001b3ull;
    }
    return h;
}

static void put_u64(uint8_t *p, uint64_t v)
{
    for (int b = 0; b < 8; b++) {
        p[b] = (uint8_t)(v >> (8 * b));
    }
}

static uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int b = 0; b < 8; b++) {
        v |= (uint64_t)p[b] << (8 * b);
    }
    return v;
}

/* The bytes a seed names: a length between `lo` and `hi` and a stream
 * of a 64-bit xorshift over the seed. */
static size_t seeded_bytes(uint64_t seed, uint8_t *out, size_t lo, size_t hi)
{
    uint64_t x = seed * 0x9E3779B97F4A7C15ull + 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    size_t len = lo + (size_t)(x % (uint64_t)(hi - lo + 1));
    for (size_t i = 0; i < len; i++) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out[i] = (uint8_t)x;
    }
    return len;
}

/* A nibble at a time, refusing anything that is not hex. */
static int hex_decode(const char *hex, uint8_t *out, size_t cap, size_t *out_len)
{
    size_t n = strlen(hex);
    if (n % 2 != 0 || n / 2 > cap) {
        return 0;
    }
    for (size_t i = 0; i < n; i += 2) {
        unsigned v = 0;
        for (int k = 0; k < 2; k++) {
            char c = hex[i + k];
            unsigned d;
            if (c >= '0' && c <= '9') {
                d = (unsigned)(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                d = (unsigned)(c - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
                d = (unsigned)(c - 'A' + 10);
            } else {
                return 0;
            }
            v = (v << 4) | d;
        }
        out[i / 2] = (uint8_t)v;
    }
    *out_len = n / 2;
    return 1;
}

/* ---------------------------------------------------------------- blob --
 * A content-addressed blob store: a hash map from the fnv-1a hash of a
 * blob to the arena reference holding it, several writer processes
 * storing overlapping blob sets, readers attached to the arena read-only
 * walking the map and hashing every blob they can reach. A writer looks
 * the hash up first and interns only what is absent; two writers that
 * intern the same blob at once both keep going, and the map keeps the
 * reference of whichever won. Blob `i` of writer `w` is shared by every
 * writer when `i % BLOB_SHARE == 0` and unique to `w` otherwise. */
#define BLOB_KEY_BYTES 8
#define BLOB_REF_BYTES 8
#define BLOB_MIN 100
#define BLOB_MAX 4000
#define BLOB_SHARE 5
#define BLOB_WAIT_MS 60000

static uint64_t blob_seed(uint32_t writer, uint32_t i)
{
    return i % BLOB_SHARE == 0 ? (uint64_t)i : (uint64_t)(writer + 1) * 1000003ull + i;
}

static uint32_t blob_map_capacity(uint32_t writers, uint32_t count)
{
    return writers * count * 2 + 64;
}

static uint64_t blob_arena_bytes(uint32_t writers, uint32_t count)
{
    return (uint64_t)writers * count * BLOB_MAX + (1u << 20);
}

/* Distinct blobs `writers` writers of `count` blobs each produce. */
static uint64_t blob_distinct(uint32_t writers, uint32_t count)
{
    uint32_t shared = (count + BLOB_SHARE - 1) / BLOB_SHARE;
    return (uint64_t)writers * count - (uint64_t)(writers - 1) * shared;
}

static void blob_paths(char *map, char *arena, size_t cap, const char *base)
{
    snprintf(map, cap, "%s_map", base);
    snprintf(arena, cap, "%s_arena", base);
}

/* Lay the store out, so every writer and reader attaches to files that
 * exist. */
int subetha_workload_blob_create(const char *base, uint32_t mode, uint32_t writers, uint32_t count)
{
    char map_path[512], arena_path[512];
    blob_paths(map_path, arena_path, sizeof map_path, base);
    subetha_handle map = SUBETHA_HANDLE_NONE, arena = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_hashmap_create(map_path, blob_map_capacity(writers, count), BLOB_KEY_BYTES, BLOB_REF_BYTES, mode,
                                        &map);
    if (rc != SUBETHA_OK) {
        wl_detail("blob create", "create the map", rc);
        return 1;
    }
    rc = subetha_arena_create(arena_path, blob_arena_bytes(writers, count), mode, &arena);
    if (rc != SUBETHA_OK) {
        wl_detail("blob create", "create the arena", rc);
        subetha_handle_destroy(map);
        return 1;
    }
    int problems = 0;
    if (subetha_handle_destroy(arena) != SUBETHA_OK || subetha_handle_destroy(map) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Store `count` blobs. items are blobs handled, bytes the ones interned,
 * refusals the blobs found already stored, retries the interns another
 * writer beat to the map. */
int subetha_workload_blob_writer(const char *base, uint32_t mode, uint32_t writer, uint32_t writers, uint32_t count,
                                 subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    char map_path[512], arena_path[512];
    blob_paths(map_path, arena_path, sizeof map_path, base);
    subetha_handle map = SUBETHA_HANDLE_NONE, arena = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_hashmap_open(map_path, blob_map_capacity(writers, count), BLOB_KEY_BYTES, BLOB_REF_BYTES, mode,
                                      &map);
    if (rc == SUBETHA_OK) {
        rc = subetha_arena_open(arena_path, blob_arena_bytes(writers, count), mode, &arena);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("blob writer", "open the store", rc);
        if (map != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(map);
        }
        return 1;
    }
    static uint8_t blob[BLOB_MAX];
    static uint8_t stored[BLOB_MAX];
    uint64_t start = now_ns();
    for (uint32_t i = 0; i < count && problems == 0; i++) {
        size_t len = seeded_bytes(blob_seed(writer, i), blob, BLOB_MIN, BLOB_MAX);
        uint8_t key[BLOB_KEY_BYTES], value[BLOB_REF_BYTES];
        put_u64(key, fnv1a64(blob, len));
        size_t got = 0;
        uint64_t t0 = now_ns();
        rc = subetha_hashmap_get(map, key, sizeof key, value, sizeof value, &got);
        if (rc == SUBETHA_OK) {
            /* Already stored, by this writer's twin or an earlier pass of
             * its own shared blobs: the reference must resolve to the
             * same bytes. */
            size_t stored_len = 0;
            rc = subetha_arena_get(arena, get_u64(value), stored, sizeof stored, &stored_len);
            if (rc != SUBETHA_OK) {
                wl_detail("blob writer", "resolve a stored blob", rc);
                problems++;
            } else if (stored_len != len || memcmp(stored, blob, len) != 0) {
                fprintf(stderr, "  blob writer %u: blob %u is stored as %zu bytes that differ from its %zu\n", (unsigned)writer,
                        (unsigned)i, stored_len, len);
                problems++;
            }
            out->refusals++;
        } else if (rc == SUBETHA_E_MAP_KEY_ABSENT) {
            uint64_t reference = 0;
            rc = subetha_arena_intern(arena, blob, len, &reference);
            if (rc != SUBETHA_OK) {
                wl_detail("blob writer", "intern a blob", rc);
                problems++;
                break;
            }
            out->bytes += len;
            put_u64(value, reference);
            uint8_t existing[BLOB_REF_BYTES];
            size_t existing_len = 0;
            bool present = false;
            rc = subetha_hashmap_insert_if_absent(map, key, sizeof key, value, sizeof value, existing, sizeof existing,
                                                  &existing_len, &present);
            if (rc != SUBETHA_OK) {
                wl_detail("blob writer", "publish a blob", rc);
                problems++;
                break;
            }
            if (present) {
                out->retries++;
            }
        } else {
            wl_detail("blob writer", "look a blob up", rc);
            problems++;
            break;
        }
        note_round_trip(out, now_ns() - t0);
        out->items++;
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(arena) != SUBETHA_OK || subetha_handle_destroy(map) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Walk the map and hash every blob it names until one walk finds every
 * distinct blob the writers produce, on an arena attached read-only.
 * items are entries verified over every walk, bytes the bytes hashed,
 * retries the walks it took. */
int subetha_workload_blob_reader(const char *base, uint32_t mode, uint32_t writers, uint32_t count,
                                 subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    char map_path[512], arena_path[512];
    blob_paths(map_path, arena_path, sizeof map_path, base);
    subetha_handle map = SUBETHA_HANDLE_NONE, arena = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_hashmap_open(map_path, blob_map_capacity(writers, count), BLOB_KEY_BYTES, BLOB_REF_BYTES, mode,
                                      &map);
    if (rc == SUBETHA_OK) {
        rc = subetha_arena_open_read_only(arena_path, blob_arena_bytes(writers, count), mode, &arena);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("blob reader", "open the store", rc);
        if (map != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(map);
        }
        return 1;
    }
    subetha_arena_stats astats;
    if (subetha_arena_read_stats(arena, &astats) != SUBETHA_OK || astats.writable) {
        fprintf(stderr, "  blob reader: the arena handle is not read-only\n");
        problems++;
    }
    uint64_t expected = blob_distinct(writers, count);
    uint64_t start = now_ns();
    uint64_t seen = 0;
    while (problems == 0 && seen < expected) {
        if (now_ns() - start > (uint64_t)BLOB_WAIT_MS * 1000000ull) {
            fprintf(stderr, "  blob reader: %llu of %llu blobs visible after %d ms\n", (unsigned long long)seen,
                    (unsigned long long)expected, BLOB_WAIT_MS);
            problems++;
            break;
        }
        seen = 0;
        uint64_t cursor = 0;
        for (;;) {
            uint8_t key[BLOB_KEY_BYTES], value[BLOB_REF_BYTES];
            size_t key_len = 0, value_len = 0;
            bool found = false;
            rc = subetha_hashmap_next(map, &cursor, key, sizeof key, &key_len, value, sizeof value, &value_len, &found);
            if (rc != SUBETHA_OK) {
                wl_detail("blob reader", "walk the map", rc);
                problems++;
                break;
            }
            if (!found) {
                break;
            }
            const uint8_t *data = NULL;
            size_t len = 0;
            uint64_t t0 = now_ns();
            rc = subetha_arena_view(arena, get_u64(value), &data, &len);
            if (rc != SUBETHA_OK) {
                wl_detail("blob reader", "view a blob", rc);
                problems++;
                break;
            }
            if (fnv1a64(data, len) != get_u64(key)) {
                fprintf(stderr, "  blob reader: a %zu-byte blob does not hash to its key\n", len);
                problems++;
                break;
            }
            note_round_trip(out, now_ns() - t0);
            out->bytes += len;
            out->items++;
            seen++;
        }
        out->retries++;
        if (seen < expected) {
            sleep_us(1000);
        }
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(arena) != SUBETHA_OK || subetha_handle_destroy(map) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* --------------------------------------------------------------- index --
 * A content index rebuilt whole, generation by generation, by whichever
 * indexer holds the owner lease: a vec of fixed records over an arena of
 * entry text, laid out with reset at a path the generation number names,
 * flushed, then published through a shared counter. Contenders tick the
 * lease's epoch while they wait, which is what would displace a holder
 * that died. Readers attach to a published generation read-only and check
 * every record against the text it names; a generation is never reset
 * under a reader because every generation has its own path. */
#define INDEX_RECORD_BYTES 24
#define INDEX_PAYLOAD_BYTES 16
#define INDEX_GRACE_TICKS 300
#define INDEX_TICK_US 10000
#define INDEX_BEAT_EVERY 64
#define INDEX_ENTRY_MIN 20
#define INDEX_ENTRY_MAX 200
#define INDEX_ACQUIRE_MS 60000
#define INDEX_DONE 0xFFFFFFFFu

static subetha_element_layout index_layout(void)
{
    subetha_element_layout layout = {.element_size = INDEX_RECORD_BYTES, .alignment = 8, .tag = 0x1DE};
    return layout;
}

static void index_paths(char *lease, char *gen, size_t cap, const char *base)
{
    snprintf(lease, cap, "%s.lease", base);
    snprintf(gen, cap, "%s.gen", base);
}

static void index_generation_paths(char *vec, char *arena, size_t cap, const char *base, uint32_t generation)
{
    snprintf(vec, cap, "%s_g%u.vec", base, (unsigned)generation);
    snprintf(arena, cap, "%s_g%u.arena", base, (unsigned)generation);
}

static uint64_t index_arena_bytes(uint32_t entries)
{
    return (uint64_t)entries * INDEX_ENTRY_MAX + 4096;
}

/* Lay the lease and the generation counter out. */
int subetha_workload_index_create(const char *base, uint32_t mode)
{
    char lease_path[512], gen_path[512];
    index_paths(lease_path, gen_path, sizeof lease_path, base);
    uint8_t initial[INDEX_PAYLOAD_BYTES] = {0};
    subetha_handle lease = SUBETHA_HANDLE_NONE, gen = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_owner_lease_create(lease_path, initial, sizeof initial, INDEX_PAYLOAD_BYTES, mode, &lease);
    if (rc != SUBETHA_OK) {
        wl_detail("index create", "create the lease", rc);
        return 1;
    }
    rc = subetha_atomic_u32_create(gen_path, 0, mode, &gen);
    if (rc != SUBETHA_OK) {
        wl_detail("index create", "create the generation counter", rc);
        subetha_handle_destroy(lease);
        return 1;
    }
    int problems = 0;
    if (subetha_handle_destroy(gen) != SUBETHA_OK || subetha_handle_destroy(lease) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Mark the index finished, so the readers stop waiting for another
 * generation. Returns the last lease term, which is the highest
 * generation number any indexer used. */
int subetha_workload_index_finish(const char *base, uint32_t mode, uint32_t *out_last_generation)
{
    char lease_path[512], gen_path[512];
    index_paths(lease_path, gen_path, sizeof lease_path, base);
    subetha_handle lease = SUBETHA_HANDLE_NONE, gen = SUBETHA_HANDLE_NONE;
    int problems = 0;
    int32_t rc = subetha_atomic_u32_open(gen_path, mode, &gen);
    if (rc == SUBETHA_OK) {
        rc = subetha_atomic_u32_store(gen, INDEX_DONE);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("index finish", "mark the counter done", rc);
        problems++;
    }
    rc = subetha_owner_lease_open(lease_path, INDEX_PAYLOAD_BYTES, mode, &lease);
    if (rc == SUBETHA_OK) {
        subetha_owner_lease_stats stats;
        rc = subetha_owner_lease_read_stats(lease, &stats);
        if (rc == SUBETHA_OK) {
            *out_last_generation = stats.lease_term + 1;
        }
    }
    if (rc != SUBETHA_OK) {
        wl_detail("index finish", "read the last term", rc);
        problems++;
    }
    if (gen != SUBETHA_HANDLE_NONE && subetha_handle_destroy(gen) != SUBETHA_OK) {
        problems++;
    }
    if (lease != SUBETHA_HANDLE_NONE && subetha_handle_destroy(lease) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Build `generations` generations of `entries` records each, holding the
 * lease for each build. items are records written, bytes the text
 * interned, retries the acquire attempts that found the lease held,
 * refusals the builds abandoned because the lease was lost, before the
 * build began or in the middle of it. */
int subetha_workload_index_writer(const char *base, uint32_t mode, uint32_t entries, uint32_t generations,
                                  subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    char lease_path[512], gen_path[512];
    index_paths(lease_path, gen_path, sizeof lease_path, base);
    subetha_handle lease = SUBETHA_HANDLE_NONE, gen = SUBETHA_HANDLE_NONE;
    uint32_t pid = 0;
    int32_t rc = subetha_current_pid(&pid);
    if (rc == SUBETHA_OK) {
        rc = subetha_owner_lease_open(lease_path, INDEX_PAYLOAD_BYTES, mode, &lease);
    }
    if (rc == SUBETHA_OK) {
        rc = subetha_atomic_u32_open(gen_path, mode, &gen);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("index writer", "open the lease", rc);
        if (lease != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(lease);
        }
        return 1;
    }
    subetha_element_layout layout = index_layout();
    static uint8_t text[INDEX_ENTRY_MAX];
    uint64_t start = now_ns();
    uint32_t built = 0;
    while (built < generations && problems == 0) {
        bool got = false;
        uint64_t waited_from = now_ns();
        for (;;) {
            rc = subetha_owner_lease_try_acquire(lease, pid, INDEX_GRACE_TICKS, &got);
            if (rc != SUBETHA_OK) {
                wl_detail("index writer", "acquire the lease", rc);
                problems++;
                break;
            }
            if (got) {
                break;
            }
            out->retries++;
            uint64_t epoch = 0;
            if ((rc = subetha_owner_lease_tick_epoch(lease, &epoch)) != SUBETHA_OK) {
                wl_detail("index writer", "tick the epoch", rc);
                problems++;
                break;
            }
            if (now_ns() - waited_from > (uint64_t)INDEX_ACQUIRE_MS * 1000000ull) {
                fprintf(stderr, "  index writer %u: no lease after %d ms\n", (unsigned)pid, INDEX_ACQUIRE_MS);
                problems++;
                break;
            }
            sleep_us(INDEX_TICK_US);
        }
        if (problems != 0) {
            break;
        }
        note_round_trip(out, now_ns() - waited_from);
        subetha_owner_lease_stats lstats;
        if ((rc = subetha_owner_lease_read_stats(lease, &lstats)) != SUBETHA_OK) {
            wl_detail("index writer", "read the term", rc);
            problems++;
            break;
        }
        /* The term is read in a call of its own, and a lower pid takes the
         * lease whenever it asks, so the lease can change hands between the
         * acquire and this read; the term read is then the taker's, and two
         * indexers would lay out one generation. Ownership confirmed after
         * the read settles whose term it was: the owner becomes this pid
         * again only through this pid's own acquire, so a term read while
         * this pid still holds the lease is the term it took. */
        subetha_owner_lease_stats confirm;
        if ((rc = subetha_owner_lease_read_stats(lease, &confirm)) != SUBETHA_OK) {
            wl_detail("index writer", "confirm the term", rc);
            problems++;
            break;
        }
        if (confirm.owner_pid != pid) {
            out->refusals++;
            continue;
        }
        /* Terms count from the first holder, so a generation number is
         * a term plus one and never the counter's initial zero. */
        uint32_t generation = lstats.lease_term + 1;
        char vec_path[512], arena_path[512];
        index_generation_paths(vec_path, arena_path, sizeof vec_path, base, generation);
        subetha_handle vec = SUBETHA_HANDLE_NONE, arena = SUBETHA_HANDLE_NONE;
        rc = subetha_vec_reset(vec_path, entries, &layout, mode, &vec);
        if (rc == SUBETHA_OK) {
            rc = subetha_arena_reset(arena_path, index_arena_bytes(entries), mode, &arena);
        }
        if (rc != SUBETHA_OK) {
            wl_detail("index writer", "lay a generation out", rc);
            problems++;
            if (vec != SUBETHA_HANDLE_NONE) {
                subetha_handle_destroy(vec);
            }
            break;
        }
        int lost = 0;
        for (uint32_t e = 0; e < entries && problems == 0 && !lost; e++) {
            size_t len = seeded_bytes((uint64_t)generation * 1000000ull + e, text, INDEX_ENTRY_MIN, INDEX_ENTRY_MAX);
            uint64_t reference = 0;
            if ((rc = subetha_arena_intern(arena, text, len, &reference)) != SUBETHA_OK) {
                wl_detail("index writer", "intern an entry", rc);
                problems++;
                break;
            }
            uint8_t record[INDEX_RECORD_BYTES];
            put_u64(record, reference);
            put_u64(record + 8, fnv1a64(text, len));
            put_u32(record + 16, (uint32_t)len);
            put_u32(record + 20, generation);
            uint64_t index = 0;
            if ((rc = subetha_vec_push_back(vec, record, sizeof record, &index)) != SUBETHA_OK) {
                wl_detail("index writer", "push a record", rc);
                problems++;
                break;
            }
            if (index != e) {
                fprintf(stderr, "  index writer: record %u landed at %llu\n", (unsigned)e, (unsigned long long)index);
                problems++;
                break;
            }
            out->bytes += len;
            if (e % INDEX_BEAT_EVERY == INDEX_BEAT_EVERY - 1) {
                bool still = false;
                if ((rc = subetha_owner_lease_beat(lease, pid, &still)) != SUBETHA_OK) {
                    wl_detail("index writer", "beat", rc);
                    problems++;
                    break;
                }
                if (!still) {
                    lost = 1;
                }
            }
        }
        if (problems == 0 && !lost) {
            if (subetha_vec_flush(vec) != SUBETHA_OK || subetha_arena_flush(arena) != SUBETHA_OK) {
                fprintf(stderr, "  index writer: a generation did not flush\n");
                problems++;
            }
        }
        if (problems == 0 && !lost) {
            uint8_t payload[INDEX_PAYLOAD_BYTES];
            put_u64(payload, generation);
            put_u64(payload + 8, entries);
            rc = subetha_owner_lease_write(lease, pid, payload, sizeof payload);
            if (rc == SUBETHA_E_NOT_OWNER) {
                lost = 1;
            } else if (rc != SUBETHA_OK) {
                wl_detail("index writer", "write the payload", rc);
                problems++;
            } else if ((rc = subetha_atomic_u32_store(gen, generation)) != SUBETHA_OK) {
                wl_detail("index writer", "publish the generation", rc);
                problems++;
            } else {
                out->items += entries;
                built++;
            }
        }
        if (lost) {
            out->refusals++;
        }
        if (subetha_handle_destroy(arena) != SUBETHA_OK || subetha_handle_destroy(vec) != SUBETHA_OK) {
            problems++;
        }
        bool released = false;
        if ((rc = subetha_owner_lease_release(lease, pid, &released)) != SUBETHA_OK) {
            wl_detail("index writer", "release the lease", rc);
            problems++;
        }
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(gen) != SUBETHA_OK || subetha_handle_destroy(lease) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Check every generation published until the counter says the index is
 * finished. items are records verified, bytes the text hashed, retries
 * the polls that found no new generation, total_ns the generations
 * verified. */
int subetha_workload_index_reader(const char *base, uint32_t mode, uint32_t entries, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    char lease_path[512], gen_path[512];
    index_paths(lease_path, gen_path, sizeof lease_path, base);
    subetha_handle gen = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_atomic_u32_open(gen_path, mode, &gen);
    if (rc != SUBETHA_OK) {
        wl_detail("index reader", "open the generation counter", rc);
        return 1;
    }
    subetha_element_layout layout = index_layout();
    uint64_t start = now_ns();
    uint32_t last = 0;
    for (;;) {
        uint32_t current = 0;
        if ((rc = subetha_atomic_u32_load(gen, &current)) != SUBETHA_OK) {
            wl_detail("index reader", "read the counter", rc);
            problems++;
            break;
        }
        if (current == INDEX_DONE) {
            break;
        }
        if (current == last) {
            out->retries++;
            sleep_us(1000);
            continue;
        }
        last = current;
        char vec_path[512], arena_path[512];
        index_generation_paths(vec_path, arena_path, sizeof vec_path, base, current);
        subetha_handle vec = SUBETHA_HANDLE_NONE, arena = SUBETHA_HANDLE_NONE;
        uint64_t t0 = now_ns();
        rc = subetha_vec_open_read_only(vec_path, entries, &layout, mode, &vec);
        if (rc == SUBETHA_OK) {
            rc = subetha_arena_open_read_only(arena_path, index_arena_bytes(entries), mode, &arena);
        }
        if (rc != SUBETHA_OK) {
            wl_detail("index reader", "open a generation", rc);
            problems++;
            if (vec != SUBETHA_HANDLE_NONE) {
                subetha_handle_destroy(vec);
            }
            break;
        }
        subetha_vec_stats vstats;
        if ((rc = subetha_vec_read_stats(vec, &vstats)) != SUBETHA_OK) {
            wl_detail("index reader", "read the vec", rc);
            problems++;
        } else if (vstats.len != entries || vstats.writable) {
            fprintf(stderr, "  index reader: generation %u holds %llu records, writable=%d\n", (unsigned)current,
                    (unsigned long long)vstats.len, (int)vstats.writable);
            problems++;
        }
        for (uint64_t i = 0; i < entries && problems == 0; i++) {
            uint8_t record[INDEX_RECORD_BYTES];
            size_t record_len = 0;
            if ((rc = subetha_vec_get(vec, i, record, sizeof record, &record_len)) != SUBETHA_OK) {
                wl_detail("index reader", "read a record", rc);
                problems++;
                break;
            }
            const uint8_t *data = NULL;
            size_t len = 0;
            if ((rc = subetha_arena_view(arena, get_u64(record), &data, &len)) != SUBETHA_OK) {
                wl_detail("index reader", "view an entry", rc);
                problems++;
                break;
            }
            if (len != get_u32(record + 16) || fnv1a64(data, len) != get_u64(record + 8) || get_u32(record + 20) != current) {
                fprintf(stderr, "  index reader: record %llu of generation %u does not match its entry\n",
                        (unsigned long long)i, (unsigned)current);
                problems++;
                break;
            }
            out->bytes += len;
            out->items++;
        }
        /* total_ns carries the generations verified, so a generation's
         * own time goes to worst_ns alone. */
        uint64_t took = now_ns() - t0;
        if (took > out->worst_ns) {
            out->worst_ns = took;
        }
        out->total_ns++;
        if (subetha_handle_destroy(arena) != SUBETHA_OK || subetha_handle_destroy(vec) != SUBETHA_OK) {
            problems++;
        }
        if (problems != 0) {
            break;
        }
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(gen) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* ---------------------------------------------------------------- mvcc --
 * A multi-version index: a versioned map whose writers keep updating,
 * removing and re-inserting their own keys while readers scan it under a
 * pin from the shared epoch table, page by page in key order. The tree
 * beneath the map takes one writer at a time, so the writers hold the
 * write side of a shared lock around each change; readers hold nothing,
 * since the pin is what keeps their view. A re-insert over a tombstone a
 * pin still reaches is refused, and the writer sweeps and retries; a full
 * tree is swept the same way. Keys are 16 bytes, the key number
 * big-endian in the first 8 so byte order is key order; values are 64
 * bytes carrying the key, the round, the writer and a hash of the three. */
#define MVCC_KEY_BYTES 16
#define MVCC_VALUE_BYTES 64
#define MVCC_MAX_PINS 16
#define MVCC_PAGE 64
#define MVCC_REMOVE_EVERY 7
#define MVCC_RETRY_US 500
#define MVCC_RETRY_MS 30000
#define MVCC_LOCK_MS 30000

static void mvcc_paths(char *tree, char *epochs, char *lock, size_t cap, const char *base)
{
    snprintf(tree, cap, "%s_tree", base);
    snprintf(epochs, cap, "%s_epochs", base);
    snprintf(lock, cap, "%s_wlock", base);
}

/* Take the write side of the writers' lock, waiting; a timeout is a
 * problem the caller reports. */
static int32_t mvcc_write_hold(subetha_handle lock, uint64_t *hold)
{
    return subetha_rwlock_write(lock, MVCC_LOCK_MS, hold);
}

static uint64_t mvcc_nodes(uint32_t writers, uint32_t keys)
{
    return (uint64_t)writers * keys / 4 + 128;
}

static void mvcc_key(uint8_t *out, uint64_t k)
{
    memset(out, 0, MVCC_KEY_BYTES);
    for (int b = 0; b < 8; b++) {
        out[b] = (uint8_t)(k >> (8 * (7 - b)));
    }
}

static uint64_t mvcc_key_number(const uint8_t *key)
{
    uint64_t k = 0;
    for (int b = 0; b < 8; b++) {
        k = (k << 8) | key[b];
    }
    return k;
}

static void mvcc_value(uint8_t *out, uint64_t k, uint64_t round, uint32_t writer)
{
    memset(out, 0, MVCC_VALUE_BYTES);
    put_u64(out, k);
    put_u64(out + 8, round);
    put_u32(out + 16, writer);
    put_u64(out + 24, fnv1a64(out, 24));
}

static int mvcc_value_ok(const uint8_t *value, uint64_t k)
{
    return get_u64(value) == k && fnv1a64(value, 24) == get_u64(value + 24);
}

/* Lay the map, its epoch table and the writers' lock out. */
int subetha_workload_mvcc_create(const char *base, uint32_t mode, uint32_t writers, uint32_t keys)
{
    char tree_path[512], epochs_path[512], lock_path[512];
    mvcc_paths(tree_path, epochs_path, lock_path, sizeof tree_path, base);
    subetha_handle map = SUBETHA_HANDLE_NONE, lock = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_versioned_map_create(tree_path, mvcc_nodes(writers, keys), MVCC_KEY_BYTES, MVCC_VALUE_BYTES,
                                              epochs_path, MVCC_MAX_PINS, mode, &map);
    if (rc != SUBETHA_OK) {
        wl_detail("mvcc create", "create the map", rc);
        return 1;
    }
    rc = subetha_rwlock_create(lock_path, mode, &lock);
    if (rc != SUBETHA_OK) {
        wl_detail("mvcc create", "create the writers' lock", rc);
        subetha_handle_destroy(map);
        return 1;
    }
    int problems = 0;
    if (subetha_handle_destroy(lock) != SUBETHA_OK || subetha_handle_destroy(map) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* `rounds` rounds over this writer's `keys` keys. items are inserts,
 * refusals removes, retries the inserts refused for a reachable
 * tombstone or a full tree and tried again after a sweep, total_ns the
 * tombstones swept. */
int subetha_workload_mvcc_writer(const char *base, uint32_t mode, uint32_t writer, uint32_t writers, uint32_t keys,
                                 uint32_t rounds, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    char tree_path[512], epochs_path[512], lock_path[512];
    mvcc_paths(tree_path, epochs_path, lock_path, sizeof tree_path, base);
    subetha_handle map = SUBETHA_HANDLE_NONE, lock = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_versioned_map_open(tree_path, mvcc_nodes(writers, keys), MVCC_KEY_BYTES, MVCC_VALUE_BYTES,
                                            epochs_path, MVCC_MAX_PINS, mode, &map);
    if (rc == SUBETHA_OK) {
        rc = subetha_rwlock_open(lock_path, mode, &lock);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("mvcc writer", "open the map", rc);
        if (map != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(map);
        }
        return 1;
    }
    uint64_t first = (uint64_t)writer * keys;
    uint64_t start = now_ns();
    for (uint32_t round = 0; round < rounds && problems == 0; round++) {
        for (uint32_t i = 0; i < keys && problems == 0; i++) {
            uint64_t k = first + i;
            uint8_t key[MVCC_KEY_BYTES], value[MVCC_VALUE_BYTES];
            mvcc_key(key, k);
            uint64_t hold = 0;
            /* Every seventh key is removed on an odd round and comes back
             * on the next, which is what meets a reader's pin. */
            if (round % 2 == 1 && i % MVCC_REMOVE_EVERY == 0) {
                uint8_t previous[MVCC_VALUE_BYTES];
                size_t previous_len = 0;
                if ((rc = mvcc_write_hold(lock, &hold)) != SUBETHA_OK) {
                    wl_detail("mvcc writer", "take the write hold", rc);
                    problems++;
                    break;
                }
                rc = subetha_versioned_map_remove(map, key, sizeof key, previous, sizeof previous, &previous_len);
                if (subetha_rwlock_unlock(lock, hold) != SUBETHA_OK) {
                    fprintf(stderr, "  mvcc writer %u: the write hold did not release\n", (unsigned)writer);
                    problems++;
                    break;
                }
                if (rc != SUBETHA_OK) {
                    wl_detail("mvcc writer", "remove a key", rc);
                    problems++;
                    break;
                }
                if (!mvcc_value_ok(previous, k)) {
                    fprintf(stderr, "  mvcc writer %u: key %llu held a value that is not its own\n", (unsigned)writer,
                            (unsigned long long)k);
                    problems++;
                    break;
                }
                out->refusals++;
                continue;
            }
            mvcc_value(value, k, round, writer);
            uint64_t t0 = now_ns();
            for (;;) {
                if ((rc = mvcc_write_hold(lock, &hold)) != SUBETHA_OK) {
                    wl_detail("mvcc writer", "take the write hold", rc);
                    problems++;
                    break;
                }
                rc = subetha_versioned_map_insert(map, key, sizeof key, value, sizeof value);
                int32_t swept = SUBETHA_OK;
                if (rc == SUBETHA_E_WOULD_BLOCK || rc == SUBETHA_E_RING_FULL || rc == SUBETHA_E_MAP_FULL) {
                    uint64_t freed = 0;
                    swept = subetha_versioned_map_sweep(map, &freed);
                }
                if (subetha_rwlock_unlock(lock, hold) != SUBETHA_OK) {
                    fprintf(stderr, "  mvcc writer %u: the write hold did not release\n", (unsigned)writer);
                    problems++;
                    break;
                }
                if (rc == SUBETHA_OK) {
                    break;
                }
                if (rc != SUBETHA_E_WOULD_BLOCK && rc != SUBETHA_E_RING_FULL && rc != SUBETHA_E_MAP_FULL) {
                    wl_detail("mvcc writer", "insert a key", rc);
                    problems++;
                    break;
                }
                out->retries++;
                if (swept != SUBETHA_OK && swept != SUBETHA_E_RING_IO) {
                    wl_detail("mvcc writer", "sweep", swept);
                    problems++;
                    break;
                }
                if (now_ns() - t0 > (uint64_t)MVCC_RETRY_MS * 1000000ull) {
                    fprintf(stderr, "  mvcc writer %u: key %llu refused for %d ms\n", (unsigned)writer,
                            (unsigned long long)k, MVCC_RETRY_MS);
                    problems++;
                    break;
                }
                sleep_us(MVCC_RETRY_US);
            }
            if (problems != 0) {
                break;
            }
            note_round_trip(out, now_ns() - t0);
            out->items++;
            out->bytes += MVCC_VALUE_BYTES;
        }
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(lock) != SUBETHA_OK || subetha_handle_destroy(map) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* `scans` full scans of a map with entries in it, each under one pin,
 * paged MVCC_PAGE entries at a time; a scan that finds the map still
 * empty is not one of them, since the readers start before the writers
 * and what they are for is the map changing under them. items are
 * entries checked, retries pages, bytes entry bytes copied out. */
int subetha_workload_mvcc_reader(const char *base, uint32_t mode, uint32_t writers, uint32_t keys, uint32_t scans,
                                 subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    char tree_path[512], epochs_path[512], lock_path[512];
    mvcc_paths(tree_path, epochs_path, lock_path, sizeof tree_path, base);
    subetha_handle map = SUBETHA_HANDLE_NONE, epochs = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_versioned_map_open(tree_path, mvcc_nodes(writers, keys), MVCC_KEY_BYTES, MVCC_VALUE_BYTES,
                                            epochs_path, MVCC_MAX_PINS, mode, &map);
    if (rc == SUBETHA_OK) {
        rc = subetha_epochs_open(epochs_path, MVCC_MAX_PINS, mode, &epochs);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("mvcc reader", "open the map", rc);
        if (map != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(map);
        }
        return 1;
    }
    static uint8_t page[MVCC_PAGE * (MVCC_KEY_BYTES + MVCC_VALUE_BYTES)];
    uint64_t total_keys = (uint64_t)writers * keys;
    uint64_t start = now_ns();
    uint32_t done = 0;
    while (done < scans && problems == 0) {
        uint64_t pin = 0;
        if ((rc = subetha_epochs_pin(epochs, &pin)) != SUBETHA_OK) {
            wl_detail("mvcc reader", "pin", rc);
            problems++;
            break;
        }
        uint64_t t0 = now_ns();
        uint8_t low[MVCC_KEY_BYTES];
        const uint8_t *from = NULL;
        uint64_t seen = 0;
        int64_t last_key = -1;
        for (;;) {
            size_t len = 0, count = 0;
            rc = subetha_versioned_map_range(map, from, NULL, MVCC_PAGE, page, sizeof page, &len, &count);
            if (rc != SUBETHA_OK) {
                wl_detail("mvcc reader", "scan a page", rc);
                problems++;
                break;
            }
            out->retries++;
            out->bytes += len;
            for (size_t e = 0; e < count; e++) {
                const uint8_t *entry = page + e * (MVCC_KEY_BYTES + MVCC_VALUE_BYTES);
                uint64_t k = mvcc_key_number(entry);
                if ((int64_t)k <= last_key || k >= total_keys || !mvcc_value_ok(entry + MVCC_KEY_BYTES, k)) {
                    fprintf(stderr, "  mvcc reader: entry %llu after %lld is out of order, out of range or not its own\n",
                            (unsigned long long)k, (long long)last_key);
                    problems++;
                    break;
                }
                last_key = (int64_t)k;
                seen++;
                out->items++;
            }
            if (problems != 0 || count == 0 || last_key + 1 >= (int64_t)total_keys) {
                break;
            }
            /* A limit counts entries examined, so a page dense in
             * tombstones is short without being the last: the scan ends
             * at an empty page or the last key, and resumes past the last
             * key it saw. An empty page is the end only while tombstones
             * stay sparser than a page, which one key in seven is. */
            mvcc_key(low, (uint64_t)last_key + 1);
            from = low;
        }
        if (problems == 0 && seen > total_keys) {
            fprintf(stderr, "  mvcc reader: a scan saw %llu entries of %llu keys\n", (unsigned long long)seen,
                    (unsigned long long)total_keys);
            problems++;
        }
        note_round_trip(out, now_ns() - t0);
        if ((rc = subetha_pin_release(epochs, pin)) != SUBETHA_OK) {
            wl_detail("mvcc reader", "release the pin", rc);
            problems++;
        }
        if (seen > 0) {
            done++;
        } else if (now_ns() - start > (uint64_t)MVCC_RETRY_MS * 1000000ull) {
            fprintf(stderr, "  mvcc reader: the map stayed empty for %d ms\n", MVCC_RETRY_MS);
            problems++;
        } else {
            sleep_us(1000);
        }
    }
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(epochs) != SUBETHA_OK || subetha_handle_destroy(map) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* --------------------------------------------------------------- graph --
 * A graph store on a frame region: node `n` is block `n`, the head of a
 * chain of edge pages linked through the region, and every page carries a
 * version word a writer makes odd before it rewrites the page and even
 * after, so a reader that copies a page under a write sees the odd
 * version or a changed one and reads again. A writer owns a range of
 * nodes and appends edges to them, taking overflow pages from the region
 * as a page fills, then prunes each chain's last page back to the region,
 * unlinking it first so a reader that copied the previous page finds the
 * chain ended. Readers walk every chain and check every edge. */
#define GRAPH_BLOCK 256
#define GRAPH_PAGE_HEADER 16
#define GRAPH_EDGES_PER_PAGE ((GRAPH_BLOCK - GRAPH_PAGE_HEADER) / 4)
#define GRAPH_RETRY_LIMIT 20000
#define GRAPH_RETRY_SPINS 8
#define GRAPH_RETRY_SLEEP_US 100

static void graph_page_header(uint8_t *page, uint32_t version, uint32_t count, uint32_t next, uint32_t node)
{
    put_u32(page, version);
    put_u32(page + 4, count);
    put_u32(page + 8, next);
    put_u32(page + 12, node);
}

/* Rewrite a page so a concurrent reader sees the change whole: the
 * version goes odd, the page follows, the version goes even. */
static int32_t graph_page_update(subetha_handle region, uint32_t block, uint8_t *page)
{
    uint32_t version = get_u32(page);
    uint8_t odd[4];
    put_u32(odd, version + 1);
    int32_t rc = subetha_frame_region_write(region, block, odd, sizeof odd);
    if (rc != SUBETHA_OK) {
        return rc;
    }
    put_u32(page, version + 1);
    rc = subetha_frame_region_write(region, block, page, GRAPH_BLOCK);
    if (rc != SUBETHA_OK) {
        return rc;
    }
    put_u32(page, version + 2);
    return subetha_frame_region_write(region, block, page, 4);
}

/* Copy a page that is not mid-write: 1 when `page` holds one, 0 when the
 * read should be tried again, -1 on an error the caller reports. */
static int graph_page_read(subetha_handle region, uint32_t block, uint8_t *page, uint32_t block_count)
{
    uint8_t before[4], after[4];
    size_t len = 0;
    if (subetha_frame_region_read(region, block, sizeof before, before, sizeof before, &len) != SUBETHA_OK) {
        return -1;
    }
    if (get_u32(before) % 2 == 1) {
        return 0;
    }
    if (subetha_frame_region_read(region, block, GRAPH_BLOCK, page, GRAPH_BLOCK, &len) != SUBETHA_OK) {
        return -1;
    }
    if (subetha_frame_region_read(region, block, sizeof after, after, sizeof after, &len) != SUBETHA_OK) {
        return -1;
    }
    if (get_u32(page) != get_u32(before) || get_u32(after) != get_u32(before)) {
        return 0;
    }
    uint32_t next = get_u32(page + 8);
    if (get_u32(page + 4) > GRAPH_EDGES_PER_PAGE || (next != SUBETHA_FRAME_NO_BLOCK && next >= block_count)) {
        return 0;
    }
    return 1;
}

static uint32_t graph_target(uint32_t node, uint32_t edge, uint32_t nodes)
{
    uint64_t x = ((uint64_t)node << 32 | edge) * 0x9E3779B97F4A7C15ull;
    x ^= x >> 29;
    return (uint32_t)(x % nodes);
}

/* Lay the region out with one head page per node. */
int subetha_workload_graph_create(const char *path, uint32_t mode, uint32_t nodes, uint32_t block_count)
{
    subetha_handle region = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_frame_region_create(path, GRAPH_BLOCK, block_count, mode, &region);
    if (rc != SUBETHA_OK) {
        wl_detail("graph create", "create the region", rc);
        return 1;
    }
    int problems = 0;
    static uint8_t page[GRAPH_BLOCK];
    for (uint32_t n = 0; n < nodes && problems == 0; n++) {
        uint32_t block = 0;
        if ((rc = subetha_frame_region_alloc(region, &block)) != SUBETHA_OK) {
            wl_detail("graph create", "take a head page", rc);
            problems++;
            break;
        }
        if (block != n) {
            fprintf(stderr, "  graph create: node %u took block %u\n", (unsigned)n, (unsigned)block);
            problems++;
            break;
        }
        memset(page, 0, sizeof page);
        graph_page_header(page, 0, 0, SUBETHA_FRAME_NO_BLOCK, n);
        if ((rc = subetha_frame_region_write(region, block, page, sizeof page)) != SUBETHA_OK) {
            wl_detail("graph create", "write a head page", rc);
            problems++;
        }
    }
    if (subetha_handle_destroy(region) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Append `edges_per_node` edges to each of this writer's nodes, then
 * prune each chain's last page. items are edges added, total_ns edges
 * pruned, retries the overflow pages taken. */
int subetha_workload_graph_writer(const char *path, uint32_t mode, uint32_t writer, uint32_t writers, uint32_t nodes,
                                  uint32_t block_count, uint32_t edges_per_node, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    subetha_handle region = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_frame_region_open(path, GRAPH_BLOCK, block_count, mode, &region);
    if (rc != SUBETHA_OK) {
        wl_detail("graph writer", "open the region", rc);
        return 1;
    }
    uint32_t first = writer * (nodes / writers);
    uint32_t last = writer + 1 == writers ? nodes : first + nodes / writers;
    uint32_t *tail = calloc(last - first, sizeof *tail);
    uint32_t *before_tail = calloc(last - first, sizeof *before_tail);
    if (tail == NULL || before_tail == NULL) {
        fprintf(stderr, "  graph writer: no memory for %u nodes\n", (unsigned)(last - first));
        free(tail);
        free(before_tail);
        subetha_handle_destroy(region);
        return 1;
    }
    static uint8_t page[GRAPH_BLOCK];
    static uint8_t fresh[GRAPH_BLOCK];
    uint64_t start = now_ns();
    for (uint32_t n = first; n < last; n++) {
        tail[n - first] = n;
        before_tail[n - first] = SUBETHA_FRAME_NO_BLOCK;
    }
    for (uint32_t e = 0; e < edges_per_node && problems == 0; e++) {
        for (uint32_t n = first; n < last && problems == 0; n++) {
            uint32_t block = tail[n - first];
            size_t len = 0;
            if ((rc = subetha_frame_region_read(region, block, GRAPH_BLOCK, page, sizeof page, &len)) != SUBETHA_OK) {
                wl_detail("graph writer", "read the tail page", rc);
                problems++;
                break;
            }
            uint32_t count = get_u32(page + 4);
            uint64_t t0 = now_ns();
            if (count == GRAPH_EDGES_PER_PAGE) {
                uint32_t next = 0;
                if ((rc = subetha_frame_region_alloc(region, &next)) != SUBETHA_OK) {
                    wl_detail("graph writer", "take an overflow page", rc);
                    problems++;
                    break;
                }
                memset(fresh, 0, sizeof fresh);
                graph_page_header(fresh, 0, 0, SUBETHA_FRAME_NO_BLOCK, n);
                if ((rc = subetha_frame_region_write(region, next, fresh, sizeof fresh)) != SUBETHA_OK) {
                    wl_detail("graph writer", "write an overflow page", rc);
                    problems++;
                    break;
                }
                put_u32(page + 8, next);
                if ((rc = graph_page_update(region, block, page)) != SUBETHA_OK) {
                    wl_detail("graph writer", "link an overflow page", rc);
                    problems++;
                    break;
                }
                before_tail[n - first] = block;
                tail[n - first] = next;
                block = next;
                memcpy(page, fresh, sizeof page);
                count = 0;
                out->retries++;
            }
            put_u32(page + GRAPH_PAGE_HEADER + 4 * count, graph_target(n, e, nodes));
            put_u32(page + 4, count + 1);
            if ((rc = graph_page_update(region, block, page)) != SUBETHA_OK) {
                wl_detail("graph writer", "append an edge", rc);
                problems++;
                break;
            }
            /* total_ns carries the pruned count, so the append's own time
             * goes to worst_ns alone. */
            uint64_t took = now_ns() - t0;
            if (took > out->worst_ns) {
                out->worst_ns = took;
            }
            out->items++;
            out->bytes += 4;
        }
    }
    for (uint32_t n = first; n < last && problems == 0; n++) {
        uint32_t previous = before_tail[n - first];
        if (previous == SUBETHA_FRAME_NO_BLOCK) {
            continue;
        }
        uint32_t pruned = tail[n - first];
        size_t len = 0;
        if ((rc = subetha_frame_region_read(region, pruned, GRAPH_BLOCK, page, sizeof page, &len)) != SUBETHA_OK) {
            wl_detail("graph writer", "read the page to prune", rc);
            problems++;
            break;
        }
        uint32_t pruned_edges = get_u32(page + 4);
        if ((rc = subetha_frame_region_read(region, previous, GRAPH_BLOCK, page, sizeof page, &len)) != SUBETHA_OK) {
            wl_detail("graph writer", "read the page before it", rc);
            problems++;
            break;
        }
        put_u32(page + 8, SUBETHA_FRAME_NO_BLOCK);
        if ((rc = graph_page_update(region, previous, page)) != SUBETHA_OK) {
            wl_detail("graph writer", "unlink the last page", rc);
            problems++;
            break;
        }
        if ((rc = subetha_frame_region_free(region, pruned)) != SUBETHA_OK) {
            wl_detail("graph writer", "free the last page", rc);
            problems++;
            break;
        }
        out->total_ns += pruned_edges;
    }
    out->elapsed_ns = now_ns() - start;
    free(tail);
    free(before_tail);
    if (subetha_handle_destroy(region) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Walk every chain: `walks` passes over the graph while the writers
 * change it, or one pass with `expected_edges` the count it must find
 * when `check_total` is set. items are pages read, bytes edges checked,
 * retries the pages read again for a write in progress or a page that
 * had left the chain. */
static int graph_walk(subetha_handle region, uint32_t nodes, uint32_t block_count, uint32_t walks, int check_total,
                      uint64_t expected_edges, subetha_workload_stats *out, const char *who)
{
    int problems = 0;
    static uint8_t page[GRAPH_BLOCK];
    for (uint32_t w = 0; w < walks && problems == 0; w++) {
        uint64_t edges = 0;
        for (uint32_t n = 0; n < nodes && problems == 0; n++) {
            uint64_t t0 = now_ns();
            uint32_t block = n;
            uint32_t restarts = 0;
            uint64_t chain_edges = 0;
            uint32_t pages = 0;
            while (block != SUBETHA_FRAME_NO_BLOCK) {
                int got = graph_page_read(region, block, page, block_count);
                if (got < 0) {
                    fprintf(stderr, "  %s: node %u: page %u would not read\n", who, (unsigned)n, (unsigned)block);
                    problems++;
                    break;
                }
                if (got == 0 || get_u32(page + 12) != n || pages > block_count) {
                    /* Mid-write, or a page pruned since the link to it
                     * was copied: the chain is walked again from its
                     * head. A few restarts spin, since a write is three
                     * short copies; past that the reader yields, because
                     * a writer that lost its core needs it back more than
                     * this walk needs the page. */
                    out->retries++;
                    if (++restarts > GRAPH_RETRY_LIMIT) {
                        fprintf(stderr, "  %s: node %u never settled\n", who, (unsigned)n);
                        problems++;
                        break;
                    }
                    if (restarts > GRAPH_RETRY_SPINS) {
                        sleep_us(GRAPH_RETRY_SLEEP_US);
                    }
                    block = n;
                    chain_edges = 0;
                    pages = 0;
                    continue;
                }
                uint32_t count = get_u32(page + 4);
                for (uint32_t e = 0; e < count; e++) {
                    if (get_u32(page + GRAPH_PAGE_HEADER + 4 * e) >= nodes) {
                        fprintf(stderr, "  %s: node %u names a node past the graph\n", who, (unsigned)n);
                        problems++;
                        break;
                    }
                }
                chain_edges += count;
                pages++;
                block = get_u32(page + 8);
            }
            note_round_trip(out, now_ns() - t0);
            out->items += pages;
            out->bytes += chain_edges;
            edges += chain_edges;
        }
        if (problems == 0 && check_total && edges != expected_edges) {
            fprintf(stderr, "  %s: %llu edges in the graph, not %llu\n", who, (unsigned long long)edges,
                    (unsigned long long)expected_edges);
            problems++;
        }
    }
    return problems;
}

int subetha_workload_graph_reader(const char *path, uint32_t mode, uint32_t nodes, uint32_t block_count, uint32_t walks,
                                  subetha_workload_stats *out)
{
    memset(out, 0, sizeof *out);
    subetha_handle region = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_frame_region_open(path, GRAPH_BLOCK, block_count, mode, &region);
    if (rc != SUBETHA_OK) {
        wl_detail("graph reader", "open the region", rc);
        return 1;
    }
    uint64_t start = now_ns();
    int problems = graph_walk(region, nodes, block_count, walks, 0, 0, out, "graph reader");
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(region) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* One quiet walk once the writers have left: every edge they report
 * adding and not pruning is in the graph. */
int subetha_workload_graph_verify(const char *path, uint32_t mode, uint32_t nodes, uint32_t block_count,
                                  uint64_t expected_edges, subetha_workload_stats *out)
{
    memset(out, 0, sizeof *out);
    subetha_handle region = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_frame_region_open(path, GRAPH_BLOCK, block_count, mode, &region);
    if (rc != SUBETHA_OK) {
        wl_detail("graph verify", "open the region", rc);
        return 1;
    }
    uint64_t start = now_ns();
    int problems = graph_walk(region, nodes, block_count, 1, 1, expected_edges, out, "graph verify");
    out->elapsed_ns = now_ns() - start;
    if (subetha_handle_destroy(region) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* -------------------------------------------------------------- memory --
 * A memory store: a set of record ids in a strategy-switching set and the
 * records themselves in a hash map, both changed under the write hold of
 * one lock and read under its read hold. Writers add records; the first
 * writer moves the set from the vector to the map once it has grown.
 * Readers look ids up under a read hold and expect the two structures to
 * agree, and the set's stamp to stand still while they hold the lock. */
#define MEMORY_ID_BYTES 8
#define MEMORY_ROW_BYTES 40
#define MEMORY_LOCK_MS 30000

static void memory_paths(char *ids, char *rows, char *lock, size_t cap, const char *base)
{
    snprintf(ids, cap, "%s_ids", base);
    snprintf(rows, cap, "%s_rows", base);
    snprintf(lock, cap, "%s_lock", base);
}

static void memory_row(uint8_t *row, uint64_t id, uint32_t writer, uint32_t sequence)
{
    memset(row, 0, MEMORY_ROW_BYTES);
    put_u64(row, id);
    put_u32(row + 8, writer);
    put_u32(row + 12, sequence);
    put_u64(row + 16, fnv1a64(row, 16));
}

static int memory_row_ok(const uint8_t *row, uint64_t id)
{
    return get_u64(row) == id && fnv1a64(row, 16) == get_u64(row + 16);
}

/* Lay the set, the map and the lock out for `capacity` records. */
int subetha_workload_memory_create(const char *base, uint32_t mode, uint64_t capacity)
{
    char ids_path[512], rows_path[512], lock_path[512];
    memory_paths(ids_path, rows_path, lock_path, sizeof ids_path, base);
    subetha_handle ids = SUBETHA_HANDLE_NONE, rows = SUBETHA_HANDLE_NONE, lock = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_universal_create(ids_path, capacity, MEMORY_ID_BYTES, SUBETHA_UNIVERSAL_VEC, mode, &ids);
    if (rc == SUBETHA_OK) {
        rc = subetha_hashmap_create(rows_path, (uint32_t)(capacity * 2 + 64), MEMORY_ID_BYTES, MEMORY_ROW_BYTES, mode, &rows);
    }
    if (rc == SUBETHA_OK) {
        rc = subetha_rwlock_create(lock_path, mode, &lock);
    }
    int problems = 0;
    if (rc != SUBETHA_OK) {
        wl_detail("memory create", "lay the store out", rc);
        problems++;
    }
    if (lock != SUBETHA_HANDLE_NONE && subetha_handle_destroy(lock) != SUBETHA_OK) {
        problems++;
    }
    if (rows != SUBETHA_HANDLE_NONE && subetha_handle_destroy(rows) != SUBETHA_OK) {
        problems++;
    }
    if (ids != SUBETHA_HANDLE_NONE && subetha_handle_destroy(ids) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

static int32_t memory_open(const char *base, uint32_t mode, uint64_t capacity, subetha_handle *ids, subetha_handle *rows,
                           subetha_handle *lock)
{
    char ids_path[512], rows_path[512], lock_path[512];
    memory_paths(ids_path, rows_path, lock_path, sizeof ids_path, base);
    *ids = SUBETHA_HANDLE_NONE;
    *rows = SUBETHA_HANDLE_NONE;
    *lock = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_universal_open(ids_path, capacity, MEMORY_ID_BYTES, mode, ids);
    if (rc == SUBETHA_OK) {
        rc = subetha_hashmap_open(rows_path, (uint32_t)(capacity * 2 + 64), MEMORY_ID_BYTES, MEMORY_ROW_BYTES, mode, rows);
    }
    if (rc == SUBETHA_OK) {
        rc = subetha_rwlock_open(lock_path, mode, lock);
    }
    if (rc != SUBETHA_OK) {
        if (*rows != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(*rows);
        }
        if (*ids != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(*ids);
        }
    }
    return rc;
}

static int memory_close(subetha_handle ids, subetha_handle rows, subetha_handle lock)
{
    int problems = 0;
    if (subetha_handle_destroy(lock) != SUBETHA_OK) {
        problems++;
    }
    if (subetha_handle_destroy(rows) != SUBETHA_OK) {
        problems++;
    }
    if (subetha_handle_destroy(ids) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Add `records` records, ids `writer * records` upward, each under the
 * write hold; writer 0 migrates the set halfway through. items are
 * records added, retries the write holds that timed out and were tried
 * again. */
int subetha_workload_memory_writer(const char *base, uint32_t mode, uint32_t writer, uint32_t writers, uint32_t records,
                                   subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint64_t capacity = (uint64_t)writers * records;
    subetha_handle ids, rows, lock;
    int32_t rc = memory_open(base, mode, capacity, &ids, &rows, &lock);
    if (rc != SUBETHA_OK) {
        wl_detail("memory writer", "open the store", rc);
        return 1;
    }
    uint64_t start = now_ns();
    for (uint32_t r = 0; r < records && problems == 0; r++) {
        uint64_t id = (uint64_t)writer * records + r;
        uint8_t key[MEMORY_ID_BYTES], row[MEMORY_ROW_BYTES];
        put_u64(key, id);
        memory_row(row, id, writer, r);
        uint64_t t0 = now_ns();
        uint64_t hold = 0;
        for (;;) {
            rc = subetha_rwlock_write(lock, MEMORY_LOCK_MS, &hold);
            if (rc == SUBETHA_OK) {
                break;
            }
            if (rc != SUBETHA_E_TIMEOUT) {
                wl_detail("memory writer", "take the write hold", rc);
                problems++;
                break;
            }
            out->retries++;
        }
        if (problems != 0) {
            break;
        }
        bool added = false;
        rc = subetha_universal_insert(ids, key, sizeof key, &added);
        if (rc != SUBETHA_OK) {
            wl_detail("memory writer", "add an id", rc);
            problems++;
        } else if (!added) {
            fprintf(stderr, "  memory writer %u: id %llu was already in the set\n", (unsigned)writer, (unsigned long long)id);
            problems++;
        }
        uint32_t outcome = 0;
        if (problems == 0) {
            rc = subetha_hashmap_insert(rows, key, sizeof key, row, sizeof row, &outcome);
            if (rc != SUBETHA_OK) {
                wl_detail("memory writer", "add a record", rc);
                problems++;
            } else if (outcome != SUBETHA_MAP_INSERTED) {
                fprintf(stderr, "  memory writer %u: id %llu already had a record\n", (unsigned)writer, (unsigned long long)id);
                problems++;
            }
        }
        if (problems == 0 && writer == 0 && r == records / 2) {
            if ((rc = subetha_universal_migrate(ids, SUBETHA_UNIVERSAL_MAP)) != SUBETHA_OK) {
                wl_detail("memory writer", "migrate the set", rc);
                problems++;
            }
        }
        if ((rc = subetha_rwlock_unlock(lock, hold)) != SUBETHA_OK) {
            wl_detail("memory writer", "release the write hold", rc);
            problems++;
        }
        note_round_trip(out, now_ns() - t0);
        out->items++;
        out->bytes += MEMORY_ID_BYTES + MEMORY_ROW_BYTES;
    }
    out->elapsed_ns = now_ns() - start;
    problems += memory_close(ids, rows, lock);
    return problems;
}

/* `lookups` lookups of ids across every writer's range, each under a
 * read hold. items are lookups, bytes the record bytes of the ids found,
 * refusals the ids not yet stored, retries the read holds that timed out
 * and were tried again. */
int subetha_workload_memory_reader(const char *base, uint32_t mode, uint32_t reader, uint32_t writers, uint32_t records,
                                   uint32_t lookups, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    uint64_t capacity = (uint64_t)writers * records;
    subetha_handle ids, rows, lock;
    int32_t rc = memory_open(base, mode, capacity, &ids, &rows, &lock);
    if (rc != SUBETHA_OK) {
        wl_detail("memory reader", "open the store", rc);
        return 1;
    }
    uint64_t start = now_ns();
    for (uint32_t i = 0; i < lookups && problems == 0; i++) {
        uint64_t x = ((uint64_t)reader << 32 | i) * 0x9E3779B97F4A7C15ull;
        x ^= x >> 31;
        uint64_t id = x % capacity;
        uint8_t key[MEMORY_ID_BYTES], row[MEMORY_ROW_BYTES];
        put_u64(key, id);
        uint64_t t0 = now_ns();
        uint64_t hold = 0;
        for (;;) {
            rc = subetha_rwlock_read(lock, MEMORY_LOCK_MS, &hold);
            if (rc == SUBETHA_OK) {
                break;
            }
            if (rc != SUBETHA_E_TIMEOUT) {
                wl_detail("memory reader", "take the read hold", rc);
                problems++;
                break;
            }
            out->retries++;
        }
        if (problems != 0) {
            break;
        }
        subetha_universal_stats before, after;
        bool present = false;
        if (subetha_universal_read_stats(ids, &before) != SUBETHA_OK ||
            subetha_universal_contains(ids, key, sizeof key, &present) != SUBETHA_OK) {
            fprintf(stderr, "  memory reader %u: the set would not answer for id %llu\n", (unsigned)reader,
                    (unsigned long long)id);
            problems++;
        } else {
            size_t len = 0;
            rc = subetha_hashmap_get(rows, key, sizeof key, row, sizeof row, &len);
            if (present && rc == SUBETHA_OK && memory_row_ok(row, id)) {
                out->bytes += MEMORY_ROW_BYTES;
            } else if (!present && rc == SUBETHA_E_MAP_KEY_ABSENT) {
                out->refusals++;
            } else {
                fprintf(stderr, "  memory reader %u: id %llu is %s the set and its record answered %d (%s)\n", (unsigned)reader,
                        (unsigned long long)id, present ? "in" : "not in", (int)rc, subetha_strerror(rc));
                problems++;
            }
            if (subetha_universal_read_stats(ids, &after) != SUBETHA_OK || after.stamp != before.stamp) {
                fprintf(stderr, "  memory reader %u: the set migrated under a read hold\n", (unsigned)reader);
                problems++;
            }
        }
        if ((rc = subetha_rwlock_unlock(lock, hold)) != SUBETHA_OK) {
            wl_detail("memory reader", "release the read hold", rc);
            problems++;
        }
        note_round_trip(out, now_ns() - t0);
        out->items++;
    }
    out->elapsed_ns = now_ns() - start;
    problems += memory_close(ids, rows, lock);
    return problems;
}

/* Once the writers have left: every record is in both structures and the
 * set finished on the map strategy. */
int subetha_workload_memory_verify(const char *base, uint32_t mode, uint32_t writers, uint32_t records)
{
    int problems = 0;
    uint64_t capacity = (uint64_t)writers * records;
    subetha_handle ids, rows, lock;
    int32_t rc = memory_open(base, mode, capacity, &ids, &rows, &lock);
    if (rc != SUBETHA_OK) {
        wl_detail("memory verify", "open the store", rc);
        return 1;
    }
    subetha_universal_stats ustats;
    subetha_hashmap_stats hstats;
    if (subetha_universal_read_stats(ids, &ustats) != SUBETHA_OK || subetha_hashmap_read_stats(rows, &hstats) != SUBETHA_OK) {
        fprintf(stderr, "  memory verify: the store would not report\n");
        problems++;
    } else if (ustats.len != capacity || hstats.len != capacity || ustats.strategy != SUBETHA_UNIVERSAL_MAP) {
        fprintf(stderr, "  memory verify: %llu ids and %llu records of %llu, strategy %u\n", (unsigned long long)ustats.len,
                (unsigned long long)hstats.len, (unsigned long long)capacity, (unsigned)ustats.strategy);
        problems++;
    }
    problems += memory_close(ids, rows, lock);
    return problems;
}

/* -------------------------------------------------------------- stream --
 * A cluster stream: one receiver process standing up a sealed
 * Sens-O-Matic endpoint, several sender processes each dialing it over
 * more than one sealed stream, so a process installs its crypto provider
 * once and reuses it. Items carry the sender, the stream, a sequence
 * number and a hash of the payload; each stream ends with a sequence of
 * STREAM_END. The receiver checks order and integrity per stream, counts
 * the sequence gaps, and reads both halves' loss counters with their
 * reports. A gap with the kernel's own count reading an exact zero is a
 * delivery fault; a gap under any other report is recorded. */
#define STREAM_SYMBOL 1200
#define STREAM_PAYLOAD_MIN 200
#define STREAM_PAYLOAD_MAX 900
#define STREAM_HEADER 24
#define STREAM_END 0xFFFFFFFFu
#define STREAM_PACE_US 200
#define STREAM_QUIET_MS 10000
#define STREAM_FINISH_MS 10000
/* The receiver is the only thing that acknowledges, and it acknowledges
 * only while it polls, so it stays past the last stream for as long as a
 * sender is allowed to wait on one. A shorter stay ends the only source
 * of acknowledgement while a sender is still owed one, which reaches
 * that sender as a peer that stopped responding rather than as the
 * drain it actually is. */
#define STREAM_LINGER_MS STREAM_FINISH_MS
#define STREAM_POLL_US 200
/* In managed mode a sidecar thread drives the decoder on its own
 * millisecond cadence and the polling thread only carries away what
 * that sidecar has already delivered, so a poll faster than the
 * sidecar finds nothing and spends the core the sidecar needs. In
 * strict mode the polling thread is the one driving the decoder, so
 * there is nothing for it to compete with and it polls at the rate
 * the stream is paced at. */
#define STREAM_POLL_MANAGED_US 2000

static void stream_header(uint8_t *item, uint32_t sender, uint32_t stream, uint32_t sequence, size_t payload_len,
                          const uint8_t *payload)
{
    put_u32(item, sender);
    put_u32(item + 4, stream);
    put_u32(item + 8, sequence);
    put_u32(item + 12, (uint32_t)payload_len);
    put_u64(item + 16, fnv1a64(payload, payload_len));
}

/* Stand the receiver up on a port of the system's choosing, announce it,
 * and take `senders * streams` streams of `items` items each. items are
 * items delivered, refusals the sequence gaps, retries the polls that
 * found nothing, total_ns the streams that reached their end. */
int subetha_workload_stream_receiver(const char *cert_hex, const char *key_hex, uint32_t mode, uint32_t senders,
                                     uint32_t streams, uint32_t items, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    static uint8_t cert[4096], key[4096];
    size_t cert_len = 0, key_len = 0;
    if (!hex_decode(cert_hex, cert, sizeof cert, &cert_len) || !hex_decode(key_hex, key, sizeof key, &key_len)) {
        fprintf(stderr, "  stream receiver: the certificate or key is not hex\n");
        fprintf(stdout, "STREAM-RECEIVER-FAILED %d\n", (int)SUBETHA_E_INVALID_ARGUMENT);
        fflush(stdout);
        return 1;
    }
    subetha_handle rx = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_sens_receiver_tls("127.0.0.1:0", STREAM_SYMBOL, mode, cert, cert_len, key, key_len,
                                           SUBETHA_SENS_CODE_RLC, (size_t)senders * streams, &rx);
    uint16_t port = 0;
    if (rc == SUBETHA_OK) {
        rc = subetha_sens_local_port(rx, &port);
    }
    if (rc != SUBETHA_OK) {
        wl_detail("stream receiver", "stand up", rc);
        fprintf(stdout, "STREAM-RECEIVER-FAILED %d\n", (int)rc);
        fflush(stdout);
        if (rx != SUBETHA_HANDLE_NONE) {
            subetha_handle_destroy(rx);
        }
        return 1;
    }
    fprintf(stdout, "STREAM-RECEIVER-READY %u\n", (unsigned)port);
    fflush(stdout);
    uint32_t total_streams = senders * streams;
    uint32_t *expected = calloc(total_streams, sizeof *expected);
    uint8_t *ended = calloc(total_streams, sizeof *ended);
    if (expected == NULL || ended == NULL) {
        fprintf(stderr, "  stream receiver: no memory for %u streams\n", (unsigned)total_streams);
        free(expected);
        free(ended);
        subetha_handle_destroy(rx);
        return 1;
    }
    static uint8_t item[STREAM_HEADER + STREAM_PAYLOAD_MAX];
    uint32_t poll_us = mode == SUBETHA_MODE_MANAGED ? STREAM_POLL_MANAGED_US : STREAM_POLL_US;
    uint64_t start = now_ns();
    uint64_t last_delivery = start;
    uint32_t streams_ended = 0;
    while (streams_ended < total_streams && problems == 0) {
        size_t len = 0;
        rc = subetha_sens_poll(rx, item, sizeof item, &len);
        if (rc == SUBETHA_E_RING_EMPTY) {
            out->retries++;
            if (now_ns() - last_delivery > (uint64_t)STREAM_QUIET_MS * 1000000ull) {
                break;
            }
            sleep_us(poll_us);
            continue;
        }
        if (rc != SUBETHA_OK) {
            wl_detail("stream receiver", "poll", rc);
            problems++;
            break;
        }
        last_delivery = now_ns();
        if (len < STREAM_HEADER) {
            fprintf(stderr, "  stream receiver: a %zu-byte item\n", len);
            problems++;
            break;
        }
        uint32_t sender = get_u32(item), stream = get_u32(item + 4), sequence = get_u32(item + 8);
        uint32_t payload_len = get_u32(item + 12);
        if (sender >= senders || stream >= streams || payload_len != len - STREAM_HEADER ||
            fnv1a64(item + STREAM_HEADER, payload_len) != get_u64(item + 16)) {
            fprintf(stderr, "  stream receiver: an item from sender %u stream %u does not hold together\n", (unsigned)sender,
                    (unsigned)stream);
            problems++;
            break;
        }
        uint32_t slot = sender * streams + stream;
        if (ended[slot]) {
            fprintf(stderr, "  stream receiver: sender %u stream %u delivered past its end\n", (unsigned)sender,
                    (unsigned)stream);
            problems++;
            break;
        }
        if (sequence == STREAM_END) {
            out->refusals += items - expected[slot];
            ended[slot] = 1;
            streams_ended++;
            continue;
        }
        if (sequence < expected[slot]) {
            fprintf(stderr, "  stream receiver: sender %u stream %u went back to %u after %u\n", (unsigned)sender,
                    (unsigned)stream, (unsigned)sequence, (unsigned)expected[slot]);
            problems++;
            break;
        }
        out->refusals += sequence - expected[slot];
        expected[slot] = sequence + 1;
        out->items++;
        out->bytes += len;
    }
    for (uint32_t s = 0; s < total_streams; s++) {
        if (!ended[s]) {
            out->refusals += items - expected[s];
        }
    }
    out->total_ns = streams_ended;
    out->elapsed_ns = now_ns() - start;
    /* The senders finish by waiting for their last items to be acked,
     * and an ack is feedback this half sends as it polls, so it keeps
     * polling a while after the last stream ended rather than closing
     * the socket their acks would come from. */
    uint64_t linger_from = now_ns();
    while (problems == 0 && now_ns() - linger_from < (uint64_t)STREAM_LINGER_MS * 1000000ull) {
        size_t len = 0;
        rc = subetha_sens_poll(rx, item, sizeof item, &len);
        if (rc == SUBETHA_E_RING_EMPTY) {
            sleep_us(poll_us);
            continue;
        }
        if (rc != SUBETHA_OK) {
            wl_detail("stream receiver", "poll after the end", rc);
            problems++;
            break;
        }
        fprintf(stderr, "  stream receiver: a %zu-byte item after every stream ended\n", len);
        problems++;
    }
    subetha_sens_stats stats;
    if ((rc = subetha_sens_read_stats(rx, &stats)) != SUBETHA_OK) {
        wl_detail("stream receiver", "read the stats", rc);
        problems++;
    } else {
        fprintf(stdout,
                "STREAM-RECEIVER-DROPS kernel_dropped=%llu kernel_dropped_report=%u missed=%llu missed_report=%u "
                "unroutable=%llu preauth_dropped=%llu unopened=%llu handshake_failures=%llu\n",
                (unsigned long long)stats.kernel_dropped, (unsigned)stats.kernel_dropped_report,
                (unsigned long long)stats.missed, (unsigned)stats.missed_report, (unsigned long long)stats.unroutable,
                (unsigned long long)stats.preauth_dropped, (unsigned long long)stats.unopened,
                (unsigned long long)stats.handshake_failures);
        fflush(stdout);
        if (out->refusals != 0 && stats.kernel_dropped_report == SUBETHA_DROPS_EXACT && stats.kernel_dropped == 0) {
            fprintf(stderr, "  stream receiver: %llu items missing with the kernel reporting no drop\n",
                    (unsigned long long)out->refusals);
            problems++;
        }
    }
    free(expected);
    free(ended);
    if (subetha_handle_destroy(rx) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Dial the receiver on `port` over `streams` sealed streams and send
 * `items` items on each, round-robin, STREAM_PACE_US apart, ending every
 * stream and finishing it, which waits for the far end's acknowledgment
 * of all of it. items are items sent, bytes their bytes, worst_ns the
 * longest send or finish, total_ns the sender's own missed count summed
 * over its streams. */
int subetha_workload_stream_sender(const char *cert_hex, uint32_t port, uint32_t mode, uint32_t sender, uint32_t streams,
                                   uint32_t items, subetha_workload_stats *out)
{
    int problems = 0;
    memset(out, 0, sizeof *out);
    static uint8_t cert[4096];
    size_t cert_len = 0;
    if (!hex_decode(cert_hex, cert, sizeof cert, &cert_len)) {
        fprintf(stderr, "  stream sender: the certificate is not hex\n");
        return 1;
    }
    char peer[64];
    snprintf(peer, sizeof peer, "127.0.0.1:%u", (unsigned)port);
    subetha_handle *tx = calloc(streams, sizeof *tx);
    if (tx == NULL) {
        fprintf(stderr, "  stream sender: no memory for %u streams\n", (unsigned)streams);
        return 1;
    }
    uint32_t opened = 0;
    int32_t rc = SUBETHA_OK;
    for (uint32_t s = 0; s < streams; s++) {
        rc = subetha_sens_sender_tls("127.0.0.1:0", peer, STREAM_SYMBOL, mode, cert, cert_len, NULL, &tx[s]);
        if (rc != SUBETHA_OK) {
            wl_detail("stream sender", "dial", rc);
            problems++;
            break;
        }
        opened++;
    }
    static uint8_t item[STREAM_HEADER + STREAM_PAYLOAD_MAX];
    static uint8_t payload[STREAM_PAYLOAD_MAX];
    uint64_t start = now_ns();
    for (uint32_t i = 0; i < items && problems == 0; i++) {
        for (uint32_t s = 0; s < streams && problems == 0; s++) {
            size_t payload_len = seeded_bytes(((uint64_t)sender << 40) | ((uint64_t)s << 32) | i, payload, STREAM_PAYLOAD_MIN,
                                              STREAM_PAYLOAD_MAX);
            stream_header(item, sender, s, i, payload_len, payload);
            memcpy(item + STREAM_HEADER, payload, payload_len);
            uint64_t t0 = now_ns();
            if ((rc = subetha_sens_send(tx[s], item, STREAM_HEADER + payload_len)) != SUBETHA_OK) {
                wl_detail("stream sender", "send an item", rc);
                problems++;
                break;
            }
            /* total_ns carries the sender's own missed count, so a send's
             * time goes to worst_ns alone. */
            uint64_t took = now_ns() - t0;
            if (took > out->worst_ns) {
                out->worst_ns = took;
            }
            out->items++;
            out->bytes += STREAM_HEADER + payload_len;
            spin_us(STREAM_PACE_US);
        }
    }
    for (uint32_t s = 0; s < opened && problems == 0; s++) {
        stream_header(item, sender, s, STREAM_END, 0, payload);
        if ((rc = subetha_sens_send(tx[s], item, STREAM_HEADER)) != SUBETHA_OK) {
            wl_detail("stream sender", "end a stream", rc);
            problems++;
            break;
        }
        if ((rc = subetha_sens_flush(tx[s])) != SUBETHA_OK) {
            wl_detail("stream sender", "flush a stream", rc);
            problems++;
            break;
        }
    }
    /* A stream ends when the far end has acknowledged all of it; a sender
     * that closed on the last send would leave its tail to a repair that
     * nothing follows. The receiver is alive and polling, so a stream it
     * never acknowledges is a stream it could not deliver. */
    for (uint32_t s = 0; s < opened && problems == 0; s++) {
        bool acked = false;
        uint64_t t0 = now_ns();
        if ((rc = subetha_sens_finish(tx[s], STREAM_FINISH_MS, &acked)) != SUBETHA_OK) {
            wl_detail("stream sender", "finish a stream", rc);
            problems++;
            break;
        }
        uint64_t took = now_ns() - t0;
        if (took > out->worst_ns) {
            out->worst_ns = took;
        }
        if (!acked) {
            fprintf(stderr, "  stream sender %u: stream %u was not acknowledged within %d ms\n", (unsigned)sender,
                    (unsigned)s, STREAM_FINISH_MS);
            problems++;
        }
    }
    out->elapsed_ns = now_ns() - start;
    for (uint32_t s = 0; s < opened; s++) {
        subetha_sens_stats stats;
        if (subetha_sens_read_stats(tx[s], &stats) == SUBETHA_OK && stats.missed_report == SUBETHA_DROPS_EXACT) {
            out->total_ns += stats.missed;
        }
        if (subetha_handle_destroy(tx[s]) != SUBETHA_OK) {
            problems++;
        }
    }
    free(tx);
    return problems;
}
