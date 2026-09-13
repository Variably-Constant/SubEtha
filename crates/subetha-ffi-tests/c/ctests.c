/*
 * The C test suite for the SubEtha C ABI. Every check that fails prints
 * one line naming the file, the line, and what was expected, and the
 * count of failures is what the Rust harness asserts on. Nothing here
 * aborts: a failing check is recorded and the suite goes on, so one run
 * reports every problem it finds.
 */

#include "subetha.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <poll.h>
#include <time.h>
#endif

static int failures = 0;

/* The pause a peer takes between polls of a value the other process is
 * about to write. Windows rounds up to the scheduler's tick, which is why
 * the callers ask for a couple of hundred microseconds rather than a few. */
static void sleep_us(uint32_t us)
{
#if defined(_WIN32)
    Sleep(us / 1000 + (us % 1000 != 0));
#else
    struct timespec ts;
    ts.tv_sec = us / 1000000;
    ts.tv_nsec = (long)(us % 1000000) * 1000;
    nanosleep(&ts, NULL);
#endif
}

static void report_detail(void)
{
    char detail[512];
    size_t needed = subetha_last_error_detail(detail, sizeof detail);
    if (needed > 1) {
        fprintf(stderr, "        detail: %s\n", detail);
    }
}

#define CHECK(cond)                                                                        \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            failures++;                                                                    \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);             \
        }                                                                                  \
    } while (0)

#define EXPECT_CODE(expr, want)                                                            \
    do {                                                                                   \
        int32_t got_ = (expr);                                                             \
        int32_t want_ = (want);                                                            \
        if (got_ != want_) {                                                               \
            failures++;                                                                    \
            fprintf(stderr, "  FAIL %s:%d: %s\n        returned %d (%s), wanted %d (%s)\n", \
                    __FILE__, __LINE__, #expr, (int)got_, subetha_strerror(got_),         \
                    (int)want_, subetha_strerror(want_));                                  \
            report_detail();                                                               \
        }                                                                                  \
    } while (0)

/* Named fields, not positions: the struct gains fields between releases,
 * and a positional list silently slides every value one place along when
 * it does. */
static const subetha_ring_options strict_options = {.mode = SUBETHA_MODE_STRICT, .stamps = SUBETHA_STAMPS_NONE};

static void test_version_and_init(void)
{
    uint32_t packed = subetha_abi_version();
    CHECK((packed >> 16) == SUBETHA_ABI_VERSION_MAJOR);
    CHECK(((packed >> 8) & 0xff) == SUBETHA_ABI_VERSION_MINOR);
    CHECK((packed & 0xff) == SUBETHA_ABI_VERSION_PATCH);
    /* Built from the header's own constants rather than written out, so
     * the string and the packed number cannot drift apart and a tier that
     * moves the version does not have to remember this line. */
    char want_version[32];
    snprintf(want_version, sizeof want_version, "%u.%u.%u", (unsigned)SUBETHA_ABI_VERSION_MAJOR,
             (unsigned)SUBETHA_ABI_VERSION_MINOR, (unsigned)SUBETHA_ABI_VERSION_PATCH);
    CHECK(strcmp(subetha_abi_version_string(), want_version) == 0);
    CHECK(strlen(subetha_crate_version_string()) > 0);

    /* Nothing runs before init, and the refusal says why. */
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &h), SUBETHA_E_NOT_INITIALIZED);
    EXPECT_CODE(subetha_init(7), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    EXPECT_CODE(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    EXPECT_CODE(subetha_init(SUBETHA_MODE_MANAGED), SUBETHA_E_INIT_CONFLICT);
    uint32_t mode = 99;
    EXPECT_CODE(subetha_default_mode(&mode), SUBETHA_OK);
    CHECK(mode == SUBETHA_MODE_STRICT);
    EXPECT_CODE(subetha_default_mode(NULL), SUBETHA_E_INVALID_ARGUMENT);
}

static void test_codes_and_handles(void)
{
    CHECK(strcmp(subetha_strerror(SUBETHA_OK), "ok") == 0);
    CHECK(strcmp(subetha_strerror(12345), "unknown subetha code") == 0);

    uint32_t kind = 0;
    bool poisoned = true;
    EXPECT_CODE(subetha_handle_kind(SUBETHA_HANDLE_NONE, &kind), SUBETHA_E_INVALID_HANDLE);
    EXPECT_CODE(subetha_handle_is_poisoned(SUBETHA_HANDLE_NONE, &poisoned), SUBETHA_E_INVALID_HANDLE);
    EXPECT_CODE(subetha_handle_destroy(SUBETHA_HANDLE_NONE), SUBETHA_E_INVALID_HANDLE);
    EXPECT_CODE(subetha_handle_destroy(0xFFFFFFFFFFFFFFFFull), SUBETHA_E_INVALID_HANDLE);

    /* The detail names the handle that was refused. */
    char detail[256];
    size_t needed = subetha_last_error_detail(detail, sizeof detail);
    CHECK(needed > 1 && needed <= sizeof detail);
    CHECK(strstr(detail, "handle") != NULL);
    /* A short buffer is cut and says how much it needed. */
    char tiny[4];
    size_t again = subetha_last_error_detail(tiny, sizeof tiny);
    CHECK(again == needed);
    CHECK(tiny[3] == '\0');
    CHECK(subetha_last_error_detail(NULL, 0) == needed);

    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &h), SUBETHA_OK);
    CHECK(h != SUBETHA_HANDLE_NONE);
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_RING);
    EXPECT_CODE(subetha_handle_is_poisoned(h, &poisoned), SUBETHA_OK);
    CHECK(!poisoned);
    CHECK(subetha_live_handles() == 1);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_E_INVALID_HANDLE);
    EXPECT_CODE(subetha_ring_read_stats(h, NULL), SUBETHA_E_INVALID_HANDLE);
    CHECK(subetha_live_handles() == 0);

    /* Bad construction arguments are refused before anything is built. */
    EXPECT_CODE(subetha_ring_create_anon(0, 1, 64, &strict_options, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 100, &strict_options, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, NULL, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, NULL), SUBETHA_E_INVALID_ARGUMENT);
    subetha_ring_options bad_mode = {.mode = 42, .stamps = SUBETHA_STAMPS_NONE};
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &bad_mode, &h), SUBETHA_E_INVALID_ARGUMENT);
    /* Managed mode with no cadence named takes the default rather than
     * refusing. The cadence is a latency budget with no knee in it - a
     * request/response round trip costs exactly one interval and a
     * streaming caller costs nothing - so there is no value a caller must
     * discover before it can create a ring, and every value stays
     * available through the option. */
    subetha_ring_options managed_without_interval = {.mode = SUBETHA_MODE_MANAGED, .stamps = SUBETHA_STAMPS_NONE};
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &managed_without_interval, &h), SUBETHA_OK);
    CHECK(subetha_live_handles() == 1);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    /* Read through a variable: comparing the constant directly is a
     * constant conditional, which one of the three toolchains treats as
     * an error. */
    uint64_t default_us = SUBETHA_SCAN_INTERVAL_DEFAULT_US;
    CHECK(default_us == 250);
    CHECK(subetha_live_handles() == 0);
}

static void test_anon_ring(void)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &h), SUBETHA_OK);
    uint32_t pid = 99, cid = 99;
    EXPECT_CODE(subetha_ring_register_producer(h, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(h, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_producer(h, NULL), SUBETHA_E_INVALID_ARGUMENT);

    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_ring_try_pop(h, cid, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_ring_try_push(h, pid, (const uint8_t *)"hello", 5), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_pop(h, cid, out, sizeof out, &len), SUBETHA_OK);
    /* A pop yields the whole slot, zero past the payload. */
    CHECK(len == SUBETHA_RING_SLOT_BYTES);
    CHECK(memcmp(out, "hello", 5) == 0);
    CHECK(out[5] == 0 && out[SUBETHA_RING_SLOT_BYTES - 1] == 0);

    /* Argument checks on the data path. */
    EXPECT_CODE(subetha_ring_try_push(h, pid, NULL, 3), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_try_push(h, pid, out, SUBETHA_RING_SLOT_BYTES + 1),
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    EXPECT_CODE(subetha_ring_try_pop(h, cid, out, 8, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_ring_try_pop(h, cid, NULL, sizeof out, &len), SUBETHA_E_INVALID_ARGUMENT);

    /* A payload every shape accepts round-trips exactly. */
    uint8_t big[SUBETHA_RING_PAYLOAD_MAX];
    for (size_t i = 0; i < sizeof big; i++) {
        big[i] = (uint8_t)(i * 7);
    }
    EXPECT_CODE(subetha_ring_try_push(h, pid, big, sizeof big), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_pop(h, cid, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == SUBETHA_RING_SLOT_BYTES);
    CHECK(memcmp(out, big, sizeof big) == 0);

    /* Fill it: the capacity is honored and the refusal is named. */
    int pushed = 0;
    for (int i = 0; i < 1000; i++) {
        if (subetha_ring_try_push(h, pid, (const uint8_t *)"x", 1) != SUBETHA_OK) {
            break;
        }
        pushed++;
    }
    CHECK(pushed >= 63 && pushed <= 64);
    EXPECT_CODE(subetha_ring_try_push(h, pid, (const uint8_t *)"x", 1), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_ring_push_wait(h, pid, (const uint8_t *)"x", 1, 20), SUBETHA_E_TIMEOUT);
    int popped = 0;
    while (subetha_ring_try_pop(h, cid, out, sizeof out, &len) == SUBETHA_OK) {
        popped++;
    }
    CHECK(popped == pushed);

    /* A wait on an empty ring times out and says so; a negative timeout
     * other than the forever sentinel is refused. */
    EXPECT_CODE(subetha_ring_pop_wait(h, cid, out, sizeof out, &len, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_ring_pop_wait(h, cid, out, sizeof out, &len, -7), SUBETHA_E_INVALID_ARGUMENT);

    subetha_ring_stats stats;
    memset(&stats, 0xff, sizeof stats);
    EXPECT_CODE(subetha_ring_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.shape == SUBETHA_RING_SHAPE_SPSC);
    CHECK(stats.mode == SUBETHA_MODE_STRICT);
    CHECK(stats.active_producers == 1);
    CHECK(stats.active_consumers == 1);
    CHECK(stats.max_producers == 1);
    CHECK(stats.max_consumers == 1);
    CHECK(stats.capacity == 64);
    CHECK(stats.approx_len == 0);
    CHECK(stats.waker_full == 0);
    CHECK(stats.sidecar_morphs == 0);

    EXPECT_CODE(subetha_ring_unregister_consumer(h, cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_unregister_producer(h, pid), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
}

static void test_managed_ring_morphs(void)
{
    subetha_ring_options managed = {.mode = SUBETHA_MODE_MANAGED, .scan_interval_us = 1000, .stamps = SUBETHA_STAMPS_NONE};
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(4, 4, 64, &managed, &h), SUBETHA_OK);
    uint32_t p1, p2, c1;
    EXPECT_CODE(subetha_ring_register_producer(h, &p1), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_producer(h, &p2), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(h, &c1), SUBETHA_OK);
    subetha_ring_stats stats;
    EXPECT_CODE(subetha_ring_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.mode == SUBETHA_MODE_MANAGED);
    CHECK(stats.active_producers == 2);
    /* Two producers and one consumer is the multi-producer shape, which
     * registration itself selects. */
    CHECK(stats.shape == SUBETHA_RING_SHAPE_MPSC);
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_ring_try_push(h, p1, (const uint8_t *)"one", 3), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, p2, (const uint8_t *)"two", 3), SUBETHA_OK);
    int got = 0;
    while (subetha_ring_try_pop(h, c1, out, sizeof out, &len) == SUBETHA_OK) {
        CHECK(len == SUBETHA_RING_SLOT_BYTES);
        CHECK(memcmp(out, "one", 3) == 0 || memcmp(out, "two", 3) == 0);
        got++;
    }
    CHECK(got == 2);
    /* Destroying a managed ring joins its sidecar thread; the call returns
     * only when it has. */
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
}

static void test_file_ring_two_handles(const char *scratch_prefix)
{
    char prefix[1024];
    snprintf(prefix, sizeof prefix, "%s-file", scratch_prefix);

    subetha_handle creator = SUBETHA_HANDLE_NONE;
    subetha_handle attacher = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create(prefix, 2, 2, 64, &strict_options, &creator), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_open(prefix, 2, 2, 64, &strict_options, &attacher), SUBETHA_OK);
    /* A different capacity is a layout mismatch, not a fresh ring. */
    subetha_handle wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_open(prefix, 2, 2, 128, &strict_options, &wrong),
                SUBETHA_E_RING_LAYOUT_MISMATCH);
    CHECK(wrong == SUBETHA_HANDLE_NONE);

    uint32_t pid = 0, cid = 0;
    EXPECT_CODE(subetha_ring_register_producer(creator, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(attacher, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(creator, pid, (const uint8_t *)"across", 6), SUBETHA_OK);
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_ring_pop_wait(attacher, cid, out, sizeof out, &len, 2000), SUBETHA_OK);
    CHECK(len == SUBETHA_RING_SLOT_BYTES);
    CHECK(memcmp(out, "across", 6) == 0 && out[6] == 0);

    /* Unlink while handles are open is refused on Windows, where a mapped
     * file cannot be removed, and counted; it is not silent anywhere. */
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_ring_unlink(prefix, 2, &report), SUBETHA_OK);
    CHECK(report.failed == 0);
    /* spsc, vyukov, peers, 2 mpsc, 2 mpmc, 2 wakers, the notifier record =
     * 10 removed; the frame and ordering regions were never created, so 2
     * missing. */
    CHECK(report.removed == 10);
    /* The frame and ordering regions were never created, and neither was
     * the holder table, since this ring asked for no hold. */
    CHECK(report.missing == 3);
    EXPECT_CODE(subetha_ring_unlink(prefix, 2, &report), SUBETHA_OK);
    CHECK(report.removed == 0);
    CHECK(report.missing == 12);
    EXPECT_CODE(subetha_ring_unlink(NULL, 2, &report), SUBETHA_E_INVALID_ARGUMENT);
}

/* A ring can be asked to remove its backings when the last handle
 * holding them goes, which is the only way a C caller gets that: the
 * backings otherwise outlive every handle, because a ring is usually
 * created so something else can attach later. */
static void test_ring_last_holder(const char *scratch_prefix)
{
    char prefix[1024];
    snprintf(prefix, sizeof prefix, "%s-lasthold", scratch_prefix);

    /* Either field alone is an incomplete request. Unlinking with no
     * holder table has nowhere to record who is holding, and the library
     * names no default for the count. */
    subetha_ring_options no_count = strict_options;
    no_count.last_holder = SUBETHA_LAST_HOLDER_UNLINK;
    subetha_handle refused = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create(prefix, 1, 1, 64, &no_count, &refused),
                SUBETHA_E_INVALID_ARGUMENT);
    CHECK(refused == SUBETHA_HANDLE_NONE);

    /* A hold needs files to remove, so a ring with none refuses it by
     * name rather than accepting the call and doing nothing. */
    subetha_ring_options held = strict_options;
    held.max_holders = 4;
    held.last_holder = SUBETHA_LAST_HOLDER_UNLINK;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &held, &refused),
                SUBETHA_E_INVALID_ARGUMENT);
    CHECK(refused == SUBETHA_HANDLE_NONE);

    subetha_handle first = SUBETHA_HANDLE_NONE;
    subetha_handle second = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create(prefix, 1, 1, 64, &held, &first), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_open(prefix, 1, 1, 64, &held, &second), SUBETHA_OK);

    /* The ring works while both hold it. */
    uint32_t pid = 0, cid = 0;
    EXPECT_CODE(subetha_ring_register_producer(first, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(second, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(first, pid, (const uint8_t *)"held", 4), SUBETHA_OK);
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_ring_pop_wait(second, cid, out, sizeof out, &len, 2000), SUBETHA_OK);
    CHECK(memcmp(out, "held", 4) == 0);

    /* One holder of two leaving must leave the ring alone. A third
     * handle opening proves the backings are still there, which asking
     * the file system would not: this is the ring's own view of them. */
    EXPECT_CODE(subetha_handle_destroy(first), SUBETHA_OK);
    subetha_handle third = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_open(prefix, 1, 1, 64, &held, &third), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(third), SUBETHA_OK);

    /* The last one out takes them with it, so a following unlink finds
     * nothing left to remove. */
    EXPECT_CODE(subetha_handle_destroy(second), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_ring_unlink(prefix, 1, &report), SUBETHA_OK);
    /* Nothing left to remove: the wakers and the notifier record go with
     * the ring's own backings, because a caller who asked for the prefix
     * to be cleaned means the whole prefix. */
    CHECK(report.removed == 0);
    CHECK(report.failed == 0);
    CHECK(report.missing > 0);
}

static void test_shm_ring(void)
{
    subetha_handle creator = SUBETHA_HANDLE_NONE;
    subetha_handle attacher = SUBETHA_HANDLE_NONE;
    char name[64];
    snprintf(name, sizeof name, "subetha_ctest_%ld", (long)subetha_live_handles() + 1000);
    EXPECT_CODE(subetha_ring_create_shm(name, 1, 1, 64, SUBETHA_SHM_SESSION, &strict_options, &creator),
                SUBETHA_OK);
    EXPECT_CODE(subetha_ring_open_shm(name, 1, 1, 64, SUBETHA_SHM_SESSION, &strict_options, &attacher),
                SUBETHA_OK);
    EXPECT_CODE(subetha_ring_open_shm(name, 1, 1, 64, 7, &strict_options, &attacher),
                SUBETHA_E_INVALID_ARGUMENT);
    uint32_t pid = 0, cid = 0;
    EXPECT_CODE(subetha_ring_register_producer(creator, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(attacher, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(creator, pid, (const uint8_t *)"shm", 3), SUBETHA_OK);
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_ring_pop_wait(attacher, cid, out, sizeof out, &len, 2000), SUBETHA_OK);
    CHECK(len == SUBETHA_RING_SLOT_BYTES && memcmp(out, "shm", 3) == 0 && out[3] == 0);
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_ring_unlink_shm(name, SUBETHA_SHM_SESSION, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 0 && report.failed == 0);
}

static void test_frames_on_the_ring(void)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &h), SUBETHA_OK);
    uint32_t pid = 0, cid = 0;
    EXPECT_CODE(subetha_ring_register_producer(h, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(h, &cid), SUBETHA_OK);

    static uint8_t big[1000];
    for (size_t i = 0; i < sizeof big; i++) {
        big[i] = (uint8_t)(i % 251);
    }
    uint32_t frame_class = 99;
    EXPECT_CODE(subetha_ring_send_frame(h, pid, big, sizeof big, SUBETHA_LAYOUT_AUTO, &frame_class), SUBETHA_OK);
    CHECK(frame_class == SUBETHA_FRAME_OFFSET);
    EXPECT_CODE(subetha_ring_send_frame(h, pid, (const uint8_t *)"small", 5, SUBETHA_LAYOUT_AUTO, &frame_class),
                SUBETHA_OK);
    CHECK(frame_class == SUBETHA_FRAME_INLINE);
    EXPECT_CODE(subetha_ring_send_frame(h, pid, big, sizeof big, SUBETHA_LAYOUT_FORCE_INLINE, NULL),
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    EXPECT_CODE(subetha_ring_send_frame(h, pid, big, 5, 9, NULL), SUBETHA_E_INVALID_ARGUMENT);
    static uint8_t huge[SUBETHA_RING_FRAME_DEFAULT_BLOCK + 1];
    EXPECT_CODE(subetha_ring_send_frame(h, pid, huge, sizeof huge, SUBETHA_LAYOUT_AUTO, NULL),
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE);

    /* A short buffer holds the frame rather than losing it. */
    uint8_t small_buf[16];
    size_t len = 0;
    EXPECT_CODE(subetha_ring_recv_frame(h, cid, small_buf, sizeof small_buf, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    CHECK(len == sizeof big);
    static uint8_t room[4096];
    EXPECT_CODE(subetha_ring_recv_frame(h, cid, room, sizeof room, &len), SUBETHA_OK);
    CHECK(len == sizeof big && memcmp(room, big, sizeof big) == 0);
    EXPECT_CODE(subetha_ring_recv_frame_wait(h, cid, room, sizeof room, &len, 1000), SUBETHA_OK);
    CHECK(len == 5 && memcmp(room, "small", 5) == 0);
    EXPECT_CODE(subetha_ring_recv_frame(h, cid, room, sizeof room, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_ring_recv_frame_wait(h, cid, room, sizeof room, &len, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_ring_send_frame_wait(h, pid, (const uint8_t *)"w", 1, SUBETHA_LAYOUT_AUTO, 1000, &frame_class),
                SUBETHA_OK);
    EXPECT_CODE(subetha_ring_recv_frame(h, cid, room, sizeof room, &len), SUBETHA_OK);
    CHECK(len == 1 && room[0] == 'w');
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A ring built with its own frame region geometry carries a frame
     * past the default block; one geometry field without the other is
     * refused. */
    subetha_ring_options framed = strict_options;
    framed.frame_block = 65536;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &framed, &h), SUBETHA_E_INVALID_ARGUMENT);
    framed.frame_blocks = 4;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &framed, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_producer(h, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(h, &cid), SUBETHA_OK);
    static uint8_t wide[40000];
    for (size_t i = 0; i < sizeof wide; i++) {
        wide[i] = (uint8_t)(i % 253);
    }
    EXPECT_CODE(subetha_ring_send_frame(h, pid, wide, sizeof wide, SUBETHA_LAYOUT_AUTO, &frame_class), SUBETHA_OK);
    CHECK(frame_class == SUBETHA_FRAME_OFFSET);
    static uint8_t wide_out[65536];
    EXPECT_CODE(subetha_ring_recv_frame(h, cid, wide_out, sizeof wide_out, &len), SUBETHA_OK);
    CHECK(len == sizeof wide && memcmp(wide_out, wide, sizeof wide) == 0);
    EXPECT_CODE(subetha_ring_send_frame(h, pid, wide_out, sizeof wide_out + 1, SUBETHA_LAYOUT_AUTO, NULL),
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
}

static void test_spsc_ring(void)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_spsc_create_anon(3, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_spsc_create_anon(4, 9, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_spsc_create_anon(4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_SPSC);

    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_spsc_try_pop(h, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    for (uint8_t i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_spsc_try_push(h, &i, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_spsc_try_push(h, out, 1), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_spsc_push_wait(h, out, 1, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_spsc_try_push(h, out, SUBETHA_RING_SLOT_BYTES + 1), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    EXPECT_CODE(subetha_spsc_pop_wait(h, out, sizeof out, &len, 1000), SUBETHA_OK);
    CHECK(len == SUBETHA_RING_SLOT_BYTES && out[0] == 0);
    EXPECT_CODE(subetha_spsc_try_pop(h, out, 8, &len), SUBETHA_E_BUFFER_TOO_SMALL);

    subetha_spsc_stats stats;
    EXPECT_CODE(subetha_spsc_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.head == 4 && stats.tail == 1 && stats.approx_len == 3);
    CHECK(stats.mode == SUBETHA_MODE_STRICT && !stats.phase_locking);
    EXPECT_CODE(subetha_spsc_set_phase_locking(h, true), SUBETHA_OK);
    EXPECT_CODE(subetha_spsc_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.phase_locking);
    /* An adaptive-ring call on this handle is refused by kind. */
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_spsc_wake_all(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
}

static void test_spsc_file_two_handles(const char *scratch_prefix)
{
    char base[1024];
    snprintf(base, sizeof base, "%s-spsc", scratch_prefix);
    subetha_handle creator = SUBETHA_HANDLE_NONE;
    subetha_handle attacher = SUBETHA_HANDLE_NONE;
    subetha_handle wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_spsc_create(base, 64, SUBETHA_MODE_STRICT, &creator), SUBETHA_OK);
    EXPECT_CODE(subetha_spsc_open(base, 64, SUBETHA_MODE_STRICT, &attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_spsc_open(base, 128, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    CHECK(wrong == SUBETHA_HANDLE_NONE);
    EXPECT_CODE(subetha_spsc_try_push(creator, (const uint8_t *)"across", 6), SUBETHA_OK);
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_spsc_pop_wait(attacher, out, sizeof out, &len, 2000), SUBETHA_OK);
    CHECK(len == SUBETHA_RING_SLOT_BYTES && memcmp(out, "across", 6) == 0 && out[6] == 0);
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    /* The ring, its two wakers and the notifier record. */
    EXPECT_CODE(subetha_spsc_unlink(base, &report), SUBETHA_OK);
    CHECK(report.removed == 4 && report.missing == 0 && report.failed == 0);
    EXPECT_CODE(subetha_spsc_unlink(base, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 3);
}

static void test_mpsc_pool(const char *scratch_prefix)
{
    subetha_handle producers[3] = {0, 0, 0};
    subetha_handle consumer = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_mpsc_create_anon_pool(0, 4, SUBETHA_MODE_STRICT, producers, &consumer),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_mpsc_create_anon_pool(3, 4, SUBETHA_MODE_STRICT, NULL, &consumer),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_mpsc_create_anon_pool(3, 4, SUBETHA_MODE_STRICT, producers, &consumer), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(producers[2], &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_MPSC_PRODUCER);
    EXPECT_CODE(subetha_handle_kind(consumer, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_MPSC_CONSUMER);
    CHECK(producers[0] != producers[1] && producers[1] != producers[2]);

    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_mpsc_try_pop(consumer, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_mpsc_try_pop(producers[0], out, sizeof out, &len), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_mpsc_try_push(consumer, out, 1), SUBETHA_E_WRONG_KIND);
    for (uint8_t i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_mpsc_try_push(producers[i], &i, 1), SUBETHA_OK);
    }
    unsigned seen = 0;
    for (int i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_mpsc_pop_wait(consumer, out, sizeof out, &len, 1000), SUBETHA_OK);
        CHECK(len == SUBETHA_RING_SLOT_BYTES && out[0] < 3);
        seen |= 1u << out[0];
    }
    CHECK(seen == 7u);
    subetha_mpsc_consumer_stats cs;
    EXPECT_CODE(subetha_mpsc_consumer_read_stats(consumer, &cs), SUBETHA_OK);
    CHECK(cs.n_producers == 3 && cs.approx_total_len == 0);
    subetha_mpsc_producer_stats ps;
    EXPECT_CODE(subetha_mpsc_producer_read_stats(producers[1], &ps), SUBETHA_OK);
    CHECK(ps.capacity == 4 && ps.head == 1);
    /* One producer's ring fills on its own; the push parks until the timeout. */
    for (int i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_mpsc_try_push(producers[0], out, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_mpsc_try_push(producers[0], out, 1), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_mpsc_push_wait(producers[0], out, 1, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_mpsc_consumer_read_stats(consumer, &cs), SUBETHA_OK);
    CHECK(cs.approx_total_len == 4);
    for (int i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_handle_destroy(producers[i]), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_handle_destroy(consumer), SUBETHA_OK);

    /* The file locale, and its unlink. */
    char prefix[1024];
    snprintf(prefix, sizeof prefix, "%s-mpsc", scratch_prefix);
    subetha_handle fp[2] = {0, 0};
    subetha_handle fc = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_mpsc_create_pool(prefix, 2, 8, SUBETHA_MODE_STRICT, fp, &fc), SUBETHA_OK);
    EXPECT_CODE(subetha_mpsc_try_push(fp[1], (const uint8_t *)"f", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_mpsc_try_pop(fc, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 'f');
    EXPECT_CODE(subetha_handle_destroy(fp[0]), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(fp[1]), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(fc), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_mpsc_unlink(prefix, 2, &report), SUBETHA_OK);
    CHECK(report.removed == 5 && report.missing == 0 && report.failed == 0);
}

static void test_mpmc_grid(const char *scratch_prefix)
{
    subetha_handle producers[4] = {0, 0, 0, 0};
    subetha_handle consumers[2] = {0, 0};
    EXPECT_CODE(subetha_mpmc_create_anon_grid(1, 2, 4, SUBETHA_MODE_STRICT, producers, consumers),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_mpmc_create_anon_grid(4, 0, 4, SUBETHA_MODE_STRICT, producers, consumers),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_mpmc_create_anon_grid(4, 2, 4, SUBETHA_MODE_STRICT, producers, consumers), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(producers[3], &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_MPMC_PRODUCER);
    EXPECT_CODE(subetha_handle_kind(consumers[1], &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_MPMC_CONSUMER);
    subetha_mpmc_consumer_stats cs;
    EXPECT_CODE(subetha_mpmc_consumer_read_stats(consumers[0], &cs), SUBETHA_OK);
    CHECK(cs.n_rings == 2 && cs.approx_subset_len == 0);

    for (uint8_t i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_mpmc_try_push(producers[i], &i, 1), SUBETHA_OK);
    }
    /* Ring i belongs to consumer i % 2. */
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    unsigned seen0 = 0, seen1 = 0;
    for (int i = 0; i < 2; i++) {
        EXPECT_CODE(subetha_mpmc_pop_wait(consumers[0], out, sizeof out, &len, 1000), SUBETHA_OK);
        seen0 |= 1u << out[0];
        EXPECT_CODE(subetha_mpmc_pop_wait(consumers[1], out, sizeof out, &len, 1000), SUBETHA_OK);
        seen1 |= 1u << out[0];
    }
    CHECK(seen0 == 5u && seen1 == 10u);
    EXPECT_CODE(subetha_mpmc_try_pop(consumers[0], out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_mpmc_pop_wait(consumers[1], out, sizeof out, &len, 20), SUBETHA_E_TIMEOUT);
    subetha_mpmc_producer_stats ps;
    EXPECT_CODE(subetha_mpmc_producer_read_stats(producers[2], &ps), SUBETHA_OK);
    CHECK(ps.capacity == 4 && ps.head == 1);
    for (int i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_handle_destroy(producers[i]), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_handle_destroy(consumers[0]), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(consumers[1]), SUBETHA_OK);

    char prefix[1024];
    snprintf(prefix, sizeof prefix, "%s-mpmc", scratch_prefix);
    subetha_handle fp[2] = {0, 0};
    subetha_handle fc[1] = {0};
    EXPECT_CODE(subetha_mpmc_create_grid(prefix, 2, 1, 8, SUBETHA_MODE_STRICT, fp, fc), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(fp[0]), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(fp[1]), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(fc[0]), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_mpmc_unlink(prefix, 2, 1, &report), SUBETHA_OK);
    CHECK(report.removed == 5 && report.missing == 0 && report.failed == 0);
}

static void test_vyukov_ring(const char *scratch_prefix)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_vyukov_create_anon(64, &strict_options, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_VYUKOV);
    uint8_t out[SUBETHA_RING_PAYLOAD_MAX];
    size_t len = 0;
    EXPECT_CODE(subetha_vyukov_try_pop(h, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_vyukov_pop_wait(h, out, sizeof out, &len, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_vyukov_try_push(h, out, SUBETHA_RING_PAYLOAD_MAX + 1), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    for (uint8_t i = 0; i < 64; i++) {
        EXPECT_CODE(subetha_vyukov_try_push(h, &i, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_vyukov_try_push(h, out, 1), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_vyukov_push_wait(h, out, 1, 20), SUBETHA_E_TIMEOUT);
    for (uint8_t i = 0; i < 64; i++) {
        EXPECT_CODE(subetha_vyukov_pop_wait(h, out, sizeof out, &len, 1000), SUBETHA_OK);
        CHECK(len == SUBETHA_RING_PAYLOAD_MAX && out[0] == i);
    }
    subetha_vyukov_stats stats;
    EXPECT_CODE(subetha_vyukov_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 64 && stats.producer_seq == 64 && stats.consumer_seq == 64 && stats.approx_len == 0);
    bool found = true;
    uint64_t pos = 9;
    EXPECT_CODE(subetha_vyukov_next_stuck_slot(h, 0, &found, &pos), SUBETHA_OK);
    CHECK(!found);
    bool healed = true;
    EXPECT_CODE(subetha_vyukov_heal_stuck_slot(h, 3, &healed), SUBETHA_OK);
    CHECK(!healed);
    EXPECT_CODE(subetha_vyukov_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    char path[1024];
    snprintf(path, sizeof path, "%s-vyukov.bin", scratch_prefix);
    subetha_handle creator = SUBETHA_HANDLE_NONE;
    subetha_handle attacher = SUBETHA_HANDLE_NONE;
    subetha_handle wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_vyukov_create(path, 16, &strict_options, &creator), SUBETHA_OK);
    EXPECT_CODE(subetha_vyukov_open(path, 16, &strict_options, &attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_vyukov_open(path, 32, &strict_options, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    CHECK(wrong == SUBETHA_HANDLE_NONE);
    EXPECT_CODE(subetha_vyukov_try_push(creator, (const uint8_t *)"v", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_vyukov_pop_wait(attacher, out, sizeof out, &len, 1000), SUBETHA_OK);
    CHECK(out[0] == 'v' && out[1] == 0);
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_vyukov_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 3 && report.missing == 0 && report.failed == 0);

    char name[64];
    snprintf(name, sizeof name, "subetha_vyukov_%ld", (long)subetha_live_handles() + 2000);
    EXPECT_CODE(subetha_vyukov_create_shm(name, 16, SUBETHA_SHM_SESSION, &strict_options, &creator), SUBETHA_OK);
    EXPECT_CODE(subetha_vyukov_open_shm(name, 16, SUBETHA_SHM_SESSION, &strict_options, &attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_vyukov_try_push(attacher, (const uint8_t *)"s", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_vyukov_try_pop(creator, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 's');
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    EXPECT_CODE(subetha_vyukov_unlink_shm(name, SUBETHA_SHM_SESSION, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.failed == 0);
}

static void test_lamport_pair(const char *scratch_prefix)
{
    subetha_handle producer = SUBETHA_HANDLE_NONE;
    subetha_handle consumer = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_lamport_create_anon_pair(8, SUBETHA_MODE_STRICT, &producer, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_lamport_create_anon_pair(8, SUBETHA_MODE_STRICT, &producer, &consumer), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(producer, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_LAMPORT_PRODUCER);
    EXPECT_CODE(subetha_handle_kind(consumer, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_LAMPORT_CONSUMER);
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_lamport_try_pop(consumer, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_lamport_try_pop(producer, out, sizeof out, &len), SUBETHA_E_WRONG_KIND);
    for (uint8_t i = 0; i < 8; i++) {
        EXPECT_CODE(subetha_lamport_try_push(producer, &i, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_lamport_try_push(producer, out, 1), SUBETHA_E_RING_FULL);
    for (uint8_t i = 0; i < 8; i++) {
        EXPECT_CODE(subetha_lamport_try_pop(consumer, out, sizeof out, &len), SUBETHA_OK);
        CHECK(len == SUBETHA_RING_SLOT_BYTES && out[0] == i);
    }
    subetha_lamport_producer_stats ps;
    subetha_lamport_consumer_stats cs;
    EXPECT_CODE(subetha_lamport_producer_read_stats(producer, &ps), SUBETHA_OK);
    EXPECT_CODE(subetha_lamport_consumer_read_stats(consumer, &cs), SUBETHA_OK);
    CHECK(ps.capacity == 8 && ps.head == 8 && cs.capacity == 8 && cs.tail == 8);
    EXPECT_CODE(subetha_handle_destroy(producer), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(consumer), SUBETHA_OK);

    char path[1024];
    snprintf(path, sizeof path, "%s-lamport.bin", scratch_prefix);
    subetha_handle p2 = SUBETHA_HANDLE_NONE, c2 = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_lamport_create_pair(path, 8, SUBETHA_MODE_STRICT, &producer, &consumer), SUBETHA_OK);
    EXPECT_CODE(subetha_lamport_open_pair(path, 8, SUBETHA_MODE_STRICT, &p2, &c2), SUBETHA_OK);
    EXPECT_CODE(subetha_lamport_try_push(producer, (const uint8_t *)"l", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_lamport_try_pop(c2, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 'l');
    EXPECT_CODE(subetha_handle_destroy(producer), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(consumer), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(p2), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(c2), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_lamport_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.missing == 0 && report.failed == 0);
}

static void test_broadcast_ring(const char *scratch_prefix)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_broadcast_create_anon(1, &strict_options, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_broadcast_create_anon(4, &strict_options, &h), SUBETHA_OK);
    uint32_t a = 99, b = 99;
    EXPECT_CODE(subetha_broadcast_register_consumer(h, &a), SUBETHA_OK);
    EXPECT_CODE(subetha_broadcast_register_consumer(h, &b), SUBETHA_OK);
    CHECK(a != b && a < SUBETHA_BROADCAST_MAX_CONSUMERS && b < SUBETHA_BROADCAST_MAX_CONSUMERS);
    uint8_t out[SUBETHA_BROADCAST_PAYLOAD_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_broadcast_try_recv(h, a, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_broadcast_recv_wait(h, a, out, sizeof out, &len, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_broadcast_try_recv(h, 15, out, sizeof out, &len), SUBETHA_E_BROADCAST_INVALID_CONSUMER);
    EXPECT_CODE(subetha_broadcast_try_push(h, out, SUBETHA_BROADCAST_PAYLOAD_BYTES + 1), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    for (uint8_t i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_broadcast_try_push(h, &i, 1), SUBETHA_OK);
    }
    /* The slowest consumer gates the producer: nobody has read slot 0. */
    EXPECT_CODE(subetha_broadcast_try_push(h, out, 1), SUBETHA_E_RING_FULL);
    for (uint8_t i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_broadcast_try_recv(h, a, out, sizeof out, &len), SUBETHA_OK);
        CHECK(len == SUBETHA_BROADCAST_PAYLOAD_BYTES && out[0] == i && out[1] == 0);
    }
    EXPECT_CODE(subetha_broadcast_push_wait(h, out, 1, 20), SUBETHA_E_TIMEOUT);
    uint64_t lag = 0;
    EXPECT_CODE(subetha_broadcast_lag(h, b, &lag), SUBETHA_OK);
    CHECK(lag == 4);
    EXPECT_CODE(subetha_broadcast_lag(h, 16, &lag), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_broadcast_recv_wait(h, b, out, sizeof out, &len, 1000), SUBETHA_OK);
    CHECK(out[0] == 0);
    EXPECT_CODE(subetha_broadcast_try_push(h, (const uint8_t *)"e", 1), SUBETHA_OK);
    subetha_broadcast_stats stats;
    EXPECT_CODE(subetha_broadcast_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.producer_position == 5 && stats.active_consumers == 2 && !stats.fully_drained);
    EXPECT_CODE(subetha_broadcast_unregister_consumer(h, b), SUBETHA_OK);
    EXPECT_CODE(subetha_broadcast_recv_wait(h, a, out, sizeof out, &len, 1000), SUBETHA_OK);
    CHECK(out[0] == 'e');
    EXPECT_CODE(subetha_broadcast_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.active_consumers == 1 && stats.fully_drained);
    /* Every consumer slot can be taken, and the next request is refused. */
    uint32_t extra[SUBETHA_BROADCAST_MAX_CONSUMERS];
    unsigned taken = 0;
    for (unsigned i = 0; i < SUBETHA_BROADCAST_MAX_CONSUMERS; i++) {
        if (subetha_broadcast_register_consumer(h, &extra[i]) == SUBETHA_OK) {
            taken++;
        } else {
            break;
        }
    }
    CHECK(taken == SUBETHA_BROADCAST_MAX_CONSUMERS - 1);
    uint32_t none = 0;
    EXPECT_CODE(subetha_broadcast_register_consumer(h, &none), SUBETHA_E_BROADCAST_NO_CONSUMER_SLOT);
    EXPECT_CODE(subetha_broadcast_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_broadcast_wake_all(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    char path[1024];
    snprintf(path, sizeof path, "%s-broadcast.bin", scratch_prefix);
    subetha_handle creator = SUBETHA_HANDLE_NONE;
    subetha_handle attacher = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_broadcast_create(path, 8, &strict_options, &creator), SUBETHA_OK);
    EXPECT_CODE(subetha_broadcast_open(path, 8, &strict_options, &attacher), SUBETHA_OK);
    uint32_t c = 99;
    EXPECT_CODE(subetha_broadcast_register_consumer(attacher, &c), SUBETHA_OK);
    EXPECT_CODE(subetha_broadcast_try_push(creator, (const uint8_t *)"b", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_broadcast_recv_wait(attacher, c, out, sizeof out, &len, 1000), SUBETHA_OK);
    CHECK(out[0] == 'b');
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_broadcast_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 3 && report.missing == 0 && report.failed == 0);
}

static void test_pubsub_ring(const char *scratch_prefix)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_pubsub_create_anon(4, &strict_options, &h), SUBETHA_OK);
    subetha_pubsub_stats stats;
    EXPECT_CODE(subetha_pubsub_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.head == 0);
    uint8_t out[SUBETHA_PUBSUB_PAYLOAD_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_pubsub_read_at(h, 0, out, sizeof out), SUBETHA_E_PUBSUB_PENDING);
    EXPECT_CODE(subetha_pubsub_read_at(h, 0, out, 8), SUBETHA_E_BUFFER_TOO_SMALL);
    uint64_t position = 99;
    for (uint8_t i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_pubsub_publish(h, &i, 1, &position), SUBETHA_OK);
        CHECK(position == i);
    }
    EXPECT_CODE(subetha_pubsub_read_at(h, 1, out, sizeof out), SUBETHA_OK);
    CHECK(out[0] == 1 && out[1] == 0);
    EXPECT_CODE(subetha_pubsub_publish(h, out, SUBETHA_PUBSUB_PAYLOAD_BYTES + 1, NULL), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);

    subetha_handle sub = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_pubsub_subscribe(h, 0, SUBETHA_MODE_STRICT, &sub), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(sub, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_SUBSCRIBER);
    for (uint8_t i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_subscriber_try_next(sub, out, sizeof out, &len), SUBETHA_OK);
        CHECK(len == SUBETHA_PUBSUB_PAYLOAD_BYTES && out[0] == i);
    }
    EXPECT_CODE(subetha_subscriber_try_next(sub, out, sizeof out, &len), SUBETHA_E_PUBSUB_PENDING);
    EXPECT_CODE(subetha_subscriber_next_wait(sub, out, sizeof out, &len, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_subscriber_position(sub, &position), SUBETHA_OK);
    CHECK(position == 3);
    /* Five more publishes overwrite position 3 before it is read. */
    for (uint8_t i = 0; i < 5; i++) {
        EXPECT_CODE(subetha_pubsub_publish(h, &i, 1, NULL), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_subscriber_try_next(sub, out, sizeof out, &len), SUBETHA_E_PUBSUB_LOST);
    EXPECT_CODE(subetha_subscriber_position(sub, &position), SUBETHA_OK);
    CHECK(position == 8);
    EXPECT_CODE(subetha_subscriber_set_position(sub, 6), SUBETHA_OK);
    EXPECT_CODE(subetha_subscriber_try_next(sub, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 3);
    EXPECT_CODE(subetha_subscriber_skip(sub, 1, &position), SUBETHA_OK);
    CHECK(position == 8);
    uint32_t mode = 99;
    EXPECT_CODE(subetha_subscriber_mode(sub, &mode), SUBETHA_OK);
    CHECK(mode == SUBETHA_MODE_STRICT);
    EXPECT_CODE(subetha_pubsub_publish(sub, out, 1, NULL), SUBETHA_E_WRONG_KIND);

    /* A file position survives the subscriber handle. */
    char pos_path[1024];
    snprintf(pos_path, sizeof pos_path, "%s-pubsub.pos", scratch_prefix);
    subetha_handle filed = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_pubsub_subscribe_file(h, pos_path, 5, true, SUBETHA_MODE_STRICT, &filed), SUBETHA_OK);
    EXPECT_CODE(subetha_subscriber_try_next(filed, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 2);
    EXPECT_CODE(subetha_handle_destroy(filed), SUBETHA_OK);
    EXPECT_CODE(subetha_pubsub_subscribe_file(h, pos_path, 0, false, SUBETHA_MODE_STRICT, &filed), SUBETHA_OK);
    EXPECT_CODE(subetha_subscriber_position(filed, &position), SUBETHA_OK);
    CHECK(position == 6);
    EXPECT_CODE(subetha_handle_destroy(filed), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_pubsub_unlink_position(pos_path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_handle_destroy(sub), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    char path[1024];
    snprintf(path, sizeof path, "%s-pubsub.bin", scratch_prefix);
    subetha_handle creator = SUBETHA_HANDLE_NONE;
    subetha_handle attacher = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_pubsub_create(path, 8, &strict_options, &creator), SUBETHA_OK);
    EXPECT_CODE(subetha_pubsub_open(path, 8, &strict_options, &attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_pubsub_subscribe(attacher, 0, SUBETHA_MODE_STRICT, &sub), SUBETHA_OK);
    EXPECT_CODE(subetha_pubsub_publish(creator, (const uint8_t *)"p", 1, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_subscriber_next_wait(sub, out, sizeof out, &len, 1000), SUBETHA_OK);
    CHECK(out[0] == 'p');
    EXPECT_CODE(subetha_handle_destroy(sub), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    EXPECT_CODE(subetha_pubsub_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 2 && report.missing == 0 && report.failed == 0);
}

static void test_stamped_ring_and_contract(void)
{
    subetha_ring_options stamped = strict_options;
    stamped.stamps = SUBETHA_STAMPS_DEFAULT;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(2, 1, 64, &stamped, &h), SUBETHA_OK);
    subetha_ring_stats stats;
    EXPECT_CODE(subetha_ring_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.stamps != SUBETHA_STAMPS_NONE && stats.stamps != SUBETHA_STAMPS_DEFAULT);
    CHECK(stats.ordering_mode == SUBETHA_ORDERING_UNORDERED);
    uint32_t p0 = 0, p1 = 0, cid = 0;
    EXPECT_CODE(subetha_ring_register_producer(h, &p0), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_producer(h, &p1), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(h, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_set_ordering_mode(h, SUBETHA_ORDERING_MERGE_BY_STAMP), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_set_ordering_mode(h, 9), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.ordering_mode == SUBETHA_ORDERING_MERGE_BY_STAMP);
    EXPECT_CODE(subetha_ring_try_push(h, p0, (const uint8_t *)"a", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, p1, (const uint8_t *)"b", 1), SUBETHA_OK);
    /* Frames and stamps both claim the slot head. */
    EXPECT_CODE(subetha_ring_send_frame(h, p0, (const uint8_t *)"f", 1, SUBETHA_LAYOUT_AUTO, NULL),
                SUBETHA_E_RING_LAYOUT_MISMATCH);
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    uint64_t first = 0, second = 0;
    EXPECT_CODE(subetha_ring_pop_wait_stamped(h, cid, out, sizeof out, &len, &first, 1000), SUBETHA_OK);
    CHECK(out[0] == 'a');
    EXPECT_CODE(subetha_ring_try_pop_stamped(h, cid, out, sizeof out, &len, &second), SUBETHA_OK);
    CHECK(out[0] == 'b');
    CHECK(second >= first);
    EXPECT_CODE(subetha_ring_try_pop_stamped(h, cid, out, sizeof out, &len, &second), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_ring_try_pop_stamped(h, cid, out, sizeof out, &len, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_refresh_watermark(h, p0), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.inversions == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* An unstamped ring refuses the stamped calls by name. */
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_set_ordering_mode(h, SUBETHA_ORDERING_MERGE_STRICT), SUBETHA_E_RING_NOT_STAMPED);
    EXPECT_CODE(subetha_ring_register_consumer(h, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_pop_stamped(h, cid, out, sizeof out, &len, &first), SUBETHA_E_RING_NOT_STAMPED);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_ring_options bad_stamps = strict_options;
    bad_stamps.stamps = 9;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &bad_stamps, &h), SUBETHA_E_INVALID_ARGUMENT);

    /* A declared contract pins the producer count. */
    subetha_ring_options pinned = strict_options;
    pinned.contract.max_producers = 1;
    pinned.contract.max_consumers = 1;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &pinned, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_producer(h, &p0), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_producer(h, &p1), SUBETHA_E_RING_TOO_MANY_PRODUCERS);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_ring_options bad_contract = strict_options;
    bad_contract.contract.ordering = 9;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &bad_contract, &h), SUBETHA_E_INVALID_ARGUMENT);
}

static void test_capacity_ring(const char *scratch_prefix)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_capacity_create_anon(1, 1, 4, &strict_options, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_CAPACITY_RING);
    uint32_t pid = 0, cid = 0;
    EXPECT_CODE(subetha_capacity_register_producer(h, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_register_consumer(h, &cid), SUBETHA_OK);
    for (uint8_t i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_capacity_try_push(h, pid, &i, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_capacity_try_push(h, pid, (const uint8_t *)"x", 1), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_capacity_push_wait(h, pid, (const uint8_t *)"x", 1, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_capacity_morph_capacity(h, 3), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_capacity_morph_capacity(h, 8), SUBETHA_OK);
    subetha_capacity_stats stats;
    EXPECT_CODE(subetha_capacity_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.current_capacity == 8 && stats.pin_generation == 1 && !stats.stamped);
    for (uint8_t i = 4; i < 8; i++) {
        EXPECT_CODE(subetha_capacity_try_push(h, pid, &i, 1), SUBETHA_OK);
    }
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    for (uint8_t i = 0; i < 8; i++) {
        EXPECT_CODE(subetha_capacity_pop_wait(h, cid, out, sizeof out, &len, 1000), SUBETHA_OK);
        CHECK(len == SUBETHA_RING_SLOT_BYTES && out[0] == i);
    }
    EXPECT_CODE(subetha_capacity_try_pop(h, cid, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_capacity_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.stale_pops == 4 && stats.approx_len == 0);
    EXPECT_CODE(subetha_capacity_prewarm(h, 32), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.warm_capacity == 32);
    EXPECT_CODE(subetha_capacity_morph(h, SUBETHA_KEEP, 32, SUBETHA_TARGET_KEEP, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.current_capacity == 32 && stats.warm_hits == 1 && stats.warm_capacity == 0);
    EXPECT_CODE(subetha_capacity_morph(h, 9, 0, SUBETHA_TARGET_KEEP, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_capacity_clear_warm(h), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_set_ordering_mode(h, SUBETHA_ORDERING_MERGE_BY_STAMP), SUBETHA_E_RING_NOT_STAMPED);
    EXPECT_CODE(subetha_capacity_wake_all(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    subetha_ring_options refused = strict_options;
    refused.stamps = SUBETHA_STAMPS_TSC;
    EXPECT_CODE(subetha_capacity_create_anon(1, 1, 4, &refused, &h), SUBETHA_E_INVALID_ARGUMENT);
    refused = strict_options;
    refused.mode = SUBETHA_MODE_MANAGED;
    EXPECT_CODE(subetha_capacity_create_anon(1, 1, 4, &refused, &h), SUBETHA_E_INVALID_ARGUMENT);
    refused.scan_interval_us = 1000;
    EXPECT_CODE(subetha_capacity_create_anon(1, 1, 64, &refused, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.mode == SUBETHA_MODE_MANAGED);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    char base[1024];
    snprintf(base, sizeof base, "%s-capacity", scratch_prefix);
    subetha_handle creator = SUBETHA_HANDLE_NONE;
    subetha_handle attacher = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_capacity_create(base, 1, 1, 8, &strict_options, &creator), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_open(base, 1, 1, 8, &strict_options, &attacher), SUBETHA_OK);
    /* Another capacity names another backing, which does not exist. */
    subetha_handle wrong_capacity = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_capacity_open(base, 1, 1, 16, &strict_options, &wrong_capacity), SUBETHA_E_RING_IO);
    CHECK(wrong_capacity == SUBETHA_HANDLE_NONE);
    EXPECT_CODE(subetha_capacity_register_producer(creator, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_register_consumer(attacher, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_try_push(creator, pid, (const uint8_t *)"c", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_pop_wait(attacher, cid, out, sizeof out, &len, 1000), SUBETHA_OK);
    CHECK(out[0] == 'c');
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_capacity_unlink(base, &report), SUBETHA_OK);
    CHECK(report.failed == 0 && report.removed >= 7);
}

static void test_locale_ring(const char *scratch_prefix)
{
    char base[1024];
    snprintf(base, sizeof base, "%s-locale", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_locale_ring_create(base, 1, 1, 8, &strict_options, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_LOCALE_RING);
    uint32_t pid = 0, cid = 0;
    EXPECT_CODE(subetha_locale_ring_register_producer(h, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_locale_ring_register_consumer(h, &cid), SUBETHA_OK);
    subetha_locale_stats stats;
    EXPECT_CODE(subetha_locale_ring_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.current_locale == SUBETHA_LOCALE_ANON && stats.locale_generation == 0);
    for (uint8_t i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_locale_ring_try_push(h, pid, &i, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_locale_ring_migrate(h, SUBETHA_LOCALE_FILE), SUBETHA_OK);
    EXPECT_CODE(subetha_locale_ring_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.current_locale == SUBETHA_LOCALE_FILE && stats.locale_generation == 1 && stats.approx_len == 3);

    /* A second handle attaches to the creator's backings and sees the
     * locale the creator chose; an absent base is an I/O error. */
    subetha_handle second = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_locale_ring_open(base, 1, 1, 8, &strict_options, &second), SUBETHA_OK);
    subetha_locale_stats second_stats;
    EXPECT_CODE(subetha_locale_ring_read_stats(second, &second_stats), SUBETHA_OK);
    CHECK(second_stats.current_locale == SUBETHA_LOCALE_FILE && second_stats.approx_len == 3);
    EXPECT_CODE(subetha_handle_destroy(second), SUBETHA_OK);
    char absent[1100];
    snprintf(absent, sizeof absent, "%s-absent", base);
    EXPECT_CODE(subetha_locale_ring_open(absent, 1, 1, 8, &strict_options, &second), SUBETHA_E_RING_IO);

    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    for (uint8_t i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_locale_ring_pop_wait(h, cid, out, sizeof out, &len, 1000), SUBETHA_OK);
        CHECK(len == SUBETHA_RING_SLOT_BYTES && out[0] == i);
    }
    EXPECT_CODE(subetha_locale_ring_try_pop(h, cid, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_locale_ring_migrate(h, SUBETHA_LOCALE_SHM), SUBETHA_OK);
    EXPECT_CODE(subetha_locale_ring_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.current_locale == SUBETHA_LOCALE_SHM && stats.locale_generation == 2);
    EXPECT_CODE(subetha_locale_ring_try_push(h, pid, (const uint8_t *)"s", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_locale_ring_try_pop(h, cid, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 's');
    EXPECT_CODE(subetha_locale_ring_migrate(h, 7), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_locale_ring_request(h, SUBETHA_LOCALE_ANON), SUBETHA_E_NOT_SUPPORTED);
    EXPECT_CODE(subetha_locale_ring_set_ordering_mode(h, SUBETHA_ORDERING_MERGE_BY_STAMP), SUBETHA_E_RING_NOT_STAMPED);
    EXPECT_CODE(subetha_locale_ring_wake_all(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_locale_ring_unlink(base, 1, &report), SUBETHA_OK);
    CHECK(report.failed == 0 && report.removed >= 2);

    /* Managed mode takes a request and migrates on its own scan. */
    snprintf(base, sizeof base, "%s-locale-managed", scratch_prefix);
    subetha_ring_options managed = strict_options;
    managed.mode = SUBETHA_MODE_MANAGED;
    managed.scan_interval_us = 1000;
    EXPECT_CODE(subetha_locale_ring_create(base, 1, 1, 8, &managed, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_locale_ring_request(h, SUBETHA_LOCALE_FILE), SUBETHA_OK);
    for (int i = 0; i < 2000; i++) {
        EXPECT_CODE(subetha_locale_ring_read_stats(h, &stats), SUBETHA_OK);
        if (stats.current_locale == SUBETHA_LOCALE_FILE) {
            break;
        }
        subetha_locale_ring_wake_all(h);
    }
    /* The default policy holds a migration for 250 ms after the last one;
       the scan runs every millisecond, so the request lands within the
       polling above or the loop reports the miss. */
    for (int i = 0; i < 1000 && stats.current_locale != SUBETHA_LOCALE_FILE; i++) {
        uint8_t probe[SUBETHA_RING_SLOT_BYTES];
        size_t n = 0;
        subetha_locale_ring_pop_wait(h, 0, probe, sizeof probe, &n, 1);
        EXPECT_CODE(subetha_locale_ring_read_stats(h, &stats), SUBETHA_OK);
    }
    CHECK(stats.current_locale == SUBETHA_LOCALE_FILE);
    CHECK(stats.sidecar_migrations == 1);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_locale_ring_unlink(base, 1, &report), SUBETHA_OK);
    CHECK(report.failed == 0);
}

static void test_capacity_broadcast(const char *scratch_prefix)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_capacity_broadcast_create_anon(4, &strict_options, &h), SUBETHA_OK);
    uint32_t consumer = 99;
    EXPECT_CODE(subetha_capacity_broadcast_register_consumer(h, &consumer), SUBETHA_OK);
    for (uint8_t i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_capacity_broadcast_try_push(h, &i, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_capacity_broadcast_try_push(h, (const uint8_t *)"x", 1), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_capacity_broadcast_morph_capacity(h, 8), SUBETHA_OK);
    for (uint8_t i = 4; i < 8; i++) {
        EXPECT_CODE(subetha_capacity_broadcast_try_push(h, &i, 1), SUBETHA_OK);
    }
    uint8_t out[SUBETHA_BROADCAST_PAYLOAD_BYTES];
    size_t len = 0;
    for (uint8_t i = 0; i < 8; i++) {
        EXPECT_CODE(subetha_capacity_broadcast_recv_wait(h, consumer, out, sizeof out, &len, 1000), SUBETHA_OK);
        CHECK(len == SUBETHA_BROADCAST_PAYLOAD_BYTES && out[0] == i);
    }
    EXPECT_CODE(subetha_capacity_broadcast_try_recv(h, consumer, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    subetha_capacity_broadcast_stats stats;
    EXPECT_CODE(subetha_capacity_broadcast_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.current_capacity == 8 && stats.pin_generation == 1 && stats.active_consumers == 1);
    EXPECT_CODE(subetha_capacity_broadcast_prewarm(h, 16), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_broadcast_clear_warm(h), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_broadcast_wake_all(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    char base[1024];
    snprintf(base, sizeof base, "%s-capbroadcast", scratch_prefix);
    EXPECT_CODE(subetha_capacity_broadcast_create(base, 8, &strict_options, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_capacity_broadcast_unlink(base, &report), SUBETHA_OK);
    CHECK(report.failed == 0 && report.removed == 3);
}

static void test_capacity_pubsub(const char *scratch_prefix)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_capacity_pubsub_create_anon(4, &strict_options, &h), SUBETHA_OK);
    subetha_handle sub = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_capacity_pubsub_subscribe_from_now(h, SUBETHA_MODE_STRICT, &sub), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(sub, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_CAPACITY_SUBSCRIBER);
    for (uint8_t i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_capacity_pubsub_publish(h, &i, 1, NULL), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_capacity_pubsub_morph_capacity(h, 8), SUBETHA_OK);
    for (uint8_t i = 3; i < 6; i++) {
        EXPECT_CODE(subetha_capacity_pubsub_publish(h, &i, 1, NULL), SUBETHA_OK);
    }
    subetha_capacity_pubsub_stats stats;
    EXPECT_CODE(subetha_capacity_pubsub_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.chain_len == 2 && stats.current_capacity == 8 && stats.chain_total_capacity == 12);
    uint8_t out[SUBETHA_PUBSUB_PAYLOAD_BYTES];
    size_t len = 0;
    for (uint8_t i = 0; i < 6; i++) {
        EXPECT_CODE(subetha_capacity_subscriber_next_wait(sub, out, sizeof out, &len, 1000), SUBETHA_OK);
        CHECK(len == SUBETHA_PUBSUB_PAYLOAD_BYTES && out[0] == i);
    }
    EXPECT_CODE(subetha_capacity_subscriber_try_next(sub, out, sizeof out, &len), SUBETHA_E_PUBSUB_PENDING);
    uint64_t backing = 9, position = 9;
    EXPECT_CODE(subetha_capacity_subscriber_position(sub, &backing, &position), SUBETHA_OK);
    CHECK(backing == 1 && position == 3);
    uint32_t reclaimed = 9;
    EXPECT_CODE(subetha_capacity_pubsub_gc(h, &reclaimed), SUBETHA_OK);
    CHECK(reclaimed == 1);
    EXPECT_CODE(subetha_capacity_pubsub_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.chain_len == 1);
    subetha_handle oldest = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_capacity_pubsub_subscribe_from_oldest(h, SUBETHA_MODE_STRICT, &oldest), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_subscriber_try_next(oldest, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 3);
    uint32_t mode = 9;
    EXPECT_CODE(subetha_capacity_subscriber_mode(oldest, &mode), SUBETHA_OK);
    CHECK(mode == SUBETHA_MODE_STRICT);
    EXPECT_CODE(subetha_capacity_pubsub_prewarm(h, 16), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_pubsub_clear_warm(h), SUBETHA_OK);
    EXPECT_CODE(subetha_capacity_pubsub_wake_all(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(oldest), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(sub), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    char base[1024];
    snprintf(base, sizeof base, "%s-cappubsub", scratch_prefix);
    EXPECT_CODE(subetha_capacity_pubsub_create(base, 8, &strict_options, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report = {99, 99, 99};
    EXPECT_CODE(subetha_capacity_pubsub_unlink(base, &report), SUBETHA_OK);
    CHECK(report.failed == 0 && report.removed == 2);
}

static void test_ordered_receiver(void)
{
    subetha_ring_options counted = strict_options;
    counted.stamps = SUBETHA_STAMPS_SHARED_COUNTER;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(2, 1, 64, &counted, &h), SUBETHA_OK);
    uint32_t p0 = 0, p1 = 0, cid = 0;
    EXPECT_CODE(subetha_ring_register_producer(h, &p0), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_producer(h, &p1), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(h, &cid), SUBETHA_OK);
    subetha_handle rx = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_ordered_receiver(h, cid, SUBETHA_MODE_STRICT, &rx), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(rx, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_ORDERED_RECEIVER);
    subetha_ordered_stats stats;
    EXPECT_CODE(subetha_ordered_read_stats(rx, &stats), SUBETHA_OK);
    CHECK(stats.strategy == SUBETHA_ORDERED_REORDER && stats.window >= 2);
    for (uint8_t i = 0; i < 20; i++) {
        EXPECT_CODE(subetha_ring_try_push(h, (i % 2 == 0) ? p0 : p1, &i, 1), SUBETHA_OK);
    }
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    uint64_t stamp = 0, last = 0;
    unsigned seen = 0;
    int32_t rc;
    while ((rc = subetha_ordered_try_next(rx, out, sizeof out, &len, &stamp)) == SUBETHA_OK) {
        CHECK(stamp >= last);
        last = stamp;
        seen |= 1u << out[0];
    }
    EXPECT_CODE(rc, SUBETHA_E_RING_EMPTY);
    while ((rc = subetha_ordered_flush(rx, out, sizeof out, &len, &stamp)) == SUBETHA_OK) {
        CHECK(stamp >= last);
        last = stamp;
        seen |= 1u << out[0];
    }
    EXPECT_CODE(rc, SUBETHA_E_RING_EMPTY);
    CHECK(seen == 0xFFFFFu);
    EXPECT_CODE(subetha_ordered_next_wait(rx, out, sizeof out, &len, &stamp, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_ordered_try_next(rx, out, sizeof out, &len, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_handle_destroy(rx), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* Time stamps need no correction, and an unstamped ring has no receiver. */
    subetha_ring_options timed = strict_options;
    timed.stamps = SUBETHA_STAMPS_MONOTONIC;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 8, &timed, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(h, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_ordered_receiver(h, cid, SUBETHA_MODE_STRICT, &rx), SUBETHA_OK);
    EXPECT_CODE(subetha_ordered_read_stats(rx, &stats), SUBETHA_OK);
    CHECK(stats.strategy == SUBETHA_ORDERED_DIRECT);
    EXPECT_CODE(subetha_handle_destroy(rx), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 8, &strict_options, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_ordered_receiver(h, 0, SUBETHA_MODE_STRICT, &rx), SUBETHA_E_RING_NOT_STAMPED);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
}

#if defined(SUBETHA_TEST_HOOKS)
static void test_panic_poisons_only_its_handle(void)
{
    subetha_handle a = SUBETHA_HANDLE_NONE, b = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &a), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &b), SUBETHA_OK);
    uint64_t before = subetha_panic_count();

    EXPECT_CODE(subetha_test_panic_free(), SUBETHA_E_PANIC);
    CHECK(subetha_panic_count() == before + 1);
    char msg[128];
    size_t n = subetha_last_panic_message(msg, sizeof msg);
    CHECK(n > 1 && strstr(msg, "subetha_test_panic_free") != NULL);

    EXPECT_CODE(subetha_test_panic_on(a), SUBETHA_E_PANIC);
    CHECK(subetha_panic_count() == before + 2);
    bool poisoned = false;
    EXPECT_CODE(subetha_handle_is_poisoned(a, &poisoned), SUBETHA_OK);
    CHECK(poisoned);
    EXPECT_CODE(subetha_handle_is_poisoned(b, &poisoned), SUBETHA_OK);
    CHECK(!poisoned);
    subetha_ring_stats stats;
    EXPECT_CODE(subetha_ring_read_stats(a, &stats), SUBETHA_E_HANDLE_POISONED);
    EXPECT_CODE(subetha_ring_read_stats(b, &stats), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(a, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_RING);
    /* Destroying the poisoned handle is the recovery. */
    EXPECT_CODE(subetha_handle_destroy(a), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(b), SUBETHA_OK);
}
#endif

static void test_shutdown_closes_what_was_left(void)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &h), SUBETHA_OK);
    CHECK(subetha_live_handles() == 1);
    /* The leak is reported, not hidden. */
    EXPECT_CODE(subetha_shutdown(), SUBETHA_E_HANDLES_WERE_LIVE);
    CHECK(subetha_live_handles() == 0);
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &h), SUBETHA_E_SHUT_DOWN);
    EXPECT_CODE(subetha_shutdown(), SUBETHA_E_SHUT_DOWN);
    /* And the library comes back. */
    EXPECT_CODE(subetha_init(SUBETHA_MODE_MANAGED), SUBETHA_OK);
    uint32_t mode = 0;
    EXPECT_CODE(subetha_default_mode(&mode), SUBETHA_OK);
    CHECK(mode == SUBETHA_MODE_MANAGED);
    EXPECT_CODE(subetha_shutdown(), SUBETHA_OK);
    EXPECT_CODE(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
}

static void test_shared_stack(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-stack.bin", scratch_prefix);
    const subetha_element_layout layout = {24, 8, 42};
    const subetha_element_layout empty = {0, 8, 42};
    const subetha_element_layout other_tag = {24, 8, 43};
    subetha_handle h = SUBETHA_HANDLE_NONE;
    subetha_handle again = SUBETHA_HANDLE_NONE;
    subetha_handle wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_stack_create(path, 0, &layout, &strict_options, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_stack_create(path, 4, &empty, &strict_options, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_stack_create(path, 4, NULL, &strict_options, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_stack_open(path, 4, &layout, &strict_options, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_stack_create(path, 4, &layout, &strict_options, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_STACK);

    uint8_t out[24];
    size_t len = 0;
    EXPECT_CODE(subetha_stack_try_pop(h, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_stack_peek(h, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_stack_pop_wait(h, out, sizeof out, &len, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_stack_try_pop(h, out, 23, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    uint8_t element[25];
    memset(element, 0xAB, sizeof element);
    EXPECT_CODE(subetha_stack_try_push(h, element, 25), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    for (uint8_t i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_stack_try_push(h, &i, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_stack_try_push(h, element, 1), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_stack_push_wait(h, element, 1, 20), SUBETHA_E_TIMEOUT);

    /* A second handle on the file sees the entries; another capacity or
     * tag is refused. */
    EXPECT_CODE(subetha_stack_create(path, 4, &layout, &strict_options, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_stack_open(path, 8, &layout, &strict_options, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_stack_open(path, 4, &other_tag, &strict_options, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    subetha_stack_stats stats;
    EXPECT_CODE(subetha_stack_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.element_size == 24 && stats.alignment == 8 && stats.tag == 42);
    CHECK(stats.approx_len == 4);
    EXPECT_CODE(subetha_stack_peek(again, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 24 && out[0] == 3 && out[1] == 0);
    for (uint8_t i = 4; i-- > 0;) {
        EXPECT_CODE(subetha_stack_pop_wait(again, out, sizeof out, &len, 1000), SUBETHA_OK);
        CHECK(len == 24 && out[0] == i && out[23] == 0);
    }
    EXPECT_CODE(subetha_stack_try_pop(h, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_stack_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_stack_wake_all(h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A reset strips the entries; every handle goes first, because a mapped
     * file cannot be truncated on Windows. */
    EXPECT_CODE(subetha_stack_create(path, 4, &layout, &strict_options, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_stack_try_push(h, element, 1), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_stack_reset(path, 4, &layout, &strict_options, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_stack_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.approx_len == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    subetha_unlink_report report;
    EXPECT_CODE(subetha_stack_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 3 && report.missing == 0 && report.failed == 0);
    EXPECT_CODE(subetha_stack_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 3);
}

static void test_work_deque(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-deque.bin", scratch_prefix);
    const subetha_element_layout layout = {12, 4, 7};
    const subetha_element_layout wider = {20, 4, 7};
    const subetha_element_layout other_tag = {12, 4, 8};
    subetha_handle owner = SUBETHA_HANDLE_NONE;
    subetha_handle thief = SUBETHA_HANDLE_NONE;
    subetha_handle wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_deque_create(path, 6, &layout, &strict_options, &owner), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_deque_open_thief(path, &layout, &strict_options, &thief), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_deque_create(path, 4, &layout, &strict_options, &owner), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(owner, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_DEQUE);
    EXPECT_CODE(subetha_deque_open_thief(path, &layout, &strict_options, &thief), SUBETHA_OK);
    EXPECT_CODE(subetha_deque_open_thief(path, &wider, &strict_options, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_deque_open_thief(path, &other_tag, &strict_options, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);

    uint8_t out[16];
    size_t len = 0;
    EXPECT_CODE(subetha_deque_try_pop(owner, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_deque_try_steal(thief, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_deque_steal_wait(thief, out, sizeof out, &len, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_deque_try_steal(thief, out, 15, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    uint8_t element[13];
    memset(element, 0xCD, sizeof element);
    EXPECT_CODE(subetha_deque_try_push(owner, element, 13), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    EXPECT_CODE(subetha_deque_try_push(thief, element, 1), SUBETHA_E_DEQUE_NOT_OWNER);
    EXPECT_CODE(subetha_deque_try_pop(thief, out, sizeof out, &len), SUBETHA_E_DEQUE_NOT_OWNER);
    for (uint8_t i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_deque_try_push(owner, &i, 1), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_deque_try_push(owner, element, 1), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_deque_push_wait(owner, element, 1, 20), SUBETHA_E_TIMEOUT);
    subetha_deque_stats stats;
    EXPECT_CODE(subetha_deque_read_stats(thief, &stats), SUBETHA_OK);
    CHECK(stats.role == SUBETHA_DEQUE_THIEF && stats.capacity == 4 && stats.slot_bytes == 16);
    CHECK(stats.element_size == 12 && stats.alignment == 4 && stats.tag == 7);
    CHECK(stats.top == 0 && stats.bottom == 4 && stats.approx_len == 4);

    /* The owner pops the newest element; a steal takes the oldest. */
    EXPECT_CODE(subetha_deque_try_pop(owner, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 16 && out[0] == 3 && out[1] == 0);
    EXPECT_CODE(subetha_deque_steal_wait(thief, out, sizeof out, &len, 1000), SUBETHA_OK);
    CHECK(len == 16 && out[0] == 0);
    EXPECT_CODE(subetha_deque_try_steal(owner, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 1);
    EXPECT_CODE(subetha_deque_try_pop(owner, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 2);
    EXPECT_CODE(subetha_deque_try_pop(owner, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_deque_read_stats(owner, &stats), SUBETHA_OK);
    CHECK(stats.role == SUBETHA_DEQUE_OWNER && stats.approx_len == 0 && stats.top == stats.bottom);
    EXPECT_CODE(subetha_deque_flush(owner), SUBETHA_OK);
    EXPECT_CODE(subetha_deque_wake_all(thief), SUBETHA_OK);
    EXPECT_CODE(subetha_stack_try_push(owner, element, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(thief), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(owner), SUBETHA_OK);

    subetha_unlink_report report;
    EXPECT_CODE(subetha_deque_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 3 && report.missing == 0 && report.failed == 0);
}

/* Wait up to `timeout_ms` on a notifier's native object the way an event
 * loop would: poll() on the descriptor on Unix, WaitForSingleObject on the
 * event on Windows. Returns 1 when it was signaled, 0 on a timeout, -1 on
 * an error. */
int subetha_ctest_wait_native(uint64_t native, int timeout_ms)
{
#if defined(_WIN32)
    DWORD rc = WaitForSingleObject((HANDLE)(uintptr_t)native, (DWORD)timeout_ms);
    if (rc == WAIT_OBJECT_0) {
        return 1;
    }
    return rc == WAIT_TIMEOUT ? 0 : -1;
#else
    struct pollfd fds;
    fds.fd = (int)native;
    fds.events = POLLIN;
    fds.revents = 0;
    int rc = poll(&fds, 1, timeout_ms);
    if (rc < 0) {
        return -1;
    }
    return (rc > 0 && (fds.revents & POLLIN) != 0) ? 1 : 0;
#endif
}

static void test_notifier(const char *scratch_prefix)
{
    subetha_handle h = SUBETHA_HANDLE_NONE;
    subetha_handle n = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &h), SUBETHA_OK);
    uint32_t pid = 0, cid = 0;
    EXPECT_CODE(subetha_ring_register_producer(h, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(h, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_notifier(h, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_notifier(h, &n), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(n, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_NOTIFIER);
    bool signaled = true;
    EXPECT_CODE(subetha_notifier_is_signaled(n, &signaled), SUBETHA_OK);
    CHECK(!signaled);
    EXPECT_CODE(subetha_notifier_wait(n, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_notifier_wait(n, -2), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_try_push(h, pid, (const uint8_t *)"a", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_notifier_wait(n, 1000), SUBETHA_OK);
    EXPECT_CODE(subetha_notifier_is_signaled(n, &signaled), SUBETHA_OK);
    CHECK(signaled);
    EXPECT_CODE(subetha_notifier_drain(n), SUBETHA_OK);
    EXPECT_CODE(subetha_notifier_is_signaled(n, &signaled), SUBETHA_OK);
    CHECK(!signaled);
    /* The native object is what an event loop watches. */
    uint64_t native = 0;
    EXPECT_CODE(subetha_notifier_native(n, &native), SUBETHA_OK);
    CHECK(subetha_ctest_wait_native(native, 20) == 0);
    EXPECT_CODE(subetha_ring_try_push(h, pid, (const uint8_t *)"b", 1), SUBETHA_OK);
    CHECK(subetha_ctest_wait_native(native, 1000) == 1);
    EXPECT_CODE(subetha_notifier_drain(n), SUBETHA_OK);
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    EXPECT_CODE(subetha_ring_try_pop(h, cid, out, sizeof out, &len), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_pop(h, cid, out, sizeof out, &len), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_pop(h, cid, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_ring_try_push(n, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(n), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* On a file-backed ring a second handle's push signals the notifier
     * through the record beside the ring. */
    char prefix[1024];
    snprintf(prefix, sizeof prefix, "%s-notify", scratch_prefix);
    subetha_handle creator = SUBETHA_HANDLE_NONE, attacher = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create(prefix, 1, 1, 64, &strict_options, &creator), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_open(prefix, 1, 1, 64, &strict_options, &attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(creator, &cid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_producer(attacher, &pid), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_notifier(creator, &n), SUBETHA_OK);
    EXPECT_CODE(subetha_notifier_wait(n, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_ring_try_push(attacher, pid, (const uint8_t *)"c", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_notifier_wait(n, 1000), SUBETHA_OK);
    EXPECT_CODE(subetha_notifier_drain(n), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_pop(creator, cid, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 'c');
    EXPECT_CODE(subetha_handle_destroy(n), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(attacher, pid, (const uint8_t *)"d", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(attacher), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(creator), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_ring_unlink(prefix, 1, &report), SUBETHA_OK);
    CHECK(report.failed == 0 && report.removed >= 4);

    /* The SPSC ring carries a notifier the same way. */
    EXPECT_CODE(subetha_spsc_create_anon(64, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_spsc_notifier(h, &n), SUBETHA_OK);
    EXPECT_CODE(subetha_notifier_wait(n, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_spsc_try_push(h, (const uint8_t *)"s", 1), SUBETHA_OK);
    EXPECT_CODE(subetha_notifier_wait(n, 1000), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(n), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
}

static void test_shared_hashmap(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-hashmap.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_hashmap_create(path, 1, 4, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hashmap_create(path, 16, 0, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hashmap_create(path, 16, 40, 9, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hashmap_open(path, 16, 4, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_hashmap_create(path, 16, 4, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_HASHMAP);

    uint8_t key[4] = {1, 2, 3, 4};
    uint8_t value[8] = {10, 20, 30, 40, 50, 60, 70, 80};
    uint8_t out[8];
    size_t len = 0;
    uint32_t outcome = 99;
    EXPECT_CODE(subetha_hashmap_get(h, key, sizeof key, out, sizeof out, &len), SUBETHA_E_MAP_KEY_ABSENT);
    EXPECT_CODE(subetha_hashmap_insert(h, key, 3, value, sizeof value, &outcome), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hashmap_insert(h, key, sizeof key, value, 7, &outcome), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hashmap_insert(h, key, sizeof key, value, sizeof value, &outcome), SUBETHA_OK);
    CHECK(outcome == SUBETHA_MAP_INSERTED);
    value[0] = 11;
    EXPECT_CODE(subetha_hashmap_insert(h, key, sizeof key, value, sizeof value, &outcome), SUBETHA_OK);
    CHECK(outcome == SUBETHA_MAP_UPDATED);
    EXPECT_CODE(subetha_hashmap_get(h, key, sizeof key, out, 7, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_hashmap_get(h, key, sizeof key, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 8 && out[0] == 11 && out[7] == 80);
    bool present = false;
    EXPECT_CODE(subetha_hashmap_contains(h, key, sizeof key, &present), SUBETHA_OK);
    CHECK(present);

    /* insert_if_absent leaves a present key alone and reports its value. */
    uint8_t other[4] = {9, 9, 9, 9};
    uint8_t fresh[8] = {1, 1, 1, 1, 1, 1, 1, 1};
    uint8_t existing[8];
    size_t existing_len = 0;
    EXPECT_CODE(subetha_hashmap_insert_if_absent(h, key, sizeof key, fresh, sizeof fresh, existing, sizeof existing,
                                                 &existing_len, &present),
                SUBETHA_OK);
    CHECK(present && existing_len == 8 && existing[0] == 11);
    EXPECT_CODE(subetha_hashmap_insert_if_absent(h, other, sizeof other, fresh, sizeof fresh, existing, sizeof existing,
                                                 &existing_len, &present),
                SUBETHA_OK);
    CHECK(!present && existing_len == 0);

    /* compare_exchange swaps only on a byte-equal current value. */
    uint8_t expected[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    uint8_t replacement[8] = {2, 2, 2, 2, 2, 2, 2, 2};
    uint8_t current[8];
    size_t current_len = 0;
    bool swapped = true;
    EXPECT_CODE(subetha_hashmap_compare_exchange(h, other, sizeof other, expected, sizeof expected, replacement,
                                                 sizeof replacement, current, sizeof current, &current_len, &swapped),
                SUBETHA_OK);
    CHECK(!swapped && current_len == 8 && current[0] == 1);
    EXPECT_CODE(subetha_hashmap_compare_exchange(h, other, sizeof other, fresh, sizeof fresh, replacement,
                                                 sizeof replacement, current, sizeof current, &current_len, &swapped),
                SUBETHA_OK);
    CHECK(swapped && current_len == 0);
    uint8_t absent[4] = {7, 7, 7, 7};
    EXPECT_CODE(subetha_hashmap_compare_exchange(h, absent, sizeof absent, fresh, sizeof fresh, replacement,
                                                 sizeof replacement, current, sizeof current, &current_len, &swapped),
                SUBETHA_E_MAP_KEY_ABSENT);

    /* A second handle on the file sees the entries; a walk finds both. */
    EXPECT_CODE(subetha_hashmap_create(path, 16, 4, 8, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_hashmap_open(path, 16, 8, 4, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_hashmap_open(path, 32, 4, 8, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    subetha_hashmap_stats stats;
    EXPECT_CODE(subetha_hashmap_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 16 && stats.len == 2 && stats.key_size == 4 && stats.value_size == 8);
    CHECK(stats.tombstones == 0 && stats.load_factor == 2.0 / 16.0);
    uint64_t cursor = 0;
    uint8_t walked_key[4];
    size_t walked_key_len = 0, walked_value_len = 0;
    unsigned walked = 0;
    bool found = true;
    for (;;) {
        EXPECT_CODE(subetha_hashmap_next(again, &cursor, walked_key, sizeof walked_key, &walked_key_len, out, sizeof out,
                                         &walked_value_len, &found),
                    SUBETHA_OK);
        if (!found) {
            break;
        }
        CHECK(walked_key_len == 4 && walked_value_len == 8);
        walked++;
    }
    CHECK(walked == 2);

    /* Remove, tombstones, compact, clear. */
    EXPECT_CODE(subetha_hashmap_remove(h, key, sizeof key, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 8 && out[0] == 11);
    EXPECT_CODE(subetha_hashmap_remove(h, key, sizeof key, out, sizeof out, &len), SUBETHA_E_MAP_KEY_ABSENT);
    EXPECT_CODE(subetha_hashmap_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 1 && stats.tombstones == 1);
    uint64_t reclaimed = 0;
    EXPECT_CODE(subetha_hashmap_compact(h, &reclaimed), SUBETHA_OK);
    CHECK(reclaimed == 1);
    EXPECT_CODE(subetha_hashmap_get(again, other, sizeof other, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 2);
    EXPECT_CODE(subetha_hashmap_clear(h), SUBETHA_OK);
    EXPECT_CODE(subetha_hashmap_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.len == 0);

    /* A full table refuses the next key. */
    for (uint32_t i = 0; i < 16; i++) {
        uint8_t k[4] = {(uint8_t)i, 0, 0, 0};
        EXPECT_CODE(subetha_hashmap_insert(h, k, sizeof k, value, sizeof value, NULL), SUBETHA_OK);
    }
    uint8_t seventeenth[4] = {16, 0, 0, 0};
    EXPECT_CODE(subetha_hashmap_insert(h, seventeenth, sizeof seventeenth, value, sizeof value, NULL), SUBETHA_E_MAP_FULL);
    EXPECT_CODE(subetha_hashmap_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A reset strips the entries once every handle is gone. */
    EXPECT_CODE(subetha_hashmap_reset(path, 16, 4, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_hashmap_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_hashmap_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.missing == 0 && report.failed == 0);
    EXPECT_CODE(subetha_hashmap_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_string_arena(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-arena.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, reader = SUBETHA_HANDLE_NONE,
                   wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_arena_create(path, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_arena_create(path, SUBETHA_ARENA_CAPACITY_MAX + 1, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_arena_open(path, 256, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_arena_open_read_only(path, 256, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_arena_create(path, 256, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_ARENA);

    /* A reference names the offset and length an intern landed at. */
    uint64_t ref = 0, empty = 0, packed = 0;
    EXPECT_CODE(subetha_arena_intern(h, (const uint8_t *)"hello", 5, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_arena_intern(h, (const uint8_t *)"hello", 5, &ref), SUBETHA_OK);
    CHECK(subetha_arena_ref_offset(ref) == 0 && subetha_arena_ref_len(ref) == 5);
    EXPECT_CODE(subetha_arena_intern(h, NULL, 0, &empty), SUBETHA_OK);
    CHECK(subetha_arena_ref_offset(empty) == 5 && subetha_arena_ref_len(empty) == 0);
    uint8_t out[8];
    size_t len = 0;
    EXPECT_CODE(subetha_arena_get(h, ref, out, 4, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_arena_get(h, ref, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 5 && memcmp(out, "hello", 5) == 0);
    EXPECT_CODE(subetha_arena_get(h, empty, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 0);
    const uint8_t *view = NULL;
    size_t view_len = 0;
    EXPECT_CODE(subetha_arena_view(h, ref, &view, &view_len), SUBETHA_OK);
    CHECK(view != NULL && view_len == 5 && memcmp(view, "hello", 5) == 0);
    EXPECT_CODE(subetha_arena_ref_pack(0, 5, &packed), SUBETHA_OK);
    CHECK(packed == ref);
    EXPECT_CODE(subetha_arena_ref_pack(SUBETHA_ARENA_CAPACITY_MAX + 1, 5, &packed), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_arena_ref_pack(0, (uint32_t)SUBETHA_ARENA_VALUE_MAX + 1, &packed), SUBETHA_E_INVALID_ARGUMENT);
    uint64_t beyond = 0;
    EXPECT_CODE(subetha_arena_ref_pack(5, 100, &beyond), SUBETHA_OK);
    EXPECT_CODE(subetha_arena_get(h, beyond, out, sizeof out, &len), SUBETHA_E_ARENA_INVALID_REF);
    EXPECT_CODE(subetha_arena_view(h, beyond, &view, &view_len), SUBETHA_E_ARENA_INVALID_REF);

    /* A second writable handle and a read-only one resolve the same bytes;
     * a capacity the file was not built with is refused. */
    EXPECT_CODE(subetha_arena_open(path, 256, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_arena_open(path, 512, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_arena_open(path, 128, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_arena_open_read_only(path, 256, SUBETHA_MODE_STRICT, &reader), SUBETHA_OK);
    EXPECT_CODE(subetha_arena_get(reader, ref, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 5 && memcmp(out, "hello", 5) == 0);
    EXPECT_CODE(subetha_arena_intern(reader, (const uint8_t *)"no", 2, &packed), SUBETHA_E_READ_ONLY);
    EXPECT_CODE(subetha_arena_clear(reader), SUBETHA_E_READ_ONLY);
    subetha_arena_stats stats;
    EXPECT_CODE(subetha_arena_read_stats(reader, &stats), SUBETHA_OK);
    CHECK(!stats.writable && stats.capacity_bytes == 256 && stats.used_bytes == 5 && stats.remaining_bytes == 251);
    EXPECT_CODE(subetha_arena_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.writable && stats.mode == SUBETHA_MODE_STRICT);

    /* A value past the room is refused whole; one that fits lands. */
    uint8_t big[300];
    memset(big, 'x', sizeof big);
    EXPECT_CODE(subetha_arena_intern(again, big, sizeof big, &packed), SUBETHA_E_ARENA_FULL);
    EXPECT_CODE(subetha_arena_intern(again, big, 251, &packed), SUBETHA_OK);
    CHECK(subetha_arena_ref_offset(packed) == 5 && subetha_arena_ref_len(packed) == 251);
    EXPECT_CODE(subetha_arena_intern(again, big, 1, &packed), SUBETHA_E_ARENA_FULL);
    EXPECT_CODE(subetha_arena_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.used_bytes == 256 && stats.remaining_bytes == 0);
    EXPECT_CODE(subetha_arena_flush(h), SUBETHA_OK);

    /* A clear empties the arena for every handle; old references stop
     * resolving. */
    EXPECT_CODE(subetha_arena_clear(h), SUBETHA_OK);
    EXPECT_CODE(subetha_arena_read_stats(reader, &stats), SUBETHA_OK);
    CHECK(stats.used_bytes == 0);
    EXPECT_CODE(subetha_arena_get(again, ref, out, sizeof out, &len), SUBETHA_E_ARENA_INVALID_REF);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(reader), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A reset lays out an empty arena once every handle is gone. */
    EXPECT_CODE(subetha_arena_reset(path, 128, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_arena_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity_bytes == 128 && stats.used_bytes == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_arena_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.missing == 0 && report.failed == 0);
    EXPECT_CODE(subetha_arena_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_shared_vec(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-vec.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, reader = SUBETHA_HANDLE_NONE,
                   wrong = SUBETHA_HANDLE_NONE;
    const subetha_element_layout layout = {24, 8, 0x5645433234ULL};
    const subetha_element_layout other_tag = {24, 8, 1};
    const subetha_element_layout wide = {24, 128, 0x5645433234ULL};
    EXPECT_CODE(subetha_vec_create(path, 0, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_vec_create(path, 4, NULL, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_vec_open(path, 4, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_vec_create(path, 4, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_VEC);

    /* Pushes land at consecutive indexes; a full vec refuses the next. */
    uint8_t element[24], out[24];
    size_t len = 0;
    uint64_t index = 99;
    memset(element, 1, sizeof element);
    EXPECT_CODE(subetha_vec_get(h, 0, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_vec_pop_back(h, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_vec_push_back(h, element, 23, &index), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_vec_push_back(h, element, sizeof element, &index), SUBETHA_OK);
    CHECK(index == 0);
    for (uint8_t i = 1; i < 4; i++) {
        memset(element, i + 1, sizeof element);
        EXPECT_CODE(subetha_vec_push_back(h, element, sizeof element, &index), SUBETHA_OK);
        CHECK(index == i);
    }
    EXPECT_CODE(subetha_vec_push_back(h, element, sizeof element, NULL), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_vec_get(h, 2, out, 23, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_vec_get(h, 2, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 24 && out[0] == 3 && out[23] == 3);
    memset(element, 42, sizeof element);
    EXPECT_CODE(subetha_vec_set(h, 2, element, sizeof element), SUBETHA_OK);
    EXPECT_CODE(subetha_vec_set(h, 4, element, sizeof element), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_vec_get(h, 2, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 42);

    /* A second handle and a read-only one see the elements; another
     * layout, capacity or alignment is refused. */
    EXPECT_CODE(subetha_vec_open(path, 4, &layout, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_vec_open(path, 4, &other_tag, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_vec_open(path, 8, &layout, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_vec_open(path, 4, &wide, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_vec_open_read_only(path, 4, &layout, SUBETHA_MODE_STRICT, &reader), SUBETHA_OK);
    subetha_vec_stats stats;
    EXPECT_CODE(subetha_vec_read_stats(reader, &stats), SUBETHA_OK);
    CHECK(!stats.writable && stats.capacity == 4 && stats.len == 4 && stats.element_size == 24);
    CHECK(stats.alignment == 8 && stats.tag == layout.tag && stats.slot_stride == 64);
    EXPECT_CODE(subetha_vec_get(reader, 2, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 42);
    EXPECT_CODE(subetha_vec_push_back(reader, element, sizeof element, NULL), SUBETHA_E_READ_ONLY);
    EXPECT_CODE(subetha_vec_pop_back(reader, out, sizeof out, &len), SUBETHA_E_READ_ONLY);
    EXPECT_CODE(subetha_vec_set(reader, 0, element, sizeof element), SUBETHA_E_READ_ONLY);
    EXPECT_CODE(subetha_vec_clear(reader), SUBETHA_E_READ_ONLY);

    /* A pop and a clear are seen by every handle. */
    EXPECT_CODE(subetha_vec_pop_back(again, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 24 && out[0] == 4);
    EXPECT_CODE(subetha_vec_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 3 && stats.writable && stats.mode == SUBETHA_MODE_STRICT);
    EXPECT_CODE(subetha_vec_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_vec_clear(h), SUBETHA_OK);
    EXPECT_CODE(subetha_vec_read_stats(reader, &stats), SUBETHA_OK);
    CHECK(stats.len == 0);
    EXPECT_CODE(subetha_vec_get(again, 0, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_vec_push_back(again, element, sizeof element, &index), SUBETHA_OK);
    CHECK(index == 0);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(reader), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A reset lays out an empty vec; a wide alignment gets its own slot
     * geometry. */
    EXPECT_CODE(subetha_vec_reset(path, 2, &wide, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_vec_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 0 && stats.capacity == 2 && stats.alignment == 128 && stats.slot_stride == 256);
    EXPECT_CODE(subetha_vec_push_back(h, element, sizeof element, &index), SUBETHA_OK);
    EXPECT_CODE(subetha_vec_get(h, 0, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 42);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_vec_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.missing == 0 && report.failed == 0);
    EXPECT_CODE(subetha_vec_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_shared_slab(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-slab.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, reader = SUBETHA_HANDLE_NONE,
                   wrong = SUBETHA_HANDLE_NONE;
    const subetha_element_layout layout = {100, 8, 0x534c4142313030ULL};
    const subetha_element_layout other_tag = {100, 8, 2};
    const subetha_element_layout wide = {100, 256, 0x534c4142313030ULL};
    EXPECT_CODE(subetha_slab_create(path, 0, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_slab_open(path, 4, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_slab_create(path, 4, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_SLAB);

    /* A slot nothing wrote reads as zeros at version 0; a write is one
     * version step of two. */
    uint8_t record[100], out[100];
    size_t len = 0;
    uint32_t version = 9;
    EXPECT_CODE(subetha_slab_get(h, 3, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 100 && out[0] == 0 && out[99] == 0);
    EXPECT_CODE(subetha_slab_slot_version(h, 3, &version), SUBETHA_OK);
    CHECK(version == 0);
    EXPECT_CODE(subetha_slab_get(h, 4, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_slab_set(h, 4, record, sizeof record), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_slab_slot_version(h, 4, &version), SUBETHA_E_OUT_OF_BOUNDS);
    memset(record, 7, sizeof record);
    record[99] = 8;
    EXPECT_CODE(subetha_slab_set(h, 3, record, 99), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_slab_set(h, 3, record, sizeof record), SUBETHA_OK);
    EXPECT_CODE(subetha_slab_get(h, 3, out, 99, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_slab_get(h, 3, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 100 && out[0] == 7 && out[99] == 8);
    EXPECT_CODE(subetha_slab_slot_version(h, 3, &version), SUBETHA_OK);
    CHECK(version == 2);

    /* A second handle and a read-only one see the record; another
     * layout or capacity is refused. */
    EXPECT_CODE(subetha_slab_open(path, 4, &layout, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_slab_open(path, 4, &other_tag, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_slab_open(path, 8, &layout, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_slab_open(path, 4, &wide, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_slab_open_read_only(path, 4, &layout, SUBETHA_MODE_STRICT, &reader), SUBETHA_OK);
    subetha_slab_stats stats;
    EXPECT_CODE(subetha_slab_read_stats(reader, &stats), SUBETHA_OK);
    CHECK(!stats.writable && stats.capacity == 4 && stats.element_size == 100 && stats.alignment == 8);
    CHECK(stats.tag == layout.tag && stats.slot_stride == 128);
    EXPECT_CODE(subetha_slab_get(reader, 3, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 7);
    EXPECT_CODE(subetha_slab_set(reader, 3, record, sizeof record), SUBETHA_E_READ_ONLY);
    EXPECT_CODE(subetha_slab_get(again, 3, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[99] == 8);
    EXPECT_CODE(subetha_slab_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.writable && stats.mode == SUBETHA_MODE_STRICT);
    EXPECT_CODE(subetha_slab_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(reader), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A reset lays out an empty slab; a wide alignment gets its own slot
     * geometry. */
    EXPECT_CODE(subetha_slab_reset(path, 2, &wide, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_slab_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 2 && stats.alignment == 256 && stats.slot_stride == 512);
    EXPECT_CODE(subetha_slab_get(h, 1, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 0);
    EXPECT_CODE(subetha_slab_set(h, 1, record, sizeof record), SUBETHA_OK);
    EXPECT_CODE(subetha_slab_get(h, 1, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 7 && out[99] == 8);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_slab_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.missing == 0 && report.failed == 0);
    EXPECT_CODE(subetha_slab_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_shared_region(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-region.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    const subetha_element_layout layout = {24, 8, 0x524547494f4eULL};
    const subetha_element_layout other_tag = {24, 8, 3};
    const subetha_element_layout wide = {24, 64, 0x524547494f4eULL};
    EXPECT_CODE(subetha_region_create(path, 0, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_region_create(path, SUBETHA_REGION_NIL_INDEX, &layout, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_region_open(path, 3, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_region_create(path, 3, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_REGION);

    /* The bump cursor hands out consecutive slots; a full region refuses
     * the next allocation. */
    uint8_t element[24], out[24];
    size_t len = 0;
    uint32_t a = 0, b = 0, c = 0, spare = 0;
    memset(element, 1, sizeof element);
    EXPECT_CODE(subetha_region_allocate(h, element, 23, &a), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_region_allocate(h, element, sizeof element, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_region_allocate(h, element, sizeof element, &a), SUBETHA_OK);
    memset(element, 2, sizeof element);
    EXPECT_CODE(subetha_region_allocate(h, element, sizeof element, &b), SUBETHA_OK);
    memset(element, 3, sizeof element);
    EXPECT_CODE(subetha_region_allocate(h, element, sizeof element, &c), SUBETHA_OK);
    CHECK(a == 0 && b == 1 && c == 2);
    EXPECT_CODE(subetha_region_allocate(h, element, sizeof element, &spare), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_region_get(h, b, out, 23, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_region_get(h, b, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 24 && out[0] == 2);
    EXPECT_CODE(subetha_region_get(h, SUBETHA_REGION_NIL_INDEX, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_region_get(h, 3, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    memset(element, 42, sizeof element);
    EXPECT_CODE(subetha_region_set(h, b, element, sizeof element), SUBETHA_OK);
    EXPECT_CODE(subetha_region_set(h, 3, element, sizeof element), SUBETHA_E_OUT_OF_BOUNDS);

    /* A second handle sees the slots; another layout or capacity is
     * refused. A freed slot is handed out again. */
    EXPECT_CODE(subetha_region_open(path, 3, &layout, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_region_open(path, 3, &other_tag, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_region_open(path, 8, &layout, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_region_open(path, 3, &wide, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_region_get(again, b, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 42);
    subetha_region_stats stats;
    EXPECT_CODE(subetha_region_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 3 && stats.len == 3 && stats.free_count == 0 && stats.element_size == 24);
    CHECK(stats.alignment == 8 && stats.tag == layout.tag && stats.slots_offset == 80);
    EXPECT_CODE(subetha_region_free(again, b, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 24 && out[0] == 42);
    EXPECT_CODE(subetha_region_free(again, SUBETHA_REGION_NIL_INDEX, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_region_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 2 && stats.free_count == 1);
    uint32_t reused = SUBETHA_REGION_NIL_INDEX;
    EXPECT_CODE(subetha_region_allocate(h, element, sizeof element, &reused), SUBETHA_OK);
    CHECK(reused == b);
    EXPECT_CODE(subetha_region_flush(h), SUBETHA_OK);

    /* A clear is seen by every handle. */
    EXPECT_CODE(subetha_region_clear(h), SUBETHA_OK);
    EXPECT_CODE(subetha_region_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.len == 0 && stats.free_count == 0);
    EXPECT_CODE(subetha_region_allocate(again, element, sizeof element, &a), SUBETHA_OK);
    CHECK(a == 0);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A reset lays out an empty region; a wide alignment moves the slot
     * array. */
    EXPECT_CODE(subetha_region_reset(path, 3, &wide, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_region_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 0 && stats.alignment == 64 && stats.slots_offset == 128);
    EXPECT_CODE(subetha_region_allocate(h, element, sizeof element, &a), SUBETHA_OK);
    EXPECT_CODE(subetha_region_get(h, a, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 42);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_region_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.missing == 0 && report.failed == 0);
    EXPECT_CODE(subetha_region_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_shared_atomics(const char *scratch_prefix)
{
    char path32[1024], path64[1024], pathb[1024];
    snprintf(path32, sizeof path32, "%s-atomic32.bin", scratch_prefix);
    snprintf(path64, sizeof path64, "%s-atomic64.bin", scratch_prefix);
    snprintf(pathb, sizeof pathb, "%s-atomicb.bin", scratch_prefix);
    subetha_handle a32 = SUBETHA_HANDLE_NONE, a64 = SUBETHA_HANDLE_NONE, ab = SUBETHA_HANDLE_NONE,
                   again = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_atomic_u32_open(path32, SUBETHA_MODE_STRICT, &a32), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_atomic_u32_create(path32, 5, SUBETHA_MODE_STRICT, &a32), SUBETHA_OK);
    EXPECT_CODE(subetha_atomic_u64_create(path64, 500, SUBETHA_MODE_STRICT, &a64), SUBETHA_OK);
    EXPECT_CODE(subetha_atomic_bool_create(pathb, false, SUBETHA_MODE_STRICT, &ab), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(a64, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_ATOMIC);
    subetha_atomic_stats stats;
    EXPECT_CODE(subetha_atomic_read_stats(a32, &stats), SUBETHA_OK);
    CHECK(stats.width == SUBETHA_ATOMIC_U32 && stats.bytes == 4 && stats.mode == SUBETHA_MODE_STRICT);
    EXPECT_CODE(subetha_atomic_read_stats(a64, &stats), SUBETHA_OK);
    CHECK(stats.width == SUBETHA_ATOMIC_U64 && stats.bytes == 8);
    EXPECT_CODE(subetha_atomic_read_stats(ab, &stats), SUBETHA_OK);
    CHECK(stats.width == SUBETHA_ATOMIC_BOOL && stats.bytes == 1);

    /* An attach leaves the live value in place; a second create does not
     * reset it. */
    EXPECT_CODE(subetha_atomic_u32_create(path32, 99, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    uint32_t v32 = 0;
    EXPECT_CODE(subetha_atomic_u32_load(again, &v32), SUBETHA_OK);
    CHECK(v32 == 5);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);

    /* The plain forms are sequentially consistent; the explicit ones take
     * an ordering, and an ordering an operation cannot have is refused. */
    uint32_t prev32 = 0;
    EXPECT_CODE(subetha_atomic_u32_store(a32, 10), SUBETHA_OK);
    EXPECT_CODE(subetha_atomic_u32_load(a32, &v32), SUBETHA_OK);
    CHECK(v32 == 10);
    EXPECT_CODE(subetha_atomic_u32_fetch_add(a32, 7, &prev32), SUBETHA_OK);
    CHECK(prev32 == 10);
    EXPECT_CODE(subetha_atomic_u32_fetch_sub_explicit(a32, 2, SUBETHA_ORDER_ACQ_REL, &prev32), SUBETHA_OK);
    CHECK(prev32 == 17);
    EXPECT_CODE(subetha_atomic_u32_fetch_or(a32, 0x80, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_atomic_u32_fetch_and(a32, 0xFF, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_atomic_u32_fetch_xor(a32, 1, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_atomic_u32_load_explicit(a32, SUBETHA_ORDER_ACQUIRE, &v32), SUBETHA_OK);
    CHECK(v32 == ((((15u | 0x80u) & 0xFFu) ^ 1u)));
    EXPECT_CODE(subetha_atomic_u32_swap(a32, 3, &prev32), SUBETHA_OK);
    CHECK(prev32 == v32);
    EXPECT_CODE(subetha_atomic_u32_load_explicit(a32, SUBETHA_ORDER_RELEASE, &v32), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_atomic_u32_load_explicit(a32, SUBETHA_ORDER_ACQ_REL, &v32), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_atomic_u32_store_explicit(a32, 3, SUBETHA_ORDER_ACQUIRE), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_atomic_u32_store_explicit(a32, 3, SUBETHA_ORDER_ACQ_REL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_atomic_u32_load_explicit(a32, 9, &v32), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_atomic_u32_load(a32, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* A compare-exchange swaps only on the value it expects. */
    bool swapped = true;
    uint32_t current = 0;
    EXPECT_CODE(subetha_atomic_u32_compare_exchange(a32, 99, 4, &current, &swapped), SUBETHA_OK);
    CHECK(!swapped && current == 3);
    EXPECT_CODE(subetha_atomic_u32_compare_exchange(a32, 3, 4, &current, &swapped), SUBETHA_OK);
    CHECK(swapped && current == 3);
    EXPECT_CODE(subetha_atomic_u32_compare_exchange_explicit(a32, 4, 5, SUBETHA_ORDER_ACQ_REL, SUBETHA_ORDER_ACQUIRE,
                                                            NULL, &swapped),
                SUBETHA_OK);
    CHECK(swapped);
    EXPECT_CODE(subetha_atomic_u32_compare_exchange_explicit(a32, 5, 6, SUBETHA_ORDER_SEQ_CST, SUBETHA_ORDER_RELEASE,
                                                            NULL, &swapped),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_atomic_u32_compare_exchange(a32, 5, 6, &current, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* The 64-bit width has the same shape, and a handle of one width
     * refuses another's entry points. */
    uint64_t v64 = 0, prev64 = 0;
    EXPECT_CODE(subetha_atomic_u64_load(a64, &v64), SUBETHA_OK);
    CHECK(v64 == 500);
    EXPECT_CODE(subetha_atomic_u64_fetch_add_explicit(a64, 1000, SUBETHA_ORDER_RELAXED, &prev64), SUBETHA_OK);
    CHECK(prev64 == 500);
    EXPECT_CODE(subetha_atomic_u64_load(a64, &v64), SUBETHA_OK);
    CHECK(v64 == 1500);
    EXPECT_CODE(subetha_atomic_u64_load(a32, &v64), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_atomic_u32_load(a64, &v32), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_atomic_bool_load(a64, &swapped), SUBETHA_E_WRONG_KIND);

    /* The flag reads, writes and swaps. */
    bool flag = true;
    EXPECT_CODE(subetha_atomic_bool_load(ab, &flag), SUBETHA_OK);
    CHECK(!flag);
    EXPECT_CODE(subetha_atomic_bool_store(ab, true), SUBETHA_OK);
    EXPECT_CODE(subetha_atomic_bool_load_explicit(ab, SUBETHA_ORDER_ACQUIRE, &flag), SUBETHA_OK);
    CHECK(flag);
    EXPECT_CODE(subetha_atomic_bool_swap(ab, false, &flag), SUBETHA_OK);
    CHECK(flag);
    EXPECT_CODE(subetha_atomic_bool_load(ab, &flag), SUBETHA_OK);
    CHECK(!flag);
    EXPECT_CODE(subetha_atomic_bool_store_explicit(ab, true, SUBETHA_ORDER_ACQUIRE), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_atomic_flush(a64), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(a64, 0, (const uint8_t *)"x", 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(ab), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(a64), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(a32), SUBETHA_OK);

    /* A reset discards the value; an open of the wrong width is refused. */
    EXPECT_CODE(subetha_atomic_u32_reset(path32, 1, SUBETHA_MODE_STRICT, &a32), SUBETHA_OK);
    EXPECT_CODE(subetha_atomic_u32_load(a32, &v32), SUBETHA_OK);
    CHECK(v32 == 1);
    EXPECT_CODE(subetha_atomic_u64_open(path32, SUBETHA_MODE_STRICT, &again), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_handle_destroy(a32), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_atomic_unlink(path32, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_atomic_unlink(path64, &report), SUBETHA_OK);
    CHECK(report.removed == 1);
    EXPECT_CODE(subetha_atomic_unlink(pathb, &report), SUBETHA_OK);
    CHECK(report.removed == 1);
    EXPECT_CODE(subetha_atomic_unlink(path32, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

/* The values a walk of `h` yields, front to back, into `out`; returns how
 * many there were, at most `cap`. */
static unsigned list_walk_forward(subetha_handle h, uint8_t (*out)[16], unsigned cap)
{
    unsigned n = 0;
    uint32_t at = SUBETHA_LIST_HEAD_INDEX;
    if (subetha_list_first(h, &at) != SUBETHA_OK) {
        return 0;
    }
    while (at != SUBETHA_LIST_HEAD_INDEX && n < cap) {
        size_t len = 0;
        if (subetha_list_get(h, at, out[n], 16, &len) != SUBETHA_OK || len != 16) {
            break;
        }
        n++;
        if (subetha_list_next(h, at, &at) != SUBETHA_OK) {
            break;
        }
    }
    return n;
}

static void test_shared_list(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-list.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    const subetha_element_layout layout = {16, 4, 0x4c495354ULL};
    const subetha_element_layout other_tag = {16, 4, 5};
    EXPECT_CODE(subetha_list_create(path, 1, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_list_open(path, 8, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_list_create(path, 8, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_LIST);

    /* An empty list is the head pointing at itself. */
    uint32_t first = 99, last = 99;
    uint8_t out[16], value[16];
    size_t len = 0;
    EXPECT_CODE(subetha_list_first(h, &first), SUBETHA_OK);
    EXPECT_CODE(subetha_list_last(h, &last), SUBETHA_OK);
    CHECK(first == SUBETHA_LIST_HEAD_INDEX && last == SUBETHA_LIST_HEAD_INDEX);
    EXPECT_CODE(subetha_list_pop_front(h, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);
    EXPECT_CODE(subetha_list_pop_back(h, out, sizeof out, &len), SUBETHA_E_RING_EMPTY);

    /* Pushes at both ends; each returns the node's own index. */
    uint32_t a = 0, b = 0, c = 0;
    memset(value, 2, sizeof value);
    EXPECT_CODE(subetha_list_push_back(h, value, 15, &b), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_list_push_back(h, value, sizeof value, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_list_push_back(h, value, sizeof value, &b), SUBETHA_OK);
    memset(value, 1, sizeof value);
    EXPECT_CODE(subetha_list_push_front(h, value, sizeof value, &a), SUBETHA_OK);
    memset(value, 3, sizeof value);
    EXPECT_CODE(subetha_list_push_back(h, value, sizeof value, &c), SUBETHA_OK);
    uint8_t walked[8][16];
    CHECK(list_walk_forward(h, walked, 8) == 3);
    CHECK(walked[0][0] == 1 && walked[1][0] == 2 && walked[2][0] == 3);
    subetha_list_stats stats;
    EXPECT_CODE(subetha_list_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 8 && stats.len == 3 && stats.element_size == 16 && stats.node_size == 24);
    CHECK(stats.alignment == 4 && stats.tag == layout.tag && stats.mode == SUBETHA_MODE_STRICT);

    /* A walk from the back crosses the same nodes in reverse. */
    uint32_t at = SUBETHA_LIST_HEAD_INDEX;
    unsigned back = 0;
    EXPECT_CODE(subetha_list_last(h, &at), SUBETHA_OK);
    while (at != SUBETHA_LIST_HEAD_INDEX) {
        EXPECT_CODE(subetha_list_get(h, at, out, sizeof out, &len), SUBETHA_OK);
        CHECK(out[0] == 3 - back);
        back++;
        EXPECT_CODE(subetha_list_prev(h, at, &at), SUBETHA_OK);
    }
    CHECK(back == 3);

    /* A removal from the middle keeps the ring whole. */
    EXPECT_CODE(subetha_list_remove(h, b, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 16 && out[0] == 2);
    CHECK(list_walk_forward(h, walked, 8) == 2);
    CHECK(walked[0][0] == 1 && walked[1][0] == 3);
    EXPECT_CODE(subetha_list_remove(h, SUBETHA_LIST_HEAD_INDEX, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_list_remove(h, SUBETHA_LIST_NIL_INDEX, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_list_get(h, 8, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_list_get(h, a, out, 15, &len), SUBETHA_E_BUFFER_TOO_SMALL);

    /* A set keeps the node where it is. */
    memset(value, 9, sizeof value);
    EXPECT_CODE(subetha_list_set(h, a, value, sizeof value), SUBETHA_OK);
    EXPECT_CODE(subetha_list_set(h, SUBETHA_LIST_HEAD_INDEX, value, sizeof value), SUBETHA_E_OUT_OF_BOUNDS);
    CHECK(list_walk_forward(h, walked, 8) == 2);
    CHECK(walked[0][0] == 9 && walked[1][0] == 3);

    /* A second handle sees the same nodes; another layout is refused. */
    EXPECT_CODE(subetha_list_open(path, 8, &layout, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_list_open(path, 8, &other_tag, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_list_open(path, 16, &layout, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    CHECK(list_walk_forward(again, walked, 8) == 2);
    EXPECT_CODE(subetha_list_pop_front(again, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 9);
    EXPECT_CODE(subetha_list_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 1);
    EXPECT_CODE(subetha_list_pop_back(h, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 3);
    EXPECT_CODE(subetha_list_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.len == 0);
    EXPECT_CODE(subetha_list_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A full list refuses the next push: the head takes one slot. */
    EXPECT_CODE(subetha_list_reset(path, 3, &layout, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_list_push_back(h, value, sizeof value, &a), SUBETHA_OK);
    EXPECT_CODE(subetha_list_push_back(h, value, sizeof value, &b), SUBETHA_OK);
    EXPECT_CODE(subetha_list_push_back(h, value, sizeof value, &c), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_list_remove(h, a, out, sizeof out, &len), SUBETHA_OK);
    EXPECT_CODE(subetha_list_push_front(h, value, sizeof value, &c), SUBETHA_OK);
    CHECK(c == a);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_list_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_list_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

/* One handle lookup and one panic guard for a run of operations, over an
 * array the caller already has. A batch stops at the first refusal and
 * says how many it completed. */
static void test_batch_entry_points(const char *scratch_prefix)
{
    /* An anonymous ring of eight slots: a batch of ten fills it and
     * reports eight. */
    subetha_handle ring = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 8, &strict_options, &ring), SUBETHA_OK);
    uint32_t producer = 0, consumer = 0;
    EXPECT_CODE(subetha_ring_register_producer(ring, &producer), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_register_consumer(ring, &consumer), SUBETHA_OK);

    /* The caller's own array, one item per element, indexes inside. */
    struct event {
        uint32_t index;
        uint32_t weight;
    } events[10];
    for (uint32_t i = 0; i < 10; i++) {
        events[i].index = i;
        events[i].weight = i * 3;
    }
    size_t done = 99;
    EXPECT_CODE(subetha_ring_try_push_many(ring, producer, (const uint8_t *)events, sizeof events[0],
                                           sizeof events[0], 0, &done),
                SUBETHA_OK);
    CHECK(done == 0);
    EXPECT_CODE(subetha_ring_try_push_many(ring, producer, NULL, sizeof events[0], sizeof events[0], 4, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_try_push_many(ring, producer, (const uint8_t *)events, 4, sizeof events[0], 4, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_try_push_many(ring, producer, (const uint8_t *)events, sizeof events[0],
                                           sizeof events[0], 4, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_try_push_many(ring, producer, (const uint8_t *)events, sizeof events[0],
                                           SUBETHA_RING_SLOT_BYTES + 1, 4, &done),
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    EXPECT_CODE(subetha_ring_try_push_many(ring, producer, (const uint8_t *)events, sizeof events[0],
                                           sizeof events[0], 10, &done),
                SUBETHA_OK);
    CHECK(done == 8);

    /* The pops come back in order, into the caller's array of slots. */
    uint8_t slots[10][SUBETHA_RING_SLOT_BYTES];
    memset(slots, 0, sizeof slots);
    EXPECT_CODE(subetha_ring_try_pop_many(ring, consumer, slots[0], SUBETHA_RING_SLOT_BYTES, 10, &done), SUBETHA_OK);
    CHECK(done == 8);
    for (uint32_t i = 0; i < 8; i++) {
        uint32_t index = 0, weight = 0;
        memcpy(&index, slots[i], sizeof index);
        memcpy(&weight, slots[i] + sizeof index, sizeof weight);
        CHECK(index == i && weight == i * 3);
    }
    EXPECT_CODE(subetha_ring_try_pop_many(ring, consumer, slots[0], SUBETHA_RING_SLOT_BYTES, 4, &done),
                SUBETHA_E_RING_EMPTY);
    CHECK(done == 0);
    EXPECT_CODE(subetha_ring_try_pop_many(ring, consumer, slots[0], 8, 4, &done), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_handle_destroy(ring), SUBETHA_OK);

    /* The SPSC ring, the stack and the deque take the same shape. */
    subetha_handle spsc = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_spsc_create_anon(8, SUBETHA_MODE_STRICT, &spsc), SUBETHA_OK);
    EXPECT_CODE(subetha_spsc_try_push_many(spsc, (const uint8_t *)events, sizeof events[0], sizeof events[0], 10, &done),
                SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_spsc_try_pop_many(spsc, slots[0], SUBETHA_RING_SLOT_BYTES, 10, &done), SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_handle_destroy(spsc), SUBETHA_OK);

    char path[1024];
    snprintf(path, sizeof path, "%s-batch-stack.bin", scratch_prefix);
    subetha_handle stack = SUBETHA_HANDLE_NONE;
    const subetha_element_layout eight = {8, 4, 0x4241544348ULL};
    EXPECT_CODE(subetha_stack_create(path, 8, &eight, &strict_options, &stack), SUBETHA_OK);
    EXPECT_CODE(subetha_stack_try_push_many(stack, (const uint8_t *)events, sizeof events[0], 8, 10, &done), SUBETHA_OK);
    CHECK(done == 8);
    uint8_t taken[10][8];
    EXPECT_CODE(subetha_stack_try_pop_many(stack, taken[0], 8, 10, &done), SUBETHA_OK);
    CHECK(done == 8);
    /* A stack hands them back in reverse. */
    for (uint32_t i = 0; i < 8; i++) {
        uint32_t index = 0;
        memcpy(&index, taken[i], sizeof index);
        CHECK(index == 7 - i);
    }
    EXPECT_CODE(subetha_handle_destroy(stack), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_stack_unlink(path, &report), SUBETHA_OK);

    snprintf(path, sizeof path, "%s-batch-deque.bin", scratch_prefix);
    subetha_handle deque = SUBETHA_HANDLE_NONE, thief = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_deque_create(path, 8, &eight, &strict_options, &deque), SUBETHA_OK);
    EXPECT_CODE(subetha_deque_open_thief(path, &eight, &strict_options, &thief), SUBETHA_OK);
    EXPECT_CODE(subetha_deque_try_push_many(deque, (const uint8_t *)events, sizeof events[0], 8, 8, &done), SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_deque_try_push_many(thief, (const uint8_t *)events, sizeof events[0], 8, 1, &done),
                SUBETHA_E_DEQUE_NOT_OWNER);
    CHECK(done == 0);
    EXPECT_CODE(subetha_deque_try_steal_many(thief, taken[0], 8, 10, &done), SUBETHA_OK);
    CHECK(done == 8);
    /* A thief takes from the top, oldest first. */
    for (uint32_t i = 0; i < 8; i++) {
        uint32_t index = 0;
        memcpy(&index, taken[i], sizeof index);
        CHECK(index == i);
    }
    EXPECT_CODE(subetha_handle_destroy(thief), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(deque), SUBETHA_OK);
    EXPECT_CODE(subetha_deque_unlink(path, &report), SUBETHA_OK);

    /* The MPSC pool: one producer's batch, drained by the consumer's. */
    subetha_handle producers[2] = {SUBETHA_HANDLE_NONE, SUBETHA_HANDLE_NONE};
    subetha_handle pool_consumer = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_mpsc_create_anon_pool(2, 8, SUBETHA_MODE_STRICT, producers, &pool_consumer), SUBETHA_OK);
    EXPECT_CODE(subetha_mpsc_try_push_many(producers[0], (const uint8_t *)events, sizeof events[0], sizeof events[0], 10,
                                           &done),
                SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_mpsc_try_pop_many(pool_consumer, slots[0], SUBETHA_RING_SLOT_BYTES, 10, &done), SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_mpsc_try_pop_many(pool_consumer, slots[0], SUBETHA_RING_SLOT_BYTES, 4, &done),
                SUBETHA_E_RING_EMPTY);
    CHECK(done == 0);
    EXPECT_CODE(subetha_handle_destroy(pool_consumer), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(producers[0]), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(producers[1]), SUBETHA_OK);

    /* The MPMC grid: what one producer pushed, one consumer's subset takes. */
    subetha_handle grid_producers[2] = {SUBETHA_HANDLE_NONE, SUBETHA_HANDLE_NONE};
    subetha_handle grid_consumers[1] = {SUBETHA_HANDLE_NONE};
    EXPECT_CODE(subetha_mpmc_create_anon_grid(2, 1, 8, SUBETHA_MODE_STRICT, grid_producers, grid_consumers), SUBETHA_OK);
    EXPECT_CODE(subetha_mpmc_try_push_many(grid_producers[0], (const uint8_t *)events, sizeof events[0],
                                           sizeof events[0], 10, &done),
                SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_mpmc_try_pop_many(grid_consumers[0], slots[0], SUBETHA_RING_SLOT_BYTES, 10, &done), SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_handle_destroy(grid_consumers[0]), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(grid_producers[0]), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(grid_producers[1]), SUBETHA_OK);

    /* The Vyukov ring, where a slot carries the payload rather than the
     * whole line. */
    subetha_handle vyukov = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_vyukov_create_anon(8, &strict_options, &vyukov), SUBETHA_OK);
    EXPECT_CODE(subetha_vyukov_try_push_many(vyukov, (const uint8_t *)events, sizeof events[0], sizeof events[0], 10,
                                             &done),
                SUBETHA_OK);
    CHECK(done == 8);
    uint8_t payloads[10][SUBETHA_RING_PAYLOAD_MAX];
    EXPECT_CODE(subetha_vyukov_try_pop_many(vyukov, payloads[0], SUBETHA_RING_PAYLOAD_MAX, 10, &done), SUBETHA_OK);
    CHECK(done == 8);
    for (uint32_t i = 0; i < 8; i++) {
        uint32_t index = 0;
        memcpy(&index, payloads[i], sizeof index);
        CHECK(index == i);
    }
    EXPECT_CODE(subetha_handle_destroy(vyukov), SUBETHA_OK);

    /* The Lamport pair. */
    subetha_handle lamport_producer = SUBETHA_HANDLE_NONE, lamport_consumer = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_lamport_create_anon_pair(8, SUBETHA_MODE_STRICT, &lamport_producer, &lamport_consumer),
                SUBETHA_OK);
    EXPECT_CODE(subetha_lamport_try_push_many(lamport_producer, (const uint8_t *)events, sizeof events[0],
                                              sizeof events[0], 10, &done),
                SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_lamport_try_pop_many(lamport_consumer, slots[0], SUBETHA_RING_SLOT_BYTES, 10, &done),
                SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_handle_destroy(lamport_consumer), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(lamport_producer), SUBETHA_OK);

    /* The broadcast ring: both consumers see the whole batch. */
    subetha_handle broadcast = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_broadcast_create_anon(8, &strict_options, &broadcast), SUBETHA_OK);
    uint32_t seat_a = 99, seat_b = 99;
    EXPECT_CODE(subetha_broadcast_register_consumer(broadcast, &seat_a), SUBETHA_OK);
    EXPECT_CODE(subetha_broadcast_register_consumer(broadcast, &seat_b), SUBETHA_OK);
    EXPECT_CODE(subetha_broadcast_try_push_many(broadcast, (const uint8_t *)events, sizeof events[0], sizeof events[0],
                                                8, &done),
                SUBETHA_OK);
    CHECK(done == 8);
    uint8_t heard[8][SUBETHA_BROADCAST_PAYLOAD_BYTES];
    EXPECT_CODE(subetha_broadcast_try_recv_many(broadcast, seat_a, heard[0], SUBETHA_BROADCAST_PAYLOAD_BYTES, 8, &done),
                SUBETHA_OK);
    CHECK(done == 8);
    for (uint32_t i = 0; i < 8; i++) {
        uint32_t index = 0;
        memcpy(&index, heard[i], sizeof index);
        CHECK(index == i);
    }
    EXPECT_CODE(subetha_broadcast_try_recv_many(broadcast, seat_b, heard[0], SUBETHA_BROADCAST_PAYLOAD_BYTES, 8, &done),
                SUBETHA_OK);
    CHECK(done == 8);
    EXPECT_CODE(subetha_broadcast_try_recv_many(broadcast, seat_b, heard[0], SUBETHA_BROADCAST_PAYLOAD_BYTES, 4, &done),
                SUBETHA_E_RING_EMPTY);
    CHECK(done == 0);
    EXPECT_CODE(subetha_handle_destroy(broadcast), SUBETHA_OK);
}

/* A four-byte key in the order the map sorts by: big-endian, so the bytes
 * rise with the number. */
static void btree_key(uint8_t *key, uint32_t n)
{
    key[0] = (uint8_t)(n >> 24);
    key[1] = (uint8_t)(n >> 16);
    key[2] = (uint8_t)(n >> 8);
    key[3] = (uint8_t)n;
}

static void test_shared_btree(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-btree.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    const uint64_t tag = 0x4254524545ULL;
    EXPECT_CODE(subetha_btree_create(path, 0, 4, 8, tag, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_btree_create(path, 64, 0, 8, tag, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_btree_open(path, 64, 4, 8, tag, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_btree_create(path, 64, 4, 8, tag, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_BTREE);

    uint8_t key[4], out[8], previous[8];
    size_t len = 0, previous_len = 0;
    bool replaced = true, present = true;
    btree_key(key, 7);
    EXPECT_CODE(subetha_btree_get(h, key, sizeof key, out, sizeof out, &len), SUBETHA_E_MAP_KEY_ABSENT);
    EXPECT_CODE(subetha_btree_remove(h, key, sizeof key, out, sizeof out, &len), SUBETHA_E_MAP_KEY_ABSENT);
    EXPECT_CODE(subetha_btree_contains(h, key, sizeof key, &present), SUBETHA_OK);
    CHECK(!present);
    EXPECT_CODE(subetha_btree_insert(h, key, 3, (const uint8_t *)"12345678", 8, previous, sizeof previous, &previous_len,
                                     &replaced),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_btree_insert(h, key, sizeof key, (const uint8_t *)"1234567", 7, previous, sizeof previous,
                                     &previous_len, &replaced),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_btree_insert(h, key, sizeof key, (const uint8_t *)"aaaaaaaa", 8, previous, sizeof previous,
                                     &previous_len, &replaced),
                SUBETHA_OK);
    CHECK(!replaced && previous_len == 0);
    EXPECT_CODE(subetha_btree_insert(h, key, sizeof key, (const uint8_t *)"bbbbbbbb", 8, previous, sizeof previous,
                                     &previous_len, &replaced),
                SUBETHA_OK);
    CHECK(replaced && previous_len == 8 && memcmp(previous, "aaaaaaaa", 8) == 0);
    EXPECT_CODE(subetha_btree_get(h, key, sizeof key, out, 7, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_btree_get(h, key, sizeof key, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 8 && memcmp(out, "bbbbbbbb", 8) == 0);
    EXPECT_CODE(subetha_btree_contains(h, key, sizeof key, &present), SUBETHA_OK);
    CHECK(present);

    /* Enough keys to split the root, inserted out of order. Key 7 is
     * already in the tree, so the run leaves 200 entries, not 201. */
    for (uint32_t n = 200; n > 0; n--) {
        uint8_t k[4], v[8];
        btree_key(k, n);
        memset(v, 0, sizeof v);
        v[0] = (uint8_t)n;
        EXPECT_CODE(subetha_btree_insert(h, k, sizeof k, v, sizeof v, previous, sizeof previous, &previous_len, &replaced),
                    SUBETHA_OK);
    }
    subetha_btree_stats stats;
    EXPECT_CODE(subetha_btree_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 200 && stats.capacity == 64 && stats.key_size == 4 && stats.value_size == 8);
    CHECK(stats.tag == tag && stats.node_count > 1 && stats.node_stride > 0);
    for (uint32_t n = 1; n <= 200; n++) {
        uint8_t k[4];
        btree_key(k, n);
        EXPECT_CODE(subetha_btree_get(h, k, sizeof k, out, sizeof out, &len), SUBETHA_OK);
        CHECK(out[0] == (uint8_t)n);
    }

    /* The ends of the map are its smallest and largest keys, in byte
     * order, which for big-endian keys is numeric order. */
    uint8_t first_key[4], last_key[4];
    size_t first_len = 0, last_len = 0;
    EXPECT_CODE(subetha_btree_first(h, first_key, sizeof first_key, &first_len, out, sizeof out, &len), SUBETHA_OK);
    CHECK(first_len == 4 && len == 8);
    uint8_t want[4];
    btree_key(want, 1);
    CHECK(memcmp(first_key, want, 4) == 0);
    EXPECT_CODE(subetha_btree_last(h, last_key, sizeof last_key, &last_len, out, sizeof out, &len), SUBETHA_OK);
    btree_key(want, 200);
    CHECK(memcmp(last_key, want, 4) == 0);

    /* A removal takes the entry and leaves the rest findable. */
    btree_key(key, 100);
    EXPECT_CODE(subetha_btree_remove(h, key, sizeof key, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 8 && out[0] == 100);
    EXPECT_CODE(subetha_btree_get(h, key, sizeof key, out, sizeof out, &len), SUBETHA_E_MAP_KEY_ABSENT);
    EXPECT_CODE(subetha_btree_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.len == 199);

    /* A second handle sees the same tree; another shape is refused. */
    EXPECT_CODE(subetha_btree_open(path, 64, 4, 8, tag, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_btree_open(path, 64, 4, 8, tag + 1, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_btree_open(path, 64, 8, 8, tag, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_btree_open(path, 32, 4, 8, tag, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    btree_key(key, 55);
    EXPECT_CODE(subetha_btree_get(again, key, sizeof key, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 55);
    EXPECT_CODE(subetha_btree_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);

    /* A clear empties it for both handles. */
    EXPECT_CODE(subetha_btree_clear(h), SUBETHA_OK);
    EXPECT_CODE(subetha_btree_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.len == 0);
    EXPECT_CODE(subetha_btree_first(again, first_key, sizeof first_key, &first_len, out, sizeof out, &len),
                SUBETHA_E_MAP_KEY_ABSENT);
    CHECK(first_len == 0);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A map with one node runs out when the root fills and cannot split. */
    EXPECT_CODE(subetha_btree_reset(path, 1, 4, 4, tag, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    for (uint32_t n = 0; n < SUBETHA_BTREE_KEYS_PER_NODE; n++) {
        uint8_t k[4], v[4] = {0, 0, 0, 0};
        btree_key(k, n);
        EXPECT_CODE(subetha_btree_insert(h, k, sizeof k, v, sizeof v, previous, sizeof previous, &previous_len, NULL),
                    SUBETHA_OK);
    }
    uint8_t over[4], zero[4] = {0, 0, 0, 0};
    btree_key(over, SUBETHA_BTREE_KEYS_PER_NODE);
    EXPECT_CODE(subetha_btree_insert(h, over, sizeof over, zero, sizeof zero, previous, sizeof previous, &previous_len,
                                     NULL),
                SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_btree_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_btree_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_shared_cell(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-cell.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_cell_create(path, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_cell_create(path, SUBETHA_CELL_PAYLOAD_BYTES + 1, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_cell_open(path, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_cell_create(path, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_CELL);

    /* A fresh cell reads as zeros at version zero. */
    uint8_t out[8];
    size_t len = 0;
    uint32_t version = 99;
    EXPECT_CODE(subetha_cell_get(h, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 8 && out[0] == 0 && out[7] == 0);
    EXPECT_CODE(subetha_cell_version(h, &version), SUBETHA_OK);
    CHECK(version == 0);

    /* A write is one version step of two, and the value comes back. */
    EXPECT_CODE(subetha_cell_set(h, (const uint8_t *)"abcdefg", 7), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_cell_set(h, (const uint8_t *)"abcdefgh", 8), SUBETHA_OK);
    EXPECT_CODE(subetha_cell_version(h, &version), SUBETHA_OK);
    CHECK(version == 2);
    EXPECT_CODE(subetha_cell_get(h, out, 7, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_cell_get(h, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 8 && memcmp(out, "abcdefgh", 8) == 0);
    EXPECT_CODE(subetha_cell_set(h, (const uint8_t *)"12345678", 8), SUBETHA_OK);
    EXPECT_CODE(subetha_cell_version(h, &version), SUBETHA_OK);
    CHECK(version == 4);
    EXPECT_CODE(subetha_cell_get(h, out, sizeof out, &len), SUBETHA_OK);
    CHECK(memcmp(out, "12345678", 8) == 0);

    /* A second handle of the same size sees it; another size does not. */
    EXPECT_CODE(subetha_cell_open(path, 8, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_cell_open(path, 4, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_cell_open(path, 52, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_cell_get(again, out, sizeof out, &len), SUBETHA_OK);
    CHECK(memcmp(out, "12345678", 8) == 0);
    subetha_cell_stats stats;
    EXPECT_CODE(subetha_cell_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.value_size == 8 && stats.version == 4 && stats.mode == SUBETHA_MODE_STRICT);
    EXPECT_CODE(subetha_cell_set(again, (const uint8_t *)"secondly", 8), SUBETHA_OK);
    EXPECT_CODE(subetha_cell_get(h, out, sizeof out, &len), SUBETHA_OK);
    CHECK(memcmp(out, "secondly", 8) == 0);
    EXPECT_CODE(subetha_cell_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_cell_version(h, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A create attaches rather than resetting; a reset starts over, and at
     * a size of its own. */
    EXPECT_CODE(subetha_cell_create(path, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_cell_get(h, out, sizeof out, &len), SUBETHA_OK);
    CHECK(memcmp(out, "secondly", 8) == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_cell_reset(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_cell_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.value_size == 4 && stats.version == 0);
    EXPECT_CODE(subetha_cell_get(h, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 4 && out[0] == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_cell_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_cell_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_frame_region(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-frames.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_frame_region_create(path, 4, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_frame_region_create(path, 12, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_frame_region_create(path, 64, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_frame_region_create(path, 64, SUBETHA_FRAME_NO_BLOCK, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_frame_region_open(path, 64, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_frame_region_create(path, 64, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_FRAME_REGION);
    subetha_frame_region_stats stats;
    EXPECT_CODE(subetha_frame_region_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.block_size == 64 && stats.block_count == 4 && stats.mode == SUBETHA_MODE_STRICT);
    CHECK(stats.file_size == 192 + 64 * 4);

    /* The bump cursor hands out consecutive blocks; an exhausted region
     * refuses the next one. */
    uint32_t a = SUBETHA_FRAME_NO_BLOCK, b = SUBETHA_FRAME_NO_BLOCK, c = SUBETHA_FRAME_NO_BLOCK,
             d = SUBETHA_FRAME_NO_BLOCK, spare = SUBETHA_FRAME_NO_BLOCK;
    EXPECT_CODE(subetha_frame_region_alloc(h, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_frame_region_alloc(h, &a), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_alloc(h, &b), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_alloc(h, &c), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_alloc(h, &d), SUBETHA_OK);
    CHECK(a == 0 && b == 1 && c == 2 && d == 3);
    EXPECT_CODE(subetha_frame_region_alloc(h, &spare), SUBETHA_E_RING_FULL);

    /* A payload smaller than the block round-trips; one larger than it,
     * and a block past the region, are both refused. */
    uint8_t out[64];
    size_t len = 0;
    EXPECT_CODE(subetha_frame_region_write(h, b, (const uint8_t *)"a frame", 7), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_write(h, c, (const uint8_t *)"held across the attach", 22), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_write(h, b, out, 65), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    EXPECT_CODE(subetha_frame_region_write(h, 4, out, 8), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_frame_region_write(h, SUBETHA_FRAME_NO_BLOCK, out, 8), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_frame_region_read(h, b, 7, out, 6, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_frame_region_read(h, b, 65, out, sizeof out, &len), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
    EXPECT_CODE(subetha_frame_region_read(h, 4, 7, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_frame_region_read(h, b, 7, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 7 && memcmp(out, "a frame", 7) == 0);

    /* A second handle reads what the first wrote; another geometry is
     * refused. */
    EXPECT_CODE(subetha_frame_region_open(path, 64, 4, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_open(path, 128, 4, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_frame_region_open(path, 64, 8, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    memset(out, 0, sizeof out);
    EXPECT_CODE(subetha_frame_region_read(again, b, 7, out, sizeof out, &len), SUBETHA_OK);
    CHECK(memcmp(out, "a frame", 7) == 0);

    /* Either handle may free any block, and the next allocation takes it.
     * The free list runs through the blocks, so the freed one comes back
     * with a link where its first four bytes were. */
    EXPECT_CODE(subetha_frame_region_free(again, 4), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_frame_region_free(again, SUBETHA_FRAME_NO_BLOCK), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_frame_region_free(again, b), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_alloc(h, &spare), SUBETHA_OK);
    CHECK(spare == b);
    EXPECT_CODE(subetha_frame_region_read(h, b, 7, out, sizeof out, &len), SUBETHA_OK);
    CHECK(memcmp(out + 4, "ame", 3) == 0);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_frame_region_read_stats(h, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A create attaches with the blocks in place; a reset hands out the
     * first one again. */
    EXPECT_CODE(subetha_frame_region_create(path, 64, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_alloc(h, &spare), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_frame_region_read(h, c, 22, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 22 && memcmp(out, "held across the attach", 22) == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_reset(path, 64, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_frame_region_alloc(h, &a), SUBETHA_OK);
    CHECK(a == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_frame_region_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_frame_region_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_epoch_table(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-epochs.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_epochs_create(path, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_epochs_open(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_epochs_create(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_EPOCHS);
    subetha_epochs_stats stats;
    EXPECT_CODE(subetha_epochs_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.live_pins == 0 && stats.open_tickets == 0);
    CHECK(stats.mode == SUBETHA_MODE_STRICT);

    /* With nothing open the counter is published and everything is
     * reclaimable. */
    uint64_t now = 0, horizon = 0, stamped = 0;
    EXPECT_CODE(subetha_epochs_advance(h, &stamped), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_now(h, &now), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_reclaim_horizon(h, &horizon), SUBETHA_OK);
    CHECK(now == stamped && horizon == stamped);
    EXPECT_CODE(subetha_epochs_now(h, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* A pin holds the horizon where it was taken, however far the
     * counter runs on, and lets it go on release. */
    uint64_t pin = 0;
    EXPECT_CODE(subetha_epochs_pin(h, &pin), SUBETHA_OK);
    uint64_t pinned = 0;
    EXPECT_CODE(subetha_pin_epoch(h, pin, &pinned), SUBETHA_OK);
    CHECK(pinned == stamped);
    uint64_t later = 0;
    EXPECT_CODE(subetha_epochs_advance(h, &later), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_reclaim_horizon(h, &horizon), SUBETHA_OK);
    CHECK(horizon == pinned && later > pinned);
    EXPECT_CODE(subetha_epochs_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.live_pins == 1 && stats.now == later);

    /* A version superseded after the pin is still visible to it; one
     * superseded at or before it is not, and the live sentinel always
     * is. */
    bool sees = false;
    EXPECT_CODE(subetha_pin_sees(h, pin,SUBETHA_EPOCH_LIVE, &sees), SUBETHA_OK);
    CHECK(sees);
    EXPECT_CODE(subetha_pin_sees(h, pin,later, &sees), SUBETHA_OK);
    CHECK(sees);
    EXPECT_CODE(subetha_pin_sees(h, pin,pinned, &sees), SUBETHA_OK);
    CHECK(!sees);
    EXPECT_CODE(subetha_pin_sees(h, pin, later, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_pin_release(h, pin), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_reclaim_horizon(h, &horizon), SUBETHA_OK);
    CHECK(horizon == later);

    /* Four pins fill the table and the fifth is refused by name: the
     * hold table takes the epoch table's own capacity, so it refuses
     * exactly where the epoch table would have. */
    uint64_t pins[4] = {0, 0, 0, 0};
    for (int i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_epochs_pin(h, &pins[i]), SUBETHA_OK);
    }
    uint64_t overflow = 0;
    EXPECT_CODE(subetha_epochs_pin(h, &overflow), SUBETHA_E_EPOCHS_PINS_EXHAUSTED);
    for (int i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_pin_release(h, pins[i]), SUBETHA_OK);
    }
    uint64_t reaped = 1;
    EXPECT_CODE(subetha_epochs_reap_dead_pins(h, &reaped), SUBETHA_OK);
    CHECK(reaped == 0);

    /* An open ticket holds the published epoch below its own, so a pin
     * taken while it is open sees none of what it stamps. */
    uint64_t ticket = 0;
    EXPECT_CODE(subetha_epochs_begin(h, &ticket), SUBETHA_OK);
    uint64_t reserved = 0;
    EXPECT_CODE(subetha_ticket_epoch(h, ticket, &reserved), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_now(h, &now), SUBETHA_OK);
    CHECK(reserved == later + 1 && now == later);
    EXPECT_CODE(subetha_epochs_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.open_tickets == 1);
    EXPECT_CODE(subetha_epochs_pin(h, &pin), SUBETHA_OK);
    EXPECT_CODE(subetha_pin_epoch(h, pin, &pinned), SUBETHA_OK);
    CHECK(pinned == later);
    EXPECT_CODE(subetha_pin_sees(h, pin, reserved, &sees), SUBETHA_OK);
    CHECK(sees);
    EXPECT_CODE(subetha_pin_release(h, pin), SUBETHA_OK);

    /* Publishing makes the whole compound visible at once, and the
     * token names nothing afterwards. */
    EXPECT_CODE(subetha_ticket_publish(h, ticket), SUBETHA_OK);
    EXPECT_CODE(subetha_ticket_epoch(h, ticket, &reserved), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_epochs_now(h, &now), SUBETHA_OK);
    CHECK(now == later + 1);
    EXPECT_CODE(subetha_epochs_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.open_tickets == 0);

    /* A second ticket publishes the same way; nothing is left open. */
    EXPECT_CODE(subetha_epochs_begin(h, &ticket), SUBETHA_OK);
    EXPECT_CODE(subetha_ticket_epoch(h, ticket, &reserved), SUBETHA_OK);
    EXPECT_CODE(subetha_ticket_publish(h, ticket), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_now(h, &now), SUBETHA_OK);
    CHECK(now == reserved);

    /* This process is alive, so it holds no dead ticket and frees none. */
    uint64_t dead[4];
    size_t dead_count = 99;
    EXPECT_CODE(subetha_epochs_dead_tickets(h, dead, 4, &dead_count), SUBETHA_OK);
    CHECK(dead_count == 0);
    EXPECT_CODE(subetha_epochs_dead_tickets(h, dead, 4, NULL), SUBETHA_E_INVALID_ARGUMENT);
    bool freed = true;
    EXPECT_CODE(subetha_epochs_free_dead_ticket(h, reserved, &freed), SUBETHA_OK);
    CHECK(!freed);

    /* A second handle shares the counter; another capacity is refused. */
    EXPECT_CODE(subetha_epochs_open(path, 4, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_open(path, 8, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    uint64_t from_second = 0;
    EXPECT_CODE(subetha_epochs_advance(again, &from_second), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_now(h, &now), SUBETHA_OK);
    CHECK(from_second == reserved + 1 && now == from_second);

    /* A token belongs to the handle that issued it, not to the table.
     * The two handles share the epoch table's slots - the horizon this
     * one computes moves with the other's pin - but the token itself is
     * the other handle's to give back. */
    EXPECT_CODE(subetha_epochs_pin(again, &pin), SUBETHA_OK);
    EXPECT_CODE(subetha_pin_epoch(again, pin, &pinned), SUBETHA_OK);
    CHECK(pinned == from_second);
    EXPECT_CODE(subetha_epochs_reclaim_horizon(h, &horizon), SUBETHA_OK);
    CHECK(horizon == pinned);
    EXPECT_CODE(subetha_pin_epoch(h, pin, &pinned), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_pin_release(h, pin), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_pin_release(again, pin), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_reclaim_horizon(h, &horizon), SUBETHA_OK);
    CHECK(horizon == from_second);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);

    uint8_t byte = 0;
    EXPECT_CODE(subetha_ring_try_push(h, 0, &byte, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_epochs_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_epochs_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_rwlock(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-rwlock.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_rwlock_open(path, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_rwlock_create(path, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_RWLOCK);
    subetha_rwlock_stats stats;
    EXPECT_CODE(subetha_rwlock_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.readers == 0 && !stats.has_writer && stats.waiting_writers == 0);
    CHECK(stats.mode == SUBETHA_MODE_STRICT && stats.timeouts == 0);

    /* Readers share the lock and a writer waits them out. */
    uint64_t r1 = 0, r2 = 0, w = 0;
    EXPECT_CODE(subetha_rwlock_try_read(h, &r1), SUBETHA_OK);
    uint32_t hold_kind = 99;
    EXPECT_CODE(subetha_rwlock_hold_kind(h, r1, &hold_kind), SUBETHA_OK);
    CHECK(hold_kind == SUBETHA_LOCK_READ);
    EXPECT_CODE(subetha_rwlock_try_read(h, &r2), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.readers == 2);
    EXPECT_CODE(subetha_rwlock_try_write(h, &w), SUBETHA_E_WOULD_BLOCK);

    /* A bounded wait against a held lock gives up and says so. */
    EXPECT_CODE(subetha_rwlock_write(h, 20, &w), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_rwlock_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.timeouts == 1 && stats.waiting_writers == 0);
    EXPECT_CODE(subetha_rwlock_try_read(h, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* Releasing every reader opens it to a writer, which then excludes
     * everyone. */
    EXPECT_CODE(subetha_rwlock_unlock(h, r1), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_unlock(h, r2), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.readers == 0);
    EXPECT_CODE(subetha_rwlock_write(h, 1000, &w), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_hold_kind(h, w, &hold_kind), SUBETHA_OK);
    CHECK(hold_kind == SUBETHA_LOCK_WRITE);
    EXPECT_CODE(subetha_rwlock_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.has_writer && stats.waiting_writers == 0);
    EXPECT_CODE(subetha_rwlock_try_read(h, &r1), SUBETHA_E_WOULD_BLOCK);
    EXPECT_CODE(subetha_rwlock_try_write(h, &r1), SUBETHA_E_WOULD_BLOCK);
    EXPECT_CODE(subetha_rwlock_read(h, 20, &r1), SUBETHA_E_TIMEOUT);

    /* A second handle sees the same state. Destroying the handle that
     * issued a hold gives that hold back rather than leaving the lock
     * held with nothing able to release it - a token belongs to its
     * handle, so nothing else could. */
    EXPECT_CODE(subetha_rwlock_open(path, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.has_writer && stats.timeouts == 0);
    EXPECT_CODE(subetha_rwlock_try_read(again, &r1), SUBETHA_E_WOULD_BLOCK);
    EXPECT_CODE(subetha_rwlock_hold_kind(h, w, &hold_kind), SUBETHA_OK);
    CHECK(hold_kind == SUBETHA_LOCK_WRITE);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_read_stats(again, &stats), SUBETHA_OK);
    CHECK(!stats.has_writer);
    EXPECT_CODE(subetha_rwlock_try_read(again, &r1), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_unlock(again, r1), SUBETHA_OK);

    uint8_t byte = 0;
    EXPECT_CODE(subetha_ring_try_push(again, 0, &byte, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_rwlock_flush(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);

    /* A reset lays out an unheld lock. Every handle goes first, because
     * Windows refuses to truncate a file a mapping still covers. */
    EXPECT_CODE(subetha_rwlock_reset(path, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_read_stats(h, &stats), SUBETHA_OK);
    CHECK(!stats.has_writer && stats.readers == 0);
    EXPECT_CODE(subetha_rwlock_try_write(h, &w), SUBETHA_OK);
    /* A token and a handle are both 64 bits and both carry an index and a
     * generation, so each entry point is given the other's value here. The
     * tag is what makes these refusals rather than a plausible decode
     * against the wrong table. */
    EXPECT_CODE(subetha_handle_destroy(w), SUBETHA_E_INVALID_HANDLE);
    EXPECT_CODE(subetha_rwlock_unlock(h, h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_rwlock_hold_kind(h, h, &hold_kind), SUBETHA_E_INVALID_ARGUMENT);
    /* The hold survived all three refusals and still releases exactly once. */
    EXPECT_CODE(subetha_rwlock_unlock(h, w), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_unlock(h, w), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_rwlock_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_rwlock_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

/* The named releases: what a caller in a hot loop uses instead of
 * destroying the handle, because a destroy runs a process-wide barrier
 * for one handle where these pay one for a batch of them. What has to
 * hold is that they release just as immediately - a caller in another
 * process must be able to take the lock the instant this returns, and
 * must not have to wait for a batch to fill - and that the primitive
 * comes back exactly once however the handle is closed. */
/* The barrier through the ABI, without a second process: what one
 * process can show is that the arguments are checked, that a wait with
 * nobody else coming gives the deadline back rather than blocking, and
 * that the counts it reports are the heartbeat table's. The rendezvous
 * itself is what subetha_ctest_peer_epoch_barrier demonstrates. */
/* The shared value through the ABI within one process: the arguments,
 * the bounds, and that a second handle on the same path attaches to the
 * one region rather than making a second. What one process cannot show -
 * that the holder table spans processes - is
 * subetha_ctest_peer_shared_arc's. */
/* The waker through the ABI within one process: the arguments, the
 * token's guarantees, and a park this process wakes itself. What one
 * process cannot show - that a park here is woken from over there - is
 * subetha_ctest_peer_waker's. */
/* The TCP bridge is behind a cargo feature, and its entry points exist
 * whether or not the library was built with it. Which build this is
 * decides what the test asserts, so it asks rather than assuming: a
 * library without the feature must refuse every call and name the
 * feature, and one with it must carry a ring across a socket. Skipping
 * the check and testing only the built path would let a default build
 * pass while its refusals said anything at all. */
static void test_tcp_bridge(const char *scratch_prefix)
{
    (void)scratch_prefix;
    bool available = true;
    EXPECT_CODE(subetha_transports_available(&available), SUBETHA_OK);
    EXPECT_CODE(subetha_transports_available(NULL), SUBETHA_E_INVALID_ARGUMENT);

    subetha_handle ring = SUBETHA_HANDLE_NONE, bridge = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &ring), SUBETHA_OK);

    if (!available) {
        /* Every entry point is present and every one refuses, so a caller
         * links against this build and learns at run time what it lacks. */
        EXPECT_CODE(subetha_tcp_bridge_server(ring, "127.0.0.1:0", SUBETHA_MODE_STRICT, &bridge),
                    SUBETHA_E_NOT_SUPPORTED);
        EXPECT_CODE(subetha_tcp_bridge_client(ring, "127.0.0.1:9", SUBETHA_MODE_STRICT, &bridge),
                    SUBETHA_E_NOT_SUPPORTED);
        /* The refusal names the feature, so a caller is told what to turn
         * on rather than that the call is unknown. */
        char detail[512];
        size_t needed = subetha_last_error_detail(detail, sizeof detail);
        CHECK(needed > 0 && strstr(detail, "tcp-bridge") != NULL);
        EXPECT_CODE(subetha_handle_destroy(ring), SUBETHA_OK);
        return;
    }

    /* Built with the feature: a server on a port the system picks, a
     * client aimed at it, and the ring carried between them. */
    subetha_handle server = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_tcp_bridge_server(ring, "127.0.0.1:0", SUBETHA_MODE_MANAGED, &server),
                SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(server, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_TCP_BRIDGE);

    uint16_t port = 0;
    EXPECT_CODE(subetha_tcp_bridge_local_port(server, &port), SUBETHA_OK);
    CHECK(port != 0);

    subetha_tcp_bridge_stats stats;
    EXPECT_CODE(subetha_tcp_bridge_read_stats(server, &stats), SUBETHA_OK);
    CHECK(stats.role == SUBETHA_BRIDGE_SERVER && !stats.running && !stats.finished);

    EXPECT_CODE(subetha_handle_destroy(server), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(ring), SUBETHA_OK);
}

/* The blocking bridge rides the same feature as the TCP bridge with a
 * blocking SPSC ring at each end, so the test asks which build this is the
 * same way. Built, it carries items end to end: the source ring is filled
 * first, a server on a port the system picks runs in managed mode so its
 * accept sits on its own thread, a client in strict mode blocks until
 * every item has shipped, and the sink ring is read back on this thread
 * once the server's run reports finished. Constructing halves alone would
 * show nothing about the wire. */
static void test_blocking_tcp_bridge(const char *scratch_prefix)
{
    (void)scratch_prefix;
    bool available = true;
    EXPECT_CODE(subetha_transports_available(&available), SUBETHA_OK);

    subetha_handle source = SUBETHA_HANDLE_NONE, sink = SUBETHA_HANDLE_NONE, bridge = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_spsc_create_anon(64, SUBETHA_MODE_STRICT, &source), SUBETHA_OK);
    EXPECT_CODE(subetha_spsc_create_anon(64, SUBETHA_MODE_STRICT, &sink), SUBETHA_OK);

    if (!available) {
        /* Every entry point is present and every one refuses, naming the
         * feature, so a caller links against this build and learns at run
         * time what it lacks. */
        EXPECT_CODE(subetha_blocking_tcp_bridge_server(sink, "127.0.0.1:0", SUBETHA_MODE_STRICT, &bridge),
                    SUBETHA_E_NOT_SUPPORTED);
        EXPECT_CODE(subetha_blocking_tcp_bridge_client(source, "127.0.0.1:9", SUBETHA_MODE_STRICT, &bridge),
                    SUBETHA_E_NOT_SUPPORTED);
        char detail[512];
        size_t needed = subetha_last_error_detail(detail, sizeof detail);
        CHECK(needed > 0 && strstr(detail, "tcp-bridge") != NULL);
        EXPECT_CODE(subetha_handle_destroy(sink), SUBETHA_OK);
        EXPECT_CODE(subetha_handle_destroy(source), SUBETHA_OK);
        return;
    }

    /* A ring of another kind is refused by name: the halves take a
     * blocking SPSC ring and nothing else. */
    subetha_handle adaptive = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_ring_create_anon(1, 1, 64, &strict_options, &adaptive), SUBETHA_OK);
    EXPECT_CODE(subetha_blocking_tcp_bridge_server(adaptive, "127.0.0.1:0", SUBETHA_MODE_MANAGED, &bridge),
                SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(adaptive), SUBETHA_OK);
    EXPECT_CODE(subetha_blocking_tcp_bridge_server(sink, "not-an-address", SUBETHA_MODE_MANAGED, &bridge),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_blocking_tcp_bridge_server(sink, "127.0.0.1:0", SUBETHA_MODE_MANAGED, NULL),
                SUBETHA_E_INVALID_ARGUMENT);

    subetha_handle server = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_blocking_tcp_bridge_server(sink, "127.0.0.1:0", SUBETHA_MODE_MANAGED, &server), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(server, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_BLOCKING_TCP_BRIDGE);
    uint16_t port = 0;
    EXPECT_CODE(subetha_blocking_tcp_bridge_local_port(server, &port), SUBETHA_OK);
    CHECK(port != 0);
    EXPECT_CODE(subetha_blocking_tcp_bridge_local_port(server, NULL), SUBETHA_E_INVALID_ARGUMENT);
    subetha_blocking_tcp_bridge_stats stats;
    EXPECT_CODE(subetha_blocking_tcp_bridge_read_stats(server, &stats), SUBETHA_OK);
    CHECK(stats.role == SUBETHA_BRIDGE_SERVER && !stats.running && !stats.finished);

    /* The items sit in the source ring before the client runs: the client
     * parks for the first and drains the rest in one batch. */
    enum { BRIDGE_ITEMS = 40 };
    for (uint32_t i = 0; i < BRIDGE_ITEMS; i++) {
        uint8_t slot[SUBETHA_RING_SLOT_BYTES] = {0};
        memcpy(slot, &i, sizeof i);
        EXPECT_CODE(subetha_spsc_try_push(source, slot, sizeof slot), SUBETHA_OK);
    }
    /* Managed: the accept runs on the server's own thread and this
     * returns at once. */
    EXPECT_CODE(subetha_blocking_tcp_bridge_run(server, BRIDGE_ITEMS, 10000), SUBETHA_OK);

    char addr[64];
    snprintf(addr, sizeof addr, "127.0.0.1:%u", (unsigned)port);
    subetha_handle client = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_blocking_tcp_bridge_client(source, addr, SUBETHA_MODE_STRICT, &client), SUBETHA_OK);
    EXPECT_CODE(subetha_blocking_tcp_bridge_local_port(client, &port), SUBETHA_E_INVALID_ARGUMENT);
    /* Strict: this thread carries the transfer and returns when every
     * item has shipped. */
    EXPECT_CODE(subetha_blocking_tcp_bridge_run(client, BRIDGE_ITEMS, 10000), SUBETHA_OK);
    EXPECT_CODE(subetha_blocking_tcp_bridge_read_stats(client, &stats), SUBETHA_OK);
    CHECK(stats.role == SUBETHA_BRIDGE_CLIENT && stats.finished && !stats.running);
    CHECK(stats.items == BRIDGE_ITEMS && stats.last_code == SUBETHA_OK);

    /* The server's run ends once the items have landed in the sink. */
    for (int waited = 0; waited < 10000; waited++) {
        EXPECT_CODE(subetha_blocking_tcp_bridge_read_stats(server, &stats), SUBETHA_OK);
        if (stats.finished) {
            break;
        }
        sleep_us(1000);
    }
    CHECK(stats.finished && stats.items == BRIDGE_ITEMS && stats.last_code == SUBETHA_OK);

    /* Every item, in order, byte for byte, and nothing after them. */
    for (uint32_t i = 0; i < BRIDGE_ITEMS; i++) {
        uint8_t out[SUBETHA_RING_SLOT_BYTES];
        size_t len = 0;
        EXPECT_CODE(subetha_spsc_try_pop(sink, out, sizeof out, &len), SUBETHA_OK);
        uint32_t got = 0;
        memcpy(&got, out, sizeof got);
        CHECK(len == SUBETHA_RING_SLOT_BYTES && got == i);
    }
    uint8_t spare[SUBETHA_RING_SLOT_BYTES];
    size_t spare_len = 0;
    EXPECT_CODE(subetha_spsc_try_pop(sink, spare, sizeof spare, &spare_len), SUBETHA_E_RING_EMPTY);

    EXPECT_CODE(subetha_handle_destroy(client), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(server), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(sink), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(source), SUBETHA_OK);
}

/* Sens-O-Matic through the ABI, plaintext and sealed, both halves in this
 * process over the loopback. Nothing in this family had a C test at all
 * before, so what is asserted here is the whole of what a C caller can
 * rely on: that a sender and a receiver come up, that an item put in one
 * end comes out the other byte for byte, and that the stats describe the
 * link rather than returning zeroes.
 *
 * The sealed half is behind a cargo feature whose entry points exist
 * either way, so the test asks which build this is. A library without it
 * must refuse and name the feature; one with it must carry an item under
 * a certificate the test mints for itself. Testing only the built path
 * would let a default build pass while its refusals said anything.
 *
 * Both halves are strict, so no thread exists that the test did not ask
 * for and a poll that finds nothing has only this thread to blame. */
static void test_sens(const char *scratch_prefix)
{
    (void)scratch_prefix;

    /* The plaintext pair first: it is the path that already shipped. */
    subetha_handle rx = SUBETHA_HANDLE_NONE, tx = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_sens_receiver("127.0.0.1:0", 0, SUBETHA_MODE_STRICT, &rx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_receiver("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_receiver("not-an-address", 1200, SUBETHA_MODE_STRICT, &rx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_receiver("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT, &rx), SUBETHA_OK);

    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(rx, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_SENS);

    uint16_t port = 0;
    EXPECT_CODE(subetha_sens_local_port(rx, &port), SUBETHA_OK);
    CHECK(port != 0);

    char peer[64];
    snprintf(peer, sizeof peer, "127.0.0.1:%u", (unsigned)port);
    EXPECT_CODE(subetha_sens_sender("127.0.0.1:0", peer, 1200, SUBETHA_MODE_STRICT, &tx),
                SUBETHA_OK);

    /* A half only does its own job: the receiver refuses to send and the
     * sender refuses to deliver, rather than answering emptily. */
    EXPECT_CODE(subetha_sens_send(rx, (const uint8_t *)"x", 1), SUBETHA_E_INVALID_ARGUMENT);
    uint8_t got[2048];
    size_t got_len = 0;
    EXPECT_CODE(subetha_sens_poll(tx, got, sizeof got, &got_len), SUBETHA_E_INVALID_ARGUMENT);

    static const char payload[] = "sens through the C ABI, in the clear";
    EXPECT_CODE(subetha_sens_send(tx, (const uint8_t *)payload, sizeof payload - 1), SUBETHA_OK);

    /* Strict mode makes the poll the pump, so the loop is the test: an
     * item is not late, it is undelivered until a poll drives the
     * decoder. The bound is generous because loopback under a loaded gate
     * host is still a network. */
    int delivered = 0;
    for (int i = 0; i < 2000 && !delivered; i++) {
        int32_t rc = subetha_sens_poll(rx, got, sizeof got, &got_len);
        if (rc == SUBETHA_OK) {
            delivered = 1;
            break;
        }
        CHECK(rc == SUBETHA_E_RING_EMPTY);
        sleep_us(1000);
    }
    CHECK(delivered);
    CHECK(got_len == sizeof payload - 1);
    CHECK(memcmp(got, payload, got_len) == 0);

    subetha_sens_stats stats;
    EXPECT_CODE(subetha_sens_read_stats(tx, &stats), SUBETHA_OK);
    CHECK(stats.role == SUBETHA_SENS_SENDER && stats.mode == SUBETHA_MODE_STRICT);
    CHECK(stats.items == 1);

    /* A sender knows both counts, so it can say what went missing. One
     * item over the loopback loses nothing, and the count only advances a
     * feedback window at a time, so the number is zero and it is real. */
    CHECK(stats.missed_report == SUBETHA_DROPS_EXACT);
    CHECK(stats.missed == 0);

    /* What the kernel says it dropped is whatever this host can say, and
     * the count is only a count when the report says so. Every state is
     * accepted here because the three gate hosts genuinely differ; what
     * is asserted is that the pair is coherent, never that this host is
     * the one the test was written on. */
    CHECK(stats.kernel_dropped_report == SUBETHA_DROPS_EXACT
          || stats.kernel_dropped_report == SUBETHA_DROPS_OCCURRED
          || stats.kernel_dropped_report == SUBETHA_DROPS_UNKNOWN);
    if (stats.kernel_dropped_report != SUBETHA_DROPS_EXACT) {
        /* A host that cannot count leaves the number alone rather than
         * inventing one, so it must not carry a figure a caller could
         * read as measured. */
        CHECK(stats.kernel_dropped == 0);
    }

    EXPECT_CODE(subetha_sens_read_stats(rx, &stats), SUBETHA_OK);
    CHECK(stats.role == SUBETHA_SENS_RECEIVER && stats.items >= 1);
    /* A receiver never learns what was sent, so it says so instead of
     * reporting a zero that would read as nothing lost. */
    CHECK(stats.missed_report == SUBETHA_DROPS_UNKNOWN);
    CHECK(stats.missed == 0);
    /* Nothing reached this receiver that it could not place or open, and
     * an unsealed pair never handshakes. */
    CHECK(stats.unroutable == 0 && stats.preauth_dropped == 0);
    CHECK(stats.unopened == 0 && stats.handshake_failures == 0);
    EXPECT_CODE(subetha_sens_read_stats(rx, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* Finishing is the sender's: it waits for the far end's
     * acknowledgment of everything sent. A receiver has nothing to
     * finish, and a deadline is required. */
    bool acked = false;
    EXPECT_CODE(subetha_sens_finish(rx, 100, &acked), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_finish(tx, -1, &acked), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_finish(tx, 100, NULL), SUBETHA_E_INVALID_ARGUMENT);
    /* The one item was delivered above, so its acknowledgment is either
     * already here or arrives within the deadline while this pumps. */
    EXPECT_CODE(subetha_sens_finish(tx, 3000, &acked), SUBETHA_OK);
    CHECK(acked);

    EXPECT_CODE(subetha_handle_destroy(tx), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(rx), SUBETHA_OK);

    /* Now the sealed pair. */
    bool sealed = true;
    EXPECT_CODE(subetha_sens_tls_available(&sealed), SUBETHA_OK);
    EXPECT_CODE(subetha_sens_tls_available(NULL), SUBETHA_E_INVALID_ARGUMENT);

    uint8_t cert[4096], key[4096];
    size_t cert_len = 0, key_len = 0;

    if (!sealed) {
        /* Every entry point is present and every one refuses, so a caller
         * links against this build and learns at run time what it lacks. */
        EXPECT_CODE(subetha_sens_self_signed_cert(NULL, cert, sizeof cert, &cert_len,
                                                  key, sizeof key, &key_len),
                    SUBETHA_E_NOT_SUPPORTED);
        char detail[512];
        size_t needed = subetha_last_error_detail(detail, sizeof detail);
        CHECK(needed > 0 && strstr(detail, "tls") != NULL);
        EXPECT_CODE(subetha_sens_sender_tls("127.0.0.1:0", "127.0.0.1:9", 1200,
                                            SUBETHA_MODE_STRICT, cert, 1, NULL, &tx),
                    SUBETHA_E_NOT_SUPPORTED);
        EXPECT_CODE(subetha_sens_receiver_tls("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT,
                                              cert, 1, key, 1, SUBETHA_SENS_CODE_RLC, 1, &rx),
                    SUBETHA_E_NOT_SUPPORTED);
        return;
    }

    /* A buffer too small is told the size it needs and copies nothing, so
     * a caller who cannot know the size in advance asks twice.
     *
     * The two sizes are not compared across calls: every call mints a
     * fresh certificate, and two certificates need not encode to the same
     * number of bytes. What has to hold is that a refusal names a size a
     * later call can use, which is asserted by using it. */
    size_t probe_cert = 0, probe_key = 0;
    uint8_t witness = 0xA5;
    EXPECT_CODE(subetha_sens_self_signed_cert(NULL, &witness, 1, &probe_cert, NULL, 0, &probe_key),
                SUBETHA_E_BUFFER_TOO_SMALL);
    CHECK(probe_cert > 1 && probe_key > 0);
    CHECK(probe_cert <= sizeof cert && probe_key <= sizeof key);
    CHECK(witness == 0xA5); /* refused, so nothing was written */

    EXPECT_CODE(subetha_sens_self_signed_cert(NULL, cert, sizeof cert, &cert_len,
                                              key, sizeof key, &key_len),
                SUBETHA_OK);
    CHECK(cert_len > 0 && cert_len <= sizeof cert);
    CHECK(key_len > 0 && key_len <= sizeof key);

    /* The listener refuses what the transport itself refuses: no peers to
     * serve, and no automatic code under several of them. */
    EXPECT_CODE(subetha_sens_receiver_tls("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT,
                                          cert, cert_len, key, key_len,
                                          SUBETHA_SENS_CODE_RLC, 0, &rx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_receiver_tls("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT,
                                          cert, cert_len, key, key_len, 99, 1, &rx),
                SUBETHA_E_INVALID_ARGUMENT);
    /* An empty certificate is named as such rather than reaching the
     * crypto layer as a parse error nobody can act on. */
    EXPECT_CODE(subetha_sens_receiver_tls("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT,
                                          cert, 0, key, key_len,
                                          SUBETHA_SENS_CODE_RLC, 1, &rx),
                SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_sens_receiver_tls("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT,
                                          cert, cert_len, key, key_len,
                                          SUBETHA_SENS_CODE_RLC, 1, &rx),
                SUBETHA_OK);
    port = 0;
    EXPECT_CODE(subetha_sens_local_port(rx, &port), SUBETHA_OK);
    CHECK(port != 0);
    snprintf(peer, sizeof peer, "127.0.0.1:%u", (unsigned)port);

    /* The handshake runs inside this call, so a trust root the peer does
     * not chain to is refused here rather than as a later silence. The
     * key is not a certificate, which is what makes it the wrong root. */
    EXPECT_CODE(subetha_sens_sender_tls("127.0.0.1:0", peer, 1200, SUBETHA_MODE_STRICT,
                                        key, key_len, NULL, &tx),
                SUBETHA_E_RING_IO);

    EXPECT_CODE(subetha_sens_sender_tls("127.0.0.1:0", peer, 1200, SUBETHA_MODE_STRICT,
                                        cert, cert_len, NULL, &tx),
                SUBETHA_OK);

    static const char secret[] = "sens through the C ABI, sealed";
    EXPECT_CODE(subetha_sens_send(tx, (const uint8_t *)secret, sizeof secret - 1), SUBETHA_OK);

    delivered = 0;
    for (int i = 0; i < 4000 && !delivered; i++) {
        int32_t rc = subetha_sens_poll(rx, got, sizeof got, &got_len);
        if (rc == SUBETHA_OK) {
            delivered = 1;
            break;
        }
        CHECK(rc == SUBETHA_E_RING_EMPTY);
        sleep_us(1000);
    }
    CHECK(delivered);
    CHECK(got_len == sizeof secret - 1);
    CHECK(memcmp(got, secret, got_len) == 0);

    /* The pinned code is the one in force, and nothing switched under a
     * policy that forbids switching. */
    EXPECT_CODE(subetha_sens_read_stats(rx, &stats), SUBETHA_OK);
    CHECK(stats.code == SUBETHA_SENS_CODE_RLC);
    CHECK(stats.switches == 0);

    EXPECT_CODE(subetha_handle_destroy(tx), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(rx), SUBETHA_OK);
}

/* Drive one half pair until an item arrives, or say it never did. Strict
 * mode makes the poll the pump, so this loop is what moves the stream:
 * an item is not late, it is undelivered until a poll runs. */
static int carry_one_item(subetha_handle tx, subetha_handle rx,
                          const char *payload, size_t payload_len, int budget_ms)
{
    uint8_t got[2048];
    size_t got_len = 0;
    if (subetha_sens_send(tx, (const uint8_t *)payload, payload_len) != SUBETHA_OK) {
        return 0;
    }
    /* A block code holds items until it has a full block, so one item on
     * its own has not left the sender and no amount of polling will move
     * it. Flushing is what a caller sending less than a block owes. */
    if (subetha_sens_flush(tx) != SUBETHA_OK) {
        return 0;
    }
    for (int i = 0; i < budget_ms; i++) {
        int32_t rc = subetha_sens_poll(rx, got, sizeof got, &got_len);
        if (rc == SUBETHA_OK) {
            return got_len == payload_len && memcmp(got, payload, got_len) == 0;
        }
        if (rc != SUBETHA_E_RING_EMPTY) {
            return 0;
        }
        sleep_us(1000);
    }
    return 0;
}

/* Each erasure code on its own, with no switching. These are the same
 * family as the unified endpoint above and share its send, poll, port and
 * stats calls, so what is asserted here is what pinning a code changes:
 * the parameters the caller may now name, and that the code in force is
 * the one asked for and never moves.
 *
 * Both codes are exercised end to end rather than merely constructed. A
 * constructor that binds a socket and a transport that carries an item
 * are different claims, and only the second is worth a C caller's trust. */
static void test_sens_standalone_codes(const char *scratch_prefix)
{
    (void)scratch_prefix;
    subetha_handle rx = SUBETHA_HANDLE_NONE, tx = SUBETHA_HANDLE_NONE;
    subetha_sens_stats stats;
    uint16_t port = 0;
    char peer[64];

    /* The sliding-window code alone. */
    EXPECT_CODE(subetha_sens_rlc_receiver("127.0.0.1:0", 0, SUBETHA_MODE_STRICT, &rx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_rlc_receiver("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_rlc_receiver("127.0.0.1:0", 1200, SUBETHA_MODE_STRICT, &rx),
                SUBETHA_OK);
    EXPECT_CODE(subetha_sens_local_port(rx, &port), SUBETHA_OK);
    CHECK(port != 0);
    snprintf(peer, sizeof peer, "127.0.0.1:%u", (unsigned)port);

    /* A window of no symbols and a repair every no symbols are each
     * refused by name rather than reaching the encoder. */
    EXPECT_CODE(subetha_sens_rlc_sender("127.0.0.1:0", peer, 1200, 0, 2, 15,
                                        SUBETHA_MODE_STRICT, &tx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_rlc_sender("127.0.0.1:0", peer, 1200, 16, 0, 15,
                                        SUBETHA_MODE_STRICT, &tx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_rlc_sender("127.0.0.1:0", peer, 0, 16, 2, 15,
                                        SUBETHA_MODE_STRICT, &tx),
                SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_sens_rlc_sender("127.0.0.1:0", peer, 1200, 16, 2, 15,
                                        SUBETHA_MODE_STRICT, &tx),
                SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(tx, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_SENS); /* one family, six constructors */

    CHECK(carry_one_item(tx, rx, "the sliding window, on its own", 30, 4000));

    /* The code asked for is the code in force, and a pinned code never
     * moves however the link behaves. */
    EXPECT_CODE(subetha_sens_read_stats(tx, &stats), SUBETHA_OK);
    CHECK(stats.code == SUBETHA_SENS_CODE_RLC && stats.switches == 0);
    CHECK(stats.role == SUBETHA_SENS_SENDER && stats.items == 1);
    EXPECT_CODE(subetha_sens_read_stats(rx, &stats), SUBETHA_OK);
    CHECK(stats.code == SUBETHA_SENS_CODE_RLC && stats.switches == 0);
    CHECK(stats.role == SUBETHA_SENS_RECEIVER && stats.items >= 1);

    /* A half does only its own job, whichever code carries it. */
    EXPECT_CODE(subetha_sens_send(rx, (const uint8_t *)"x", 1), SUBETHA_E_INVALID_ARGUMENT);
    uint8_t spare[64];
    size_t spare_len = 0;
    EXPECT_CODE(subetha_sens_poll(tx, spare, sizeof spare, &spare_len),
                SUBETHA_E_INVALID_ARGUMENT);

    /* Flushing a code that never holds anything back succeeds having done
     * nothing, so a caller that flushes correctly stays correct whichever
     * code it was handed. A receiving half holds nothing back and says so. */
    EXPECT_CODE(subetha_sens_flush(tx), SUBETHA_OK);
    EXPECT_CODE(subetha_sens_flush(rx), SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_handle_destroy(tx), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(rx), SUBETHA_OK);

    /* The block code alone. Its receiver takes no geometry: k and r ride
     * the wire, so it decodes whatever a sender chose. */
    EXPECT_CODE(subetha_sens_rs_receiver("127.0.0.1:0", SUBETHA_MODE_STRICT, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_rs_receiver("not-an-address", SUBETHA_MODE_STRICT, &rx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_rs_receiver("127.0.0.1:0", SUBETHA_MODE_STRICT, &rx), SUBETHA_OK);
    port = 0;
    EXPECT_CODE(subetha_sens_local_port(rx, &port), SUBETHA_OK);
    CHECK(port != 0);
    snprintf(peer, sizeof peer, "127.0.0.1:%u", (unsigned)port);

    /* A block with no data shards carries nothing and one with no parity
     * corrects nothing; both are refused by name. */
    EXPECT_CODE(subetha_sens_rs_sender("127.0.0.1:0", peer, 0, 2, 1200,
                                       SUBETHA_MODE_STRICT, &tx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_rs_sender("127.0.0.1:0", peer, 8, 0, 1200,
                                       SUBETHA_MODE_STRICT, &tx),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_sens_rs_sender("127.0.0.1:0", peer, 8, 2, 0,
                                       SUBETHA_MODE_STRICT, &tx),
                SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_sens_rs_sender("127.0.0.1:0", peer, 8, 2, 1200,
                                       SUBETHA_MODE_STRICT, &tx),
                SUBETHA_OK);

    CHECK(carry_one_item(tx, rx, "the block code, on its own", 26, 4000));

    EXPECT_CODE(subetha_sens_read_stats(rx, &stats), SUBETHA_OK);
    CHECK(stats.code == SUBETHA_SENS_CODE_RS && stats.switches == 0);
    CHECK(stats.role == SUBETHA_SENS_RECEIVER && stats.items >= 1);

    EXPECT_CODE(subetha_handle_destroy(tx), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(rx), SUBETHA_OK);
}

/* The virtual endpoint registry: a name bound to a ring here or a host
 * elsewhere, rebound without the caller relearning where its bytes go.
 *
 * The pin is what this test is really about. A Rust caller holds a
 * PinnedEndpoint whose lifetime the compiler checks; a C caller holds a
 * generation and has to ask. So what is asserted is that the generation
 * moves when it should, that a reading taken before a rebind is reported
 * stale afterwards, and that an unbound id reads as a state rather than
 * an error. */
static void test_endpoint_registry(const char *scratch_prefix)
{
    subetha_handle reg = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_endpoint_registry_create(SUBETHA_MODE_STRICT, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_endpoint_registry_create(42, &reg), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_endpoint_registry_create(SUBETHA_MODE_STRICT, &reg), SUBETHA_OK);

    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(reg, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_ENDPOINT_REGISTRY);

    uint64_t count = 99;
    EXPECT_CODE(subetha_endpoint_count(reg, &count), SUBETHA_OK);
    CHECK(count == 0);

    /* An id nobody bound is a state, not a failure: a caller acts on
     * "nothing here" and that answer can go stale like any other. */
    subetha_endpoint_target target;
    EXPECT_CODE(subetha_endpoint_read(reg, 7, &target), SUBETHA_OK);
    CHECK(target.kind == SUBETHA_ENDPOINT_NONE);
    uint64_t empty_generation = target.generation;

    /* Unbinding what was never bound is distinguishable from unbinding
     * something, which a caller reconciling state needs. */
    EXPECT_CODE(subetha_endpoint_unbind(reg, 7), SUBETHA_E_MAP_KEY_ABSENT);

    /* A remote binding needs no transport compiled in: it records where
     * bytes would go, and carrying them is the bridge's job. */
    EXPECT_CODE(subetha_endpoint_bind_remote(reg, 7, "not-an-address", "peer.example"),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_endpoint_bind_remote(reg, 7, "127.0.0.1:9099", ""),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_endpoint_bind_remote(reg, 7, "127.0.0.1:9099", "peer.example"),
                SUBETHA_OK);

    EXPECT_CODE(subetha_endpoint_count(reg, &count), SUBETHA_OK);
    CHECK(count == 1);

    EXPECT_CODE(subetha_endpoint_read(reg, 7, &target), SUBETHA_OK);
    CHECK(target.kind == SUBETHA_ENDPOINT_REMOTE);
    CHECK(target.generation != empty_generation);
    uint64_t remote_generation = target.generation;

    /* The reading taken before the bind is stale; the one taken after is
     * not. This pair is the whole of what replaces the borrow checker
     * here, so both directions are asserted. */
    bool valid = true;
    EXPECT_CODE(subetha_endpoint_still_valid(reg, empty_generation, &valid), SUBETHA_OK);
    CHECK(!valid);
    EXPECT_CODE(subetha_endpoint_still_valid(reg, remote_generation, &valid), SUBETHA_OK);
    CHECK(valid);
    EXPECT_CODE(subetha_endpoint_still_valid(reg, remote_generation, NULL),
                SUBETHA_E_INVALID_ARGUMENT);

    /* What was recorded comes back byte for byte. A short buffer is told
     * the size it needs and copies nothing. */
    char addr[64], name[64];
    size_t addr_len = 0, name_len = 0;
    EXPECT_CODE(subetha_endpoint_read_remote(reg, 7, (uint8_t *)addr, sizeof addr, &addr_len,
                                             (uint8_t *)name, sizeof name, &name_len),
                SUBETHA_OK);
    CHECK(addr_len == strlen("127.0.0.1:9099"));
    CHECK(memcmp(addr, "127.0.0.1:9099", addr_len) == 0);
    CHECK(name_len == strlen("peer.example"));
    CHECK(memcmp(name, "peer.example", name_len) == 0);

    char witness = 0x5A;
    size_t needed = 0;
    EXPECT_CODE(subetha_endpoint_read_remote(reg, 7, (uint8_t *)&witness, 1, &needed,
                                             (uint8_t *)name, sizeof name, &name_len),
                SUBETHA_E_BUFFER_TOO_SMALL);
    CHECK(needed == strlen("127.0.0.1:9099"));
    CHECK(witness == 0x5A);

    /* A local ring under the same id: the rebind replaces the target and
     * moves the generation, so a holder of the remote reading learns its
     * bytes now go somewhere else. */
    char base[1024];
    snprintf(base, sizeof base, "%s-endpoint", scratch_prefix);
    subetha_handle ring = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_locale_ring_create(base, 1, 1, 64, &strict_options, &ring), SUBETHA_OK);
    EXPECT_CODE(subetha_endpoint_bind_local(reg, 7, ring), SUBETHA_OK);

    EXPECT_CODE(subetha_endpoint_read(reg, 7, &target), SUBETHA_OK);
    CHECK(target.kind == SUBETHA_ENDPOINT_LOCAL);
    EXPECT_CODE(subetha_endpoint_still_valid(reg, remote_generation, &valid), SUBETHA_OK);
    CHECK(!valid);

    /* Rebinding replaced rather than added. */
    EXPECT_CODE(subetha_endpoint_count(reg, &count), SUBETHA_OK);
    CHECK(count == 1);

    /* Asking a local binding for its remote details is the wrong kind,
     * not an empty answer. */
    EXPECT_CODE(subetha_endpoint_read_remote(reg, 7, (uint8_t *)addr, sizeof addr, &addr_len,
                                             (uint8_t *)name, sizeof name, &name_len),
                SUBETHA_E_WRONG_KIND);

    /* A handle of another kind is refused wherever a registry is wanted. */
    EXPECT_CODE(subetha_endpoint_count(ring, &count), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_endpoint_bind_local(reg, 8, reg), SUBETHA_E_WRONG_KIND);

    EXPECT_CODE(subetha_endpoint_unbind(reg, 7), SUBETHA_OK);
    EXPECT_CODE(subetha_endpoint_count(reg, &count), SUBETHA_OK);
    CHECK(count == 0);

    EXPECT_CODE(subetha_handle_destroy(ring), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(reg), SUBETHA_OK);
}

/* The quality-of-service policy: what a workload needs, changed while
 * traffic runs.
 *
 * The interesting part is what the ABI refuses to offer. There is no call
 * that writes the whole policy at once, because each field is its own
 * atomic and one call would look atomic without being so. What is
 * asserted here is that each setter lands on its own field and leaves the
 * others alone, and that the refusals are refusals rather than values
 * quietly stored. */
static void test_qos_policy(const char *scratch_prefix)
{
    (void)scratch_prefix;
    subetha_handle q = SUBETHA_HANDLE_NONE;

    EXPECT_CODE(subetha_qos_create_preset(SUBETHA_QOS_PRESET_STREAMING, SUBETHA_MODE_STRICT, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_qos_create_preset(99, SUBETHA_MODE_STRICT, &q), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_qos_create_preset(SUBETHA_QOS_PRESET_STREAMING, SUBETHA_MODE_STRICT, &q),
                SUBETHA_OK);

    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(q, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_QOS_POLICY);

    /* The streaming preset is documented as volatile, best-effort, the
     * last 1024 items, 100 ms. A preset nobody can read back is a preset
     * nobody can rely on. */
    subetha_qos read;
    EXPECT_CODE(subetha_qos_read(q, &read), SUBETHA_OK);
    CHECK(read.durability == SUBETHA_QOS_VOLATILE);
    CHECK(read.reliability == SUBETHA_QOS_BEST_EFFORT);
    CHECK(read.history_kind == SUBETHA_QOS_KEEP_LAST);
    CHECK(read.history_depth == 1024);
    CHECK(read.max_latency_nanos == 100ull * 1000 * 1000);
    CHECK(read.ordering == SUBETHA_QOS_PER_PRODUCER);
    EXPECT_CODE(subetha_qos_read(q, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* Durability drives which locale the policy asks for. It is a
     * recommendation: nothing moves until a caller moves it. */
    uint32_t locale = 99;
    EXPECT_CODE(subetha_qos_recommended_locale(q, &locale), SUBETHA_OK);
    CHECK(locale == SUBETHA_LOCALE_ANON);
    EXPECT_CODE(subetha_qos_set_durability(q, SUBETHA_QOS_PERSISTENT), SUBETHA_OK);
    EXPECT_CODE(subetha_qos_recommended_locale(q, &locale), SUBETHA_OK);
    CHECK(locale == SUBETHA_LOCALE_FILE);

    /* One setter moves one field. Everything else is exactly as it was,
     * which is the whole reason there is no write-everything call. */
    subetha_qos after;
    EXPECT_CODE(subetha_qos_read(q, &after), SUBETHA_OK);
    CHECK(after.durability == SUBETHA_QOS_PERSISTENT);
    CHECK(after.reliability == read.reliability);
    CHECK(after.history_kind == read.history_kind);
    CHECK(after.history_depth == read.history_depth);
    CHECK(after.max_latency_nanos == read.max_latency_nanos);
    CHECK(after.ordering == read.ordering);

    /* A value that names nothing is refused, and refusing means the field
     * is unchanged rather than set to something arbitrary. */
    EXPECT_CODE(subetha_qos_set_durability(q, 77), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_qos_set_reliability(q, 77), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_qos_set_ordering(q, 77), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_qos_set_history(q, 77, 8), SUBETHA_E_INVALID_ARGUMENT);
    /* Keeping the last zero items keeps nothing, so it is not a policy. */
    EXPECT_CODE(subetha_qos_set_history(q, SUBETHA_QOS_KEEP_LAST, 0), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_qos_read(q, &read), SUBETHA_OK);
    CHECK(read.durability == SUBETHA_QOS_PERSISTENT);
    CHECK(read.history_kind == SUBETHA_QOS_KEEP_LAST && read.history_depth == 1024);

    /* Keep-all ignores the depth rather than storing it, so a caller
     * cannot come back later and read a number that means nothing. */
    EXPECT_CODE(subetha_qos_set_history(q, SUBETHA_QOS_KEEP_ALL, 4096), SUBETHA_OK);
    EXPECT_CODE(subetha_qos_read(q, &read), SUBETHA_OK);
    CHECK(read.history_kind == SUBETHA_QOS_KEEP_ALL);
    CHECK(read.history_depth == 0);

    EXPECT_CODE(subetha_qos_set_reliability(q, SUBETHA_QOS_RELIABLE), SUBETHA_OK);
    EXPECT_CODE(subetha_qos_set_ordering(q, SUBETHA_QOS_GLOBAL_FIFO), SUBETHA_OK);
    EXPECT_CODE(subetha_qos_set_max_latency(q, 250000), SUBETHA_OK);
    EXPECT_CODE(subetha_qos_read(q, &read), SUBETHA_OK);
    CHECK(read.reliability == SUBETHA_QOS_RELIABLE);
    CHECK(read.ordering == SUBETHA_QOS_GLOBAL_FIFO);
    CHECK(read.max_latency_nanos == 250000);

    /* History drives the capacity the policy asks for, and it is a power
     * of two the ring can actually take. */
    uint64_t capacity = 0;
    EXPECT_CODE(subetha_qos_recommended_capacity(q, &capacity), SUBETHA_OK);
    CHECK(capacity >= 16);
    CHECK((capacity & (capacity - 1)) == 0);

    EXPECT_CODE(subetha_handle_destroy(q), SUBETHA_OK);

    /* Building from every field at once validates before it builds, so a
     * refusal leaves no handle to leak. */
    subetha_qos want = {
        .durability = SUBETHA_QOS_TRANSIENT,
        .reliability = SUBETHA_QOS_RELIABLE,
        .history_kind = SUBETHA_QOS_KEEP_LAST,
        .history_depth = 64,
        .max_latency_nanos = 5000,
        .ordering = SUBETHA_QOS_GLOBAL_FIFO,
    };
    EXPECT_CODE(subetha_qos_create(NULL, SUBETHA_MODE_STRICT, &q), SUBETHA_E_INVALID_ARGUMENT);
    want.ordering = 77;
    EXPECT_CODE(subetha_qos_create(&want, SUBETHA_MODE_STRICT, &q), SUBETHA_E_INVALID_ARGUMENT);
    CHECK(subetha_live_handles() == 0);
    want.ordering = SUBETHA_QOS_GLOBAL_FIFO;

    EXPECT_CODE(subetha_qos_create(&want, SUBETHA_MODE_STRICT, &q), SUBETHA_OK);
    EXPECT_CODE(subetha_qos_read(q, &read), SUBETHA_OK);
    CHECK(read.durability == SUBETHA_QOS_TRANSIENT);
    CHECK(read.reliability == SUBETHA_QOS_RELIABLE);
    CHECK(read.history_kind == SUBETHA_QOS_KEEP_LAST && read.history_depth == 64);
    CHECK(read.max_latency_nanos == 5000);
    CHECK(read.ordering == SUBETHA_QOS_GLOBAL_FIFO);

    /* A handle of another kind is refused wherever a policy is wanted. */
    EXPECT_CODE(subetha_qos_set_reliability(SUBETHA_HANDLE_NONE, SUBETHA_QOS_RELIABLE),
                SUBETHA_E_INVALID_HANDLE);

    EXPECT_CODE(subetha_handle_destroy(q), SUBETHA_OK);
}

/* The shared bit vector: an array of bits several processes set, clear
 * and count without a lock.
 *
 * The previous value each write hands back is the part worth testing,
 * because it is what makes the vector usable as a claim rather than just
 * as storage: whoever is told the bit changed is the one that won it. */
static void test_bit_vec(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-bitvec.bin", scratch_prefix);
    subetha_handle b = SUBETHA_HANDLE_NONE;

    /* A vector of no bits holds nothing, and the layer below asserts on
     * it, so the boundary refuses it as an argument rather than letting
     * it arrive as a caught panic. */
    EXPECT_CODE(subetha_bit_vec_create(path, 0, SUBETHA_MODE_STRICT, &b),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_bit_vec_create(path, 256, SUBETHA_MODE_STRICT, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_bit_vec_create(NULL, 256, SUBETHA_MODE_STRICT, &b),
                SUBETHA_E_INVALID_ARGUMENT);
    CHECK(subetha_live_handles() == 0);

    EXPECT_CODE(subetha_bit_vec_reset(path, 256, SUBETHA_MODE_STRICT, &b), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(b, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_BIT_VEC);

    subetha_bit_vec_stats stats;
    EXPECT_CODE(subetha_bit_vec_read_stats(b, &stats), SUBETHA_OK);
    CHECK(stats.capacity_bits == 256);
    CHECK(stats.ones == 0 && stats.zeros == 256);
    /* Derived from one count rather than two, so the pair always sums to
     * the capacity even while another writer runs. */
    CHECK(stats.ones + stats.zeros == stats.capacity_bits);

    /* Setting a clear bit reports it was clear; setting it again reports
     * it was already set. That difference is the claim. */
    bool was = true;
    EXPECT_CODE(subetha_bit_vec_set(b, 7, &was), SUBETHA_OK);
    CHECK(!was);
    was = false;
    EXPECT_CODE(subetha_bit_vec_set(b, 7, &was), SUBETHA_OK);
    CHECK(was);

    bool v = false;
    EXPECT_CODE(subetha_bit_vec_get(b, 7, &v), SUBETHA_OK);
    CHECK(v);
    EXPECT_CODE(subetha_bit_vec_get(b, 8, &v), SUBETHA_OK);
    CHECK(!v);
    EXPECT_CODE(subetha_bit_vec_get(b, 7, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* The previous-value pointer is optional: a caller who only wants the
     * bit set should not have to supply somewhere to put an answer it
     * will not read. */
    EXPECT_CODE(subetha_bit_vec_set(b, 9, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_get(b, 9, &v), SUBETHA_OK);
    CHECK(v);

    was = false;
    EXPECT_CODE(subetha_bit_vec_clear(b, 7, &was), SUBETHA_OK);
    CHECK(was);
    EXPECT_CODE(subetha_bit_vec_get(b, 7, &v), SUBETHA_OK);
    CHECK(!v);

    /* Toggle differs from its two siblings and the parameter name says
     * so: set and clear report the value from before the call, toggle
     * reports the value after it. Bit 7 is clear here, so the flip makes
     * it set and that is what comes back. */
    bool now = false;
    EXPECT_CODE(subetha_bit_vec_toggle(b, 7, &now), SUBETHA_OK);
    CHECK(now);
    EXPECT_CODE(subetha_bit_vec_get(b, 7, &v), SUBETHA_OK);
    CHECK(v);
    /* Flipping back reports clear, which is again the value after. */
    EXPECT_CODE(subetha_bit_vec_toggle(b, 7, &now), SUBETHA_OK);
    CHECK(!now);
    EXPECT_CODE(subetha_bit_vec_toggle(b, 7, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_get(b, 7, &v), SUBETHA_OK);
    CHECK(v);

    /* Past the end is out of bounds, not a silent no-op: a caller that
     * miscomputed an index needs to be told. */
    EXPECT_CODE(subetha_bit_vec_set(b, 256, &was), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_bit_vec_get(b, 999, &v), SUBETHA_E_OUT_OF_BOUNDS);

    /* A range ends where it is told, exclusive of the upper bound. */
    EXPECT_CODE(subetha_bit_vec_clear_all(b), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_set_range(b, 16, 24), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_read_stats(b, &stats), SUBETHA_OK);
    CHECK(stats.ones == 8);
    EXPECT_CODE(subetha_bit_vec_get(b, 23, &v), SUBETHA_OK);
    CHECK(v);
    EXPECT_CODE(subetha_bit_vec_get(b, 24, &v), SUBETHA_OK);
    CHECK(!v);

    /* An inverted range is named rather than left to read as empty,
     * which is how a swapped pair of arguments goes unnoticed. */
    EXPECT_CODE(subetha_bit_vec_set_range(b, 24, 16), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_bit_vec_clear_range(b, 24, 16), SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_bit_vec_clear_range(b, 16, 20), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_read_stats(b, &stats), SUBETHA_OK);
    CHECK(stats.ones == 4);

    EXPECT_CODE(subetha_bit_vec_set_all(b), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_read_stats(b, &stats), SUBETHA_OK);
    CHECK(stats.ones == 256 && stats.zeros == 0);
    EXPECT_CODE(subetha_bit_vec_clear_all(b), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_read_stats(b, &stats), SUBETHA_OK);
    CHECK(stats.ones == 0);

    EXPECT_CODE(subetha_bit_vec_flush(b), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_flush_async(b), SUBETHA_OK);

    /* A second handle on the same file sees the same bits: that is the
     * whole point of it being shared, and a create that quietly made a
     * private copy would pass every test above. */
    EXPECT_CODE(subetha_bit_vec_set(b, 100, NULL), SUBETHA_OK);
    subetha_handle b2 = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_bit_vec_open(path, 256, SUBETHA_MODE_STRICT, &b2), SUBETHA_OK);
    EXPECT_CODE(subetha_bit_vec_get(b2, 100, &v), SUBETHA_OK);
    CHECK(v);
    was = false;
    EXPECT_CODE(subetha_bit_vec_clear(b2, 100, &was), SUBETHA_OK);
    CHECK(was);
    EXPECT_CODE(subetha_bit_vec_get(b, 100, &v), SUBETHA_OK);
    CHECK(!v);

    /* A handle of another kind is refused wherever a vector is wanted. */
    EXPECT_CODE(subetha_bit_vec_read_stats(b, NULL), SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_handle_destroy(b2), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(b), SUBETHA_OK);
}

/* The shared HyperLogLog: how many distinct items have been seen.
 *
 * Testing an estimator means testing the properties that must hold
 * rather than an exact number, because the number is allowed to be
 * wrong. What must hold: empty reads zero, re-inserting the same item
 * moves nothing, the count rises with distinct items, and the answer
 * lands within the error the precision buys. A test that demanded an
 * exact count would fail on a correct sketch. */
static void test_hyper_log_log(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-hll.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE;

    /* Precision is bounded, and a value outside it is refused here with
     * the range rather than passed down to fail less helpfully. */
    EXPECT_CODE(subetha_hll_create(path, SUBETHA_HLL_MIN_PRECISION - 1,
                                   SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hll_create(path, SUBETHA_HLL_MAX_PRECISION + 1,
                                   SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hll_create(path, 12, SUBETHA_MODE_STRICT, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    CHECK(subetha_live_handles() == 0);

    EXPECT_CODE(subetha_hll_create(path, 12, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_HLL);

    /* A sketch attached to a file a previous run left behind carries its
     * registers, so the count starts from a known state rather than an
     * assumed one. */
    EXPECT_CODE(subetha_hll_reset(h), SUBETHA_OK);

    uint64_t n = 99;
    EXPECT_CODE(subetha_hll_estimate(h, &n), SUBETHA_OK);
    CHECK(n == 0);
    EXPECT_CODE(subetha_hll_estimate(h, NULL), SUBETHA_E_INVALID_ARGUMENT);

    subetha_hll_stats stats;
    EXPECT_CODE(subetha_hll_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.precision == 12);
    CHECK(stats.n_registers == 4096); /* two to the precision */
    CHECK(stats.estimated_distinct == 0);

    /* One item, then the same item many times: a distinct count must not
     * move for a repeat, which is the whole difference between this and
     * a counter. */
    EXPECT_CODE(subetha_hll_insert(h, (const uint8_t *)"alpha", 5), SUBETHA_OK);
    EXPECT_CODE(subetha_hll_estimate(h, &n), SUBETHA_OK);
    CHECK(n == 1);
    for (int i = 0; i < 50; i++) {
        EXPECT_CODE(subetha_hll_insert(h, (const uint8_t *)"alpha", 5), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_hll_estimate(h, &n), SUBETHA_OK);
    CHECK(n == 1);

    /* Ten thousand distinct items. At precision 12 the standard error is
     * about 1.6%, so a generous 10% band is a bound the sketch must meet
     * on any run while still catching a count that is not working. */
    EXPECT_CODE(subetha_hll_reset(h), SUBETHA_OK);
    char item[32];
    for (int i = 0; i < 10000; i++) {
        int len = snprintf(item, sizeof item, "item-%d", i);
        EXPECT_CODE(subetha_hll_insert(h, (const uint8_t *)item, (size_t)len), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_hll_estimate(h, &n), SUBETHA_OK);
    CHECK(n > 9000 && n < 11000);

    /* An empty item is a value, not an absence: it must be countable. */
    EXPECT_CODE(subetha_hll_reset(h), SUBETHA_OK);
    EXPECT_CODE(subetha_hll_insert(h, (const uint8_t *)"", 0), SUBETHA_OK);
    EXPECT_CODE(subetha_hll_estimate(h, &n), SUBETHA_OK);
    CHECK(n == 1);

    EXPECT_CODE(subetha_hll_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_hll_flush_async(h), SUBETHA_OK);

    /* A second handle on the same file shares the registers, and a reset
     * through one is seen by the other. A create that made a private
     * copy would pass every assertion above. */
    subetha_handle h2 = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_hll_open(path, 12, SUBETHA_MODE_STRICT, &h2), SUBETHA_OK);
    EXPECT_CODE(subetha_hll_estimate(h2, &n), SUBETHA_OK);
    CHECK(n == 1);
    EXPECT_CODE(subetha_hll_insert(h2, (const uint8_t *)"beta", 4), SUBETHA_OK);
    EXPECT_CODE(subetha_hll_estimate(h, &n), SUBETHA_OK);
    CHECK(n == 2);
    EXPECT_CODE(subetha_hll_reset(h2), SUBETHA_OK);
    EXPECT_CODE(subetha_hll_estimate(h, &n), SUBETHA_OK);
    CHECK(n == 0);

    /* Opening at a precision the file was not built with is a layout
     * mismatch rather than a sketch that reads nonsense. */
    subetha_handle h3 = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_hll_open(path, 14, SUBETHA_MODE_STRICT, &h3),
                SUBETHA_E_RING_LAYOUT_MISMATCH);

    EXPECT_CODE(subetha_handle_destroy(h2), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
}

/* NaN-boxed values: one 64-bit word holding a double, a 32-bit integer,
 * a boolean, or nil, told apart by the bit pattern a double reserves for
 * its quiet NaNs.
 *
 * No handle and nothing shared, so what is worth asserting is the
 * discrimination itself: every kind round-trips, every kind refuses to
 * be read as any other, and a refusal leaves the caller's variable
 * alone. The last is the one that matters - a reader that wrote garbage
 * before answering false would be worse than useless. */
static void test_nan_value(const char *scratch_prefix)
{
    (void)scratch_prefix;

    /* These take no handle and need no init, unlike every other family
     * here, so they are exercised before anything is created. */
    uint64_t nil = subetha_nan_nil();
    CHECK(subetha_nan_type(nil) == SUBETHA_NAN_NIL);
    CHECK(subetha_nan_is_nil(nil));

    uint64_t d = subetha_nan_from_f64(3.5);
    uint64_t i = subetha_nan_from_i32(-42);
    uint64_t u = subetha_nan_from_u32(4000000000u);
    uint64_t t = subetha_nan_from_bool(true);
    uint64_t f = subetha_nan_from_bool(false);

    CHECK(subetha_nan_type(d) == SUBETHA_NAN_F64);
    CHECK(subetha_nan_type(i) == SUBETHA_NAN_I32);
    CHECK(subetha_nan_type(u) == SUBETHA_NAN_U32);
    CHECK(subetha_nan_type(t) == SUBETHA_NAN_BOOL);
    CHECK(subetha_nan_type(f) == SUBETHA_NAN_BOOL);
    CHECK(!subetha_nan_is_nil(d));

    /* Every kind comes back as it went in. */
    double out_d = 0.0;
    CHECK(subetha_nan_as_f64(d, &out_d));
    CHECK(out_d == 3.5);
    int32_t out_i = 0;
    CHECK(subetha_nan_as_i32(i, &out_i));
    CHECK(out_i == -42);
    uint32_t out_u = 0;
    CHECK(subetha_nan_as_u32(u, &out_u));
    CHECK(out_u == 4000000000u);
    bool out_b = false;
    CHECK(subetha_nan_as_bool(t, &out_b));
    CHECK(out_b);
    out_b = true;
    CHECK(subetha_nan_as_bool(f, &out_b));
    CHECK(!out_b);

    /* A false answer and a stored false are different things: the answer
     * says whether the word held a boolean at all. */
    out_b = true;
    CHECK(!subetha_nan_as_bool(i, &out_b));
    CHECK(out_b); /* untouched, not overwritten with false */

    /* Reading any kind as any other refuses, and leaves the caller's
     * variable exactly as it was. The sentinels are values the decode
     * would never produce, so an overwrite is visible. */
    out_d = -1.25;
    CHECK(!subetha_nan_as_f64(i, &out_d));
    CHECK(!subetha_nan_as_f64(u, &out_d));
    CHECK(!subetha_nan_as_f64(t, &out_d));
    CHECK(!subetha_nan_as_f64(nil, &out_d));
    CHECK(out_d == -1.25);

    out_i = 12345;
    CHECK(!subetha_nan_as_i32(d, &out_i));
    CHECK(!subetha_nan_as_i32(u, &out_i));
    CHECK(!subetha_nan_as_i32(nil, &out_i));
    CHECK(out_i == 12345);

    out_u = 54321;
    CHECK(!subetha_nan_as_u32(d, &out_u));
    CHECK(!subetha_nan_as_u32(i, &out_u));
    CHECK(!subetha_nan_as_u32(nil, &out_u));
    CHECK(out_u == 54321);

    /* The out pointer is optional: a caller who only wants to know the
     * kind should not have to provide somewhere to put a value. */
    CHECK(subetha_nan_as_f64(d, NULL));
    CHECK(!subetha_nan_as_f64(i, NULL));

    /* Zero and the extremes are values like any other, and are exactly
     * where a packing that loses a bit shows up. */
    CHECK(subetha_nan_as_i32(subetha_nan_from_i32(0), &out_i) && out_i == 0);
    CHECK(subetha_nan_as_i32(subetha_nan_from_i32(INT32_MIN), &out_i));
    CHECK(out_i == INT32_MIN);
    CHECK(subetha_nan_as_i32(subetha_nan_from_i32(INT32_MAX), &out_i));
    CHECK(out_i == INT32_MAX);
    CHECK(subetha_nan_as_u32(subetha_nan_from_u32(0), &out_u) && out_u == 0);
    CHECK(subetha_nan_as_u32(subetha_nan_from_u32(UINT32_MAX), &out_u));
    CHECK(out_u == UINT32_MAX);

    /* Doubles that are awkward to box: zero, a negative, and the two
     * infinities, all of which must stay doubles rather than reading as
     * boxed. */
    const double awkward[] = {0.0, -0.0, -1.0, 1e308, -1e308};
    for (size_t k = 0; k < sizeof awkward / sizeof awkward[0]; k++) {
        uint64_t packed = subetha_nan_from_f64(awkward[k]);
        CHECK(subetha_nan_type(packed) == SUBETHA_NAN_F64);
        CHECK(subetha_nan_as_f64(packed, &out_d));
        CHECK(out_d == awkward[k]);
    }

    /* A NaN survives as a NaN and stays unboxed. It does not survive as
     * the same NaN: every one becomes the canonical quiet NaN, which is
     * what stops its bits looking like a boxed value. */
    uint64_t packed_nan = subetha_nan_from_f64((double)(0.0 / 1.0) * 0.0 + (0.0 / 0.0));
    CHECK(subetha_nan_type(packed_nan) == SUBETHA_NAN_F64);
    CHECK(subetha_nan_as_f64(packed_nan, &out_d));
    CHECK(out_d != out_d); /* still a NaN */

    /* The two-level form: an index and an inner tag in the same word. */
    uint64_t tagged = 0;
    uint32_t out_index = 0, out_tag = 0;
    CHECK(subetha_nan_from_tagged(1000, 5, 3, &tagged));
    CHECK(subetha_nan_is_tagged(tagged));
    CHECK(subetha_nan_as_tagged(tagged, 3, &out_index, &out_tag));
    CHECK(out_index == 1000 && out_tag == 5);

    /* A reader that knows only the one-level form reports RESERVED
     * rather than guessing, which is what subetha_nan_is_tagged exists
     * to answer properly. */
    CHECK(subetha_nan_type(tagged) == SUBETHA_NAN_RESERVED);
    CHECK(!subetha_nan_is_tagged(d));
    CHECK(!subetha_nan_is_tagged(nil));

    /* A component that does not fit is refused, not truncated: a
     * silently narrowed index points somewhere else. One tag bit holds
     * 0 or 1, and three tag bits leave 29 for the index. */
    uint64_t untouched = 0xDEADBEEF;
    CHECK(!subetha_nan_from_tagged(0, 2, 1, &untouched));
    CHECK(untouched == 0xDEADBEEF);
    CHECK(!subetha_nan_from_tagged(1u << 29, 0, 3, &untouched));
    CHECK(untouched == 0xDEADBEEF);
    CHECK(!subetha_nan_from_tagged(0, 0, SUBETHA_NAN_MAX_TAG_BITS + 1, &untouched));
    CHECK(untouched == 0xDEADBEEF);

    /* Reading a value that is not tagged, or at an impossible width,
     * refuses and leaves both outputs alone. */
    out_index = 7; out_tag = 9;
    CHECK(!subetha_nan_as_tagged(d, 3, &out_index, &out_tag));
    CHECK(out_index == 7 && out_tag == 9);
    CHECK(!subetha_nan_as_tagged(tagged, SUBETHA_NAN_MAX_TAG_BITS + 1,
                                 &out_index, &out_tag));
    CHECK(out_index == 7 && out_tag == 9);

    /* The width is not stored in the word, so reading at the wrong one
     * yields a different pair rather than an error. Asserting this is
     * how the documented hazard stays true: a caller must remember the
     * width it wrote. */
    CHECK(subetha_nan_as_tagged(tagged, 4, &out_index, &out_tag));
    CHECK(!(out_index == 1000 && out_tag == 5));

    /* Both edges of the width range work. */
    CHECK(subetha_nan_from_tagged(123, 0, 0, &tagged));
    CHECK(subetha_nan_as_tagged(tagged, 0, &out_index, &out_tag));
    CHECK(out_index == 123 && out_tag == 0);
    CHECK(subetha_nan_from_tagged(1, 1, SUBETHA_NAN_MAX_TAG_BITS, &tagged));
    CHECK(subetha_nan_as_tagged(tagged, SUBETHA_NAN_MAX_TAG_BITS,
                                &out_index, &out_tag));
    CHECK(out_index == 1 && out_tag == 1);

    /* Both out pointers are optional. */
    CHECK(subetha_nan_from_tagged(5, 1, 2, NULL));
    CHECK(subetha_nan_from_tagged(5, 1, 2, &tagged));
    CHECK(subetha_nan_as_tagged(tagged, 2, NULL, &out_tag));
    CHECK(out_tag == 1);
    CHECK(subetha_nan_as_tagged(tagged, 2, &out_index, NULL));
    CHECK(out_index == 5);
}

/* The LRU cache: the two reads are the point. One inspects without
 * reordering, the other promotes what it reads, and choosing wrong
 * evicts the wrong entry later rather than corrupting anything - which is
 * why the distinction is in the names. What is asserted is recency
 * order, that a miss leaves the value buffer untouched, and that every
 * refusal is a refusal. */
static void test_lru_cache(const char *scratch_prefix)
{
    char base[1024];
    snprintf(base, sizeof base, "%s-lru", scratch_prefix);
    subetha_handle c = SUBETHA_HANDLE_NONE;

    EXPECT_CODE(subetha_lru_create(base, 3, 8, 8, SUBETHA_MODE_STRICT, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_lru_create(base, 3, 0, 8, SUBETHA_MODE_STRICT, &c), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_lru_create(base, 3, 8, 8, SUBETHA_MODE_STRICT, &c), SUBETHA_OK);

    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(c, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_LRU_CACHE);

    uint64_t k0 = 0, k1 = 1, k2 = 2, k3 = 3;
    uint64_t v = 0;
    bool flag = true;
    for (uint64_t i = 0; i < 3; i++) {
        v = i * 10;
        EXPECT_CODE(subetha_lru_put(c, (uint8_t *)&i, 8, (uint8_t *)&v, 8, &flag), SUBETHA_OK);
        CHECK(!flag); /* new key, nothing replaced */
    }

    /* A wrong-size key is refused rather than padded. */
    uint32_t short_key = 1;
    EXPECT_CODE(subetha_lru_put(c, (uint8_t *)&short_key, 4, (uint8_t *)&v, 8, NULL),
                SUBETHA_E_INVALID_ARGUMENT);

    /* Inspect without promoting: k0 stays the least recent. */
    uint64_t out = 0xFFFFFFFFFFFFFFFFull;
    bool found = false;
    EXPECT_CODE(subetha_lru_get(c, (uint8_t *)&k0, 8, (uint8_t *)&out, 8, &found), SUBETHA_OK);
    CHECK(found && out == 0);

    /* A miss leaves the buffer as it was. */
    out = 0xABCDABCDABCDABCDull;
    EXPECT_CODE(subetha_lru_get(c, (uint8_t *)&k3, 8, (uint8_t *)&out, 8, &found), SUBETHA_OK);
    CHECK(!found);
    CHECK(out == 0xABCDABCDABCDABCDull);

    /* Promote k0, then insert a fourth: k1 is now the oldest and goes. */
    EXPECT_CODE(subetha_lru_touch(c, (uint8_t *)&k0, 8, &flag), SUBETHA_OK);
    CHECK(flag);
    v = 30;
    EXPECT_CODE(subetha_lru_put(c, (uint8_t *)&k3, 8, (uint8_t *)&v, 8, NULL), SUBETHA_OK);
    bool present = true;
    EXPECT_CODE(subetha_lru_contains(c, (uint8_t *)&k1, 8, &present), SUBETHA_OK);
    CHECK(!present); /* evicted */
    EXPECT_CODE(subetha_lru_contains(c, (uint8_t *)&k0, 8, &present), SUBETHA_OK);
    CHECK(present); /* touched, so kept */
    (void)k2;

    /* Replace reports the previous presence. */
    v = 99;
    EXPECT_CODE(subetha_lru_put(c, (uint8_t *)&k3, 8, (uint8_t *)&v, 8, &flag), SUBETHA_OK);
    CHECK(flag);

    /* Evict the oldest by hand and read both halves back. */
    uint64_t ek = 0, ev = 0;
    bool evicted = false;
    EXPECT_CODE(subetha_lru_evict_oldest(c, (uint8_t *)&ek, 8, (uint8_t *)&ev, 8, &evicted), SUBETHA_OK);
    CHECK(evicted);

    subetha_lru_stats st;
    EXPECT_CODE(subetha_lru_read_stats(c, &st), SUBETHA_OK);
    CHECK(st.capacity == 3 && st.key_size == 8 && st.value_size == 8);
    CHECK(st.len == 2);

    /* A second handle on the same files sees the same entries. */
    subetha_handle other = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_lru_open(base, 3, 8, 8, SUBETHA_MODE_STRICT, &other), SUBETHA_OK);
    EXPECT_CODE(subetha_lru_contains(other, (uint8_t *)&k3, 8, &present), SUBETHA_OK);
    CHECK(present);
    EXPECT_CODE(subetha_handle_destroy(other), SUBETHA_OK);

    EXPECT_CODE(subetha_lru_flush(c), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(c), SUBETHA_OK);
}

/* The directed graph: edges are walked, not collected. What is asserted
 * is that a walk from one node touches only that node's edges, that an
 * edge to an unallocated node is refused, and that removing an edge from
 * a node that does not own it is refused - each of which keeps the
 * structure sound rather than merely reporting a problem. */
static void test_graph(const char *scratch_prefix)
{
    char base[1024];
    snprintf(base, sizeof base, "%s-graph", scratch_prefix);
    subetha_handle g = SUBETHA_HANDLE_NONE;

    EXPECT_CODE(subetha_graph_create(base, 8, 8, 0, 4, SUBETHA_MODE_STRICT, &g), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_graph_create(base, 8, 8, 8, 4, SUBETHA_MODE_STRICT, &g), SUBETHA_OK);

    uint32_t a = 0, b = 0, c = 0;
    uint64_t nv = 0;
    EXPECT_CODE(subetha_graph_add_node(g, (uint8_t *)&nv, 8, &a), SUBETHA_OK);
    nv = 1;
    EXPECT_CODE(subetha_graph_add_node(g, (uint8_t *)&nv, 8, &b), SUBETHA_OK);
    nv = 2;
    EXPECT_CODE(subetha_graph_add_node(g, (uint8_t *)&nv, 8, &c), SUBETHA_OK);

    uint32_t ev = 10, e_ab = 0, e_ac = 0, e_bc = 0;
    EXPECT_CODE(subetha_graph_add_edge(g, a, b, (uint8_t *)&ev, 4, &e_ab), SUBETHA_OK);
    ev = 11;
    EXPECT_CODE(subetha_graph_add_edge(g, a, c, (uint8_t *)&ev, 4, &e_ac), SUBETHA_OK);
    ev = 12;
    EXPECT_CODE(subetha_graph_add_edge(g, b, c, (uint8_t *)&ev, 4, &e_bc), SUBETHA_OK);

    /* A dangling edge is indistinguishable from a live one afterwards,
     * so an unallocated target is refused. */
    EXPECT_CODE(subetha_graph_add_edge(g, a, 999, (uint8_t *)&ev, 4, &e_bc), SUBETHA_E_INVALID_ARGUMENT);

    uint32_t degree = 0;
    EXPECT_CODE(subetha_graph_out_degree(g, a, &degree), SUBETHA_OK);
    CHECK(degree == 2);
    EXPECT_CODE(subetha_graph_out_degree(g, c, &degree), SUBETHA_OK);
    CHECK(degree == 0);

    /* Walk a's chain: exactly two edges, and both point at b or c. */
    uint32_t e = 0, walked = 0, dst = 0;
    EXPECT_CODE(subetha_graph_first_edge(g, a, &e), SUBETHA_OK);
    while (e != SUBETHA_GRAPH_NIL) {
        uint32_t val = 0;
        EXPECT_CODE(subetha_graph_edge_target(g, e, (uint8_t *)&val, 4, &dst), SUBETHA_OK);
        CHECK(dst == b || dst == c);
        CHECK(val == 10 || val == 11);
        walked++;
        EXPECT_CODE(subetha_graph_next_edge(g, e, &e), SUBETHA_OK);
    }
    CHECK(walked == 2);

    /* b's chain holds only its own edge, not a's. */
    EXPECT_CODE(subetha_graph_first_edge(g, b, &e), SUBETHA_OK);
    CHECK(e == e_bc);
    EXPECT_CODE(subetha_graph_next_edge(g, e, &e), SUBETHA_OK);
    CHECK(e == SUBETHA_GRAPH_NIL);

    /* Freeing an edge from the wrong node would leave the owning chain
     * pointing at a free slot, so it is refused. */
    EXPECT_CODE(subetha_graph_remove_edge(g, b, e_ab), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_graph_remove_edge(g, a, e_ab), SUBETHA_OK);
    EXPECT_CODE(subetha_graph_out_degree(g, a, &degree), SUBETHA_OK);
    CHECK(degree == 1);

    uint64_t back = 0;
    EXPECT_CODE(subetha_graph_node_value(g, b, (uint8_t *)&back, 8), SUBETHA_OK);
    CHECK(back == 1);

    subetha_graph_stats st;
    EXPECT_CODE(subetha_graph_read_stats(g, &st), SUBETHA_OK);
    CHECK(st.node_count == 3 && st.edge_count == 2);
    CHECK(st.node_value_size == 8 && st.edge_value_size == 4);

    EXPECT_CODE(subetha_graph_flush(g), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(g), SUBETHA_OK);
}

/* The time-point tile: a snapshot sees lanes at or below its version.
 * Asserted directly: version 0 is refused, the boundary is inclusive, a
 * removed lane is invisible even to a later snapshot, and a reused lane
 * does not leak its predecessor's version. */
static void test_time_point(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-tile.bin", scratch_prefix);
    subetha_handle t = SUBETHA_HANDLE_NONE;

    EXPECT_CODE(subetha_tile_create(path, SUBETHA_TILE_MAX_PAYLOAD + 1, SUBETHA_MODE_STRICT, &t),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_tile_create(path, 8, SUBETHA_MODE_STRICT, &t), SUBETHA_OK);

    uint64_t v = 1;
    uint32_t early = 0, late = 0;
    EXPECT_CODE(subetha_tile_insert(t, 0, (uint8_t *)&v, 8, &early), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_tile_insert(t, 10, (uint8_t *)&v, 8, &early), SUBETHA_OK);
    v = 2;
    EXPECT_CODE(subetha_tile_insert(t, 20, (uint8_t *)&v, 8, &late), SUBETHA_OK);

    uint32_t mask = 0, count = 0;
    EXPECT_CODE(subetha_tile_visible_mask(t, 5, &mask), SUBETHA_OK);
    CHECK(mask == 0);
    EXPECT_CODE(subetha_tile_visible_mask(t, 10, &mask), SUBETHA_OK);
    CHECK((mask & (1u << early)) != 0); /* inclusive boundary */
    CHECK((mask & (1u << late)) == 0);
    EXPECT_CODE(subetha_tile_visible_count(t, 20, &count), SUBETHA_OK);
    CHECK(count == 2);

    uint64_t out = 0, version = 0;
    bool present = false;
    EXPECT_CODE(subetha_tile_at(t, late, (uint8_t *)&out, 8, &version, &present), SUBETHA_OK);
    CHECK(present && out == 2 && version == 20);

    /* Removed: invisible even to a snapshot far in the future, and its
     * lane, when reused at a lower version, is not seen early. */
    EXPECT_CODE(subetha_tile_remove(t, late), SUBETHA_OK);
    EXPECT_CODE(subetha_tile_visible_count(t, 1000, &count), SUBETHA_OK);
    CHECK(count == 1);
    uint32_t again = 0;
    EXPECT_CODE(subetha_tile_insert(t, 15, (uint8_t *)&v, 8, &again), SUBETHA_OK);
    CHECK(again == late);
    EXPECT_CODE(subetha_tile_visible_count(t, 14, &count), SUBETHA_OK);
    CHECK(count == 1); /* the reused lane is not visible before 15 */
    EXPECT_CODE(subetha_tile_visible_count(t, 15, &count), SUBETHA_OK);
    CHECK(count == 2);

    subetha_tile_stats st;
    EXPECT_CODE(subetha_tile_read_stats(t, &st), SUBETHA_OK);
    CHECK(st.capacity == SUBETHA_TILE_LANES && st.len == 2 && st.payload_size == 8);

    EXPECT_CODE(subetha_tile_flush(t), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(t), SUBETHA_OK);
}

/* The content-prefix pointer: sixteen bytes the caller holds, with no
 * handle of its own. A prefix mismatch rules equality out with no
 * dereference; a match means resolve and compare. Asserted with two
 * distinct values that share a prefix, so the weaker contract is tested
 * rather than assumed. */
static void test_umbra_pointer(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-umbra-region.bin", scratch_prefix);
    const subetha_element_layout layout = {8, 8, 77};
    subetha_handle r = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_region_create(path, 16, &layout, SUBETHA_MODE_STRICT, &r), SUBETHA_OK);

    uint8_t pa[SUBETHA_UMBRA_BYTES], pb[SUBETHA_UMBRA_BYTES], pc[SUBETHA_UMBRA_BYTES];
    uint64_t va = 0x00000000AAAAAAAAull;      /* low word AAAAAAAA */
    uint64_t vb = 0xFFFFFFFFAAAAAAAAull;      /* same low word, different value */
    uint64_t vc = 0x0000000000000001ull;

    EXPECT_CODE(subetha_umbra_allocate(r, (uint8_t *)&va, 8, pa), SUBETHA_OK);
    EXPECT_CODE(subetha_umbra_allocate(r, (uint8_t *)&vb, 8, pb), SUBETHA_OK);
    EXPECT_CODE(subetha_umbra_allocate(r, (uint8_t *)&vc, 8, pc), SUBETHA_OK);
    EXPECT_CODE(subetha_umbra_allocate(r, (uint8_t *)&vc, 4, pc), SUBETHA_E_INVALID_ARGUMENT);

    uint32_t prefix = 0;
    EXPECT_CODE(subetha_umbra_prefix(pa, &prefix), SUBETHA_OK);
    CHECK(prefix == 0xAAAAAAAAu); /* the first four stored bytes, little-endian */

    bool eq = false;
    EXPECT_CODE(subetha_umbra_prefix_eq(pa, pc, &eq), SUBETHA_OK);
    CHECK(!eq); /* certain: they differ */
    EXPECT_CODE(subetha_umbra_prefix_eq(pa, pb, &eq), SUBETHA_OK);
    CHECK(eq);  /* maybe: resolve to find out */

    uint64_t ra = 0, rb = 0;
    EXPECT_CODE(subetha_umbra_resolve(r, pa, (uint8_t *)&ra, 8), SUBETHA_OK);
    EXPECT_CODE(subetha_umbra_resolve(r, pb, (uint8_t *)&rb, 8), SUBETHA_OK);
    CHECK(ra == va && rb == vb && ra != rb); /* the deref settles it */

    /* A caller-chosen prefix discriminates where content would not. */
    uint8_t pd[SUBETHA_UMBRA_BYTES];
    EXPECT_CODE(subetha_umbra_allocate_with_prefix(r, (uint8_t *)&vb, 8, 7, pd), SUBETHA_OK);
    EXPECT_CODE(subetha_umbra_prefix_eq(pb, pd, &eq), SUBETHA_OK);
    CHECK(!eq);
    bool matches = false;
    EXPECT_CODE(subetha_umbra_matches_prefix(pd, 7, &matches), SUBETHA_OK);
    CHECK(matches);

    /* A query prefix computed without storing anything. */
    EXPECT_CODE(subetha_umbra_content_prefix((uint8_t *)&va, 8, &prefix), SUBETHA_OK);
    CHECK(prefix == 0xAAAAAAAAu);

    /* Nil: says so rather than reading slot zero, which is occupied. */
    uint8_t pn[SUBETHA_UMBRA_BYTES];
    bool nil = false;
    EXPECT_CODE(subetha_umbra_nil(pn), SUBETHA_OK);
    EXPECT_CODE(subetha_umbra_is_nil(pn, &nil), SUBETHA_OK);
    CHECK(nil);
    ra = 12345;
    EXPECT_CODE(subetha_umbra_resolve(r, pn, (uint8_t *)&ra, 8), SUBETHA_E_INVALID_ARGUMENT);
    CHECK(ra == 12345); /* untouched */

    /* Extension: seven bytes fit, an eighth is refused and changes nothing. */
    uint8_t payload[7] = {1, 2, 3, 4, 5, 6, 7};
    EXPECT_CODE(subetha_umbra_set_ext(pa, 9, payload, 7), SUBETHA_OK);
    uint8_t eight[8] = {0};
    EXPECT_CODE(subetha_umbra_set_ext(pa, 9, eight, 8), SUBETHA_E_INVALID_ARGUMENT);
    uint8_t tag = 0, got[SUBETHA_UMBRA_EXT_BYTES] = {0};
    EXPECT_CODE(subetha_umbra_get_ext(pa, &tag, got), SUBETHA_OK);
    CHECK(tag == 9 && got[0] == 1 && got[6] == 7);
    EXPECT_CODE(subetha_umbra_clear_ext(pa), SUBETHA_OK);
    EXPECT_CODE(subetha_umbra_get_ext(pa, &tag, got), SUBETHA_OK);
    CHECK(tag == SUBETHA_UMBRA_EXT_NONE && got[0] == 0);
    /* And the extension did not disturb what the pointer aims at. */
    EXPECT_CODE(subetha_umbra_resolve(r, pa, (uint8_t *)&ra, 8), SUBETHA_OK);
    CHECK(ra == va);

    EXPECT_CODE(subetha_handle_destroy(r), SUBETHA_OK);
}

/* The strategy-switching set: nothing migrates on its own, a migration
 * carries every element across, and the stamp moves on a migration and
 * on nothing else - including a migration to the strategy already in
 * force, which must not burn a version. */
static void test_universal(const char *scratch_prefix)
{
    char base[1024];
    snprintf(base, sizeof base, "%s-universal", scratch_prefix);
    subetha_handle u = SUBETHA_HANDLE_NONE;

    EXPECT_CODE(subetha_universal_create(base, 16, 8, 99, SUBETHA_MODE_STRICT, &u), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_universal_create(base, 16, 0, SUBETHA_UNIVERSAL_VEC, SUBETHA_MODE_STRICT, &u),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_universal_create(base, 16, 8, SUBETHA_UNIVERSAL_VEC, SUBETHA_MODE_STRICT, &u), SUBETHA_OK);

    bool added = false;
    for (uint64_t i = 0; i < 5; i++) {
        EXPECT_CODE(subetha_universal_insert(u, (uint8_t *)&i, 8, &added), SUBETHA_OK);
        CHECK(added);
    }
    uint64_t dup = 2;
    EXPECT_CODE(subetha_universal_insert(u, (uint8_t *)&dup, 8, &added), SUBETHA_OK);
    CHECK(!added); /* a set holds each value once */

    subetha_universal_stats before;
    EXPECT_CODE(subetha_universal_read_stats(u, &before), SUBETHA_OK);
    CHECK(before.strategy == SUBETHA_UNIVERSAL_VEC && before.len == 5);
    CHECK(before.inserts == 6 && before.version == 0);

    bool present = false;
    EXPECT_CODE(subetha_universal_contains(u, (uint8_t *)&dup, 8, &present), SUBETHA_OK);
    CHECK(present);

    /* Ordinary work leaves the stamp alone. */
    subetha_universal_stats mid;
    EXPECT_CODE(subetha_universal_read_stats(u, &mid), SUBETHA_OK);
    CHECK(mid.stamp == before.stamp && mid.contains == 1);

    /* A second handle, attached before the migration, declares no
     * strategy of its own: the state file beside the backings is what
     * it reads. */
    subetha_handle v = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_universal_open(base, 64, 8, SUBETHA_MODE_STRICT, &v), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_universal_open(base, 16, 4, SUBETHA_MODE_STRICT, &v), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_universal_open(base, 16, 8, SUBETHA_MODE_STRICT, &v), SUBETHA_OK);
    subetha_universal_stats other;
    EXPECT_CODE(subetha_universal_read_stats(v, &other), SUBETHA_OK);
    CHECK(other.strategy == SUBETHA_UNIVERSAL_VEC && other.stamp == before.stamp && other.len == 5);

    /* A migration carries everything and moves the stamp once. */
    EXPECT_CODE(subetha_universal_migrate(u, SUBETHA_UNIVERSAL_MAP), SUBETHA_OK);
    subetha_universal_stats after;
    EXPECT_CODE(subetha_universal_read_stats(u, &after), SUBETHA_OK);
    CHECK(after.strategy == SUBETHA_UNIVERSAL_MAP && after.len == 5);
    CHECK(after.stamp != before.stamp && after.version == 1);
    for (uint64_t i = 0; i < 5; i++) {
        EXPECT_CODE(subetha_universal_contains(u, (uint8_t *)&i, 8, &present), SUBETHA_OK);
        CHECK(present);
    }

    /* The other handle reads the migration it did not make: its stamp
     * moved with the word, every element is where it now looks, and an
     * element added through either handle is found through the other. A
     * handle that kept a belief of its own would go on scanning an
     * emptied vector, which is how a workload found this. */
    EXPECT_CODE(subetha_universal_read_stats(v, &other), SUBETHA_OK);
    CHECK(other.strategy == SUBETHA_UNIVERSAL_MAP && other.stamp == after.stamp && other.len == 5);
    for (uint64_t i = 0; i < 5; i++) {
        EXPECT_CODE(subetha_universal_contains(v, (uint8_t *)&i, 8, &present), SUBETHA_OK);
        CHECK(present);
    }
    uint64_t late = 100;
    EXPECT_CODE(subetha_universal_insert(u, (uint8_t *)&late, 8, &added), SUBETHA_OK);
    CHECK(added);
    EXPECT_CODE(subetha_universal_contains(v, (uint8_t *)&late, 8, &present), SUBETHA_OK);
    CHECK(present);
    uint64_t later = 200;
    EXPECT_CODE(subetha_universal_insert(v, (uint8_t *)&later, 8, &added), SUBETHA_OK);
    CHECK(added);
    EXPECT_CODE(subetha_universal_contains(u, (uint8_t *)&later, 8, &present), SUBETHA_OK);
    CHECK(present);
    EXPECT_CODE(subetha_universal_read_stats(u, &after), SUBETHA_OK);
    CHECK(after.len == 7);
    EXPECT_CODE(subetha_handle_destroy(v), SUBETHA_OK);

    /* Same strategy again: no change, no version bump. */
    EXPECT_CODE(subetha_universal_migrate(u, SUBETHA_UNIVERSAL_MAP), SUBETHA_OK);
    subetha_universal_stats same;
    EXPECT_CODE(subetha_universal_read_stats(u, &same), SUBETHA_OK);
    CHECK(same.stamp == after.stamp && same.version == 1);

    EXPECT_CODE(subetha_universal_migrate(u, 99), SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_universal_clear(u), SUBETHA_OK);
    EXPECT_CODE(subetha_universal_read_stats(u, &same), SUBETHA_OK);
    CHECK(same.len == 0);

    EXPECT_CODE(subetha_universal_flush(u), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(u), SUBETHA_OK);

    /* Obtaining the set again attaches to the strategy in force, not the
     * one this caller would have started it on. */
    EXPECT_CODE(subetha_universal_create(base, 16, 8, SUBETHA_UNIVERSAL_VEC, SUBETHA_MODE_STRICT, &u), SUBETHA_OK);
    EXPECT_CODE(subetha_universal_read_stats(u, &same), SUBETHA_OK);
    CHECK(same.strategy == SUBETHA_UNIVERSAL_MAP && same.version == 1);
    EXPECT_CODE(subetha_handle_destroy(u), SUBETHA_OK);
}

/* The cascade tower: depth comes from how many levels the caller
 * supplies, and a path validates itself. Asserted directly: a value
 * comes back through its own path, a path of the wrong length is refused
 * rather than walked partly, and a nil at any level is refused naming
 * that level. */
static void test_k_tower(const char *scratch_prefix)
{
    char leaf[1024], l0[1024], l1[1024];
    snprintf(leaf, sizeof leaf, "%s-tower-leaf.bin", scratch_prefix);
    snprintf(l0, sizeof l0, "%s-tower-l0.bin", scratch_prefix);
    snprintf(l1, sizeof l1, "%s-tower-l1.bin", scratch_prefix);
    const char *levels[2] = {l0, l1};
    const uint64_t caps[2] = {32, 32};
    subetha_handle t = SUBETHA_HANDLE_NONE;

    EXPECT_CODE(subetha_tower_create(leaf, 32, 0, levels, caps, 2, SUBETHA_MODE_STRICT, &t),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_tower_create(leaf, 32, 8, levels, caps, 2, SUBETHA_MODE_STRICT, &t), SUBETHA_OK);

    subetha_tower_stats st;
    EXPECT_CODE(subetha_tower_read_stats(t, &st), SUBETHA_OK);
    CHECK(st.depth == 3 && st.value_size == 8 && st.len == 0);

    uint32_t path_a[3] = {0}, path_b[3] = {0};
    uint64_t va = 111, vb = 222;
    EXPECT_CODE(subetha_tower_append(t, (uint8_t *)&va, 8, path_a, 3), SUBETHA_OK);
    EXPECT_CODE(subetha_tower_append(t, (uint8_t *)&vb, 8, path_b, 3), SUBETHA_OK);

    uint64_t out = 0;
    EXPECT_CODE(subetha_tower_get(t, path_a, 3, (uint8_t *)&out, 8), SUBETHA_OK);
    CHECK(out == 111);
    EXPECT_CODE(subetha_tower_get(t, path_b, 3, (uint8_t *)&out, 8), SUBETHA_OK);
    CHECK(out == 222);

    /* A path of the wrong length would read one level and call the
     * second index a leaf; it is refused whole. */
    EXPECT_CODE(subetha_tower_get(t, path_a, 2, (uint8_t *)&out, 8), SUBETHA_E_INVALID_ARGUMENT);
    uint32_t short_path[2] = {0};
    EXPECT_CODE(subetha_tower_append(t, (uint8_t *)&va, 8, short_path, 2), SUBETHA_E_INVALID_ARGUMENT);

    /* A nil at any level is refused, and the value buffer is untouched. */
    for (int lvl = 0; lvl < 3; lvl++) {
        uint32_t broken[3] = {path_a[0], path_a[1], path_a[2]};
        broken[lvl] = SUBETHA_TOWER_NIL;
        out = 0xDEADull;
        EXPECT_CODE(subetha_tower_get(t, broken, 3, (uint8_t *)&out, 8), SUBETHA_E_INVALID_ARGUMENT);
        CHECK(out == 0xDEADull);
    }

    /* A path that names another path's lower level: the tower disagrees
     * from level 1 down and says so rather than resolving to 222. */
    uint32_t crossed[3] = {path_a[0], path_b[1], path_b[2]};
    out = 0xDEADull;
    EXPECT_CODE(subetha_tower_get(t, crossed, 3, (uint8_t *)&out, 8), SUBETHA_E_INVALID_ARGUMENT);
    CHECK(out == 0xDEADull);

    /* Depth one: a bare region with a one-entry path. */
    char flat[1024];
    snprintf(flat, sizeof flat, "%s-tower-flat.bin", scratch_prefix);
    subetha_handle f = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_tower_create(flat, 8, 8, NULL, NULL, 0, SUBETHA_MODE_STRICT, &f), SUBETHA_OK);
    uint32_t one[1] = {0};
    EXPECT_CODE(subetha_tower_append(f, (uint8_t *)&va, 8, one, 1), SUBETHA_OK);
    EXPECT_CODE(subetha_tower_get(f, one, 1, (uint8_t *)&out, 8), SUBETHA_OK);
    CHECK(out == 111);
    EXPECT_CODE(subetha_handle_destroy(f), SUBETHA_OK);

    EXPECT_CODE(subetha_tower_flush(t), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(t), SUBETHA_OK);
}

static void test_waker(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-waker.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_waker_create(path, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_waker_open(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_waker_create(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_WAKER);

    subetha_waker_stats stats;
    EXPECT_CODE(subetha_waker_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.parked == 0 && stats.mode == SUBETHA_MODE_STRICT);

    /* A park is a token, so a handle passed where one belongs is refused
     * rather than decoded as a slot on this waker. */
    uint64_t park = 0;
    EXPECT_CODE(subetha_waker_park(h, 5, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_waker_release(h, h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_waker_wait(h, h, 0), SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_waker_park(h, 5, &park), SUBETHA_OK);
    EXPECT_CODE(subetha_waker_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.parked == 1);

    /* A wake below the target leaves the parker where it is. */
    uint64_t woken = 99;
    EXPECT_CODE(subetha_waker_wake_up_to(h, 4, &woken), SUBETHA_OK);
    CHECK(woken == 0);

    /* At the target it is woken, and the wait gives the park back, so a
     * release afterwards names nothing. */
    EXPECT_CODE(subetha_waker_wake_up_to(h, 5, &woken), SUBETHA_OK);
    CHECK(woken == 1);
    EXPECT_CODE(subetha_waker_wait(h, park, 5000), SUBETHA_OK);
    EXPECT_CODE(subetha_waker_release(h, park), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_waker_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.parked == 0);

    /* A parker that changes its mind releases instead of waiting. */
    EXPECT_CODE(subetha_waker_park(h, 9, &park), SUBETHA_OK);
    EXPECT_CODE(subetha_waker_release(h, park), SUBETHA_OK);
    EXPECT_CODE(subetha_waker_release(h, park), SUBETHA_E_INVALID_ARGUMENT);

    /* A wait with nobody waking gives the deadline back. */
    EXPECT_CODE(subetha_waker_park(h, 11, &park), SUBETHA_OK);
    EXPECT_CODE(subetha_waker_wait(h, park, 40), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_waker_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.parked == 0);

    /* Every slot taken is a refusal the caller answers by spinning. */
    uint64_t held[4];
    for (int i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_waker_park(h, 100, &held[i]), SUBETHA_OK);
    }
    uint64_t overflow = 0;
    EXPECT_CODE(subetha_waker_park(h, 100, &overflow), SUBETHA_E_RING_WAKER_FULL);
    EXPECT_CODE(subetha_waker_wake_all(h, &woken), SUBETHA_OK);
    CHECK(woken == 4);
    for (int i = 0; i < 4; i++) {
        EXPECT_CODE(subetha_waker_wait(h, held[i], 5000), SUBETHA_OK);
    }

    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_waker_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1);
}

static void test_shared_arc(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-arc.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE;
    const uint8_t initial[8] = {1, 2, 3, 4, 5, 6, 7, 8};

    EXPECT_CODE(subetha_shared_arc_create(path, initial, 0, 4, SUBETHA_ARC_KEEP, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_shared_arc_create(path, initial, 8, 0, SUBETHA_ARC_KEEP, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    /* The policy is one of two named values, and zero is `UNLINK`, so an
     * unknown one is refused rather than taken as a default. */
    EXPECT_CODE(subetha_shared_arc_create(path, initial, 8, 4, 7, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_shared_arc_open(path, 8, 4, SUBETHA_ARC_KEEP, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_shared_arc_create(path, initial, 8, 4, SUBETHA_ARC_KEEP, SUBETHA_MODE_STRICT, &h),
                SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_SHARED_ARC);

    subetha_shared_arc_stats stats;
    EXPECT_CODE(subetha_shared_arc_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.value_bytes == 8 && stats.capacity == 4 && stats.strong_count == 1);
    CHECK(stats.on_last == SUBETHA_ARC_KEEP && stats.mode == SUBETHA_MODE_STRICT);

    uint8_t out[8];
    size_t len = 0;
    EXPECT_CODE(subetha_shared_arc_read(h, 0, 8, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 8 && memcmp(out, initial, 8) == 0);

    /* A range past the region is out of bounds; the last byte is not. */
    EXPECT_CODE(subetha_shared_arc_read(h, 4, 8, out, sizeof out, &len), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_shared_arc_write(h, 8, initial, 1), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_shared_arc_write(h, 7, initial, 1), SUBETHA_OK);
    EXPECT_CODE(subetha_shared_arc_write(h, 0, NULL, 1), SUBETHA_E_INVALID_ARGUMENT);

    /* A second handle attaches to the one region: a write through the
     * first is what the second reads. */
    EXPECT_CODE(subetha_shared_arc_open(path, 8, 4, SUBETHA_ARC_KEEP, SUBETHA_MODE_STRICT, &again),
                SUBETHA_OK);
    uint64_t count = 0;
    EXPECT_CODE(subetha_shared_arc_strong_count(again, &count), SUBETHA_OK);
    CHECK(count == 2);
    const uint8_t mark[2] = {0xAA, 0xBB};
    EXPECT_CODE(subetha_shared_arc_write(h, 2, mark, 2), SUBETHA_OK);
    EXPECT_CODE(subetha_shared_arc_read(again, 2, 2, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 2 && out[0] == 0xAA && out[1] == 0xBB);

    /* A length that disagrees with the backing is refused. */
    subetha_handle wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_shared_arc_open(path, 16, 4, SUBETHA_ARC_KEEP, SUBETHA_MODE_STRICT, &wrong),
                SUBETHA_E_RING_LAYOUT_MISMATCH);

    /* This process is alive, so it holds no dead slot and frees none. */
    uint64_t freed = 99;
    EXPECT_CODE(subetha_shared_arc_reap_dead_holders(h, &freed), SUBETHA_OK);
    CHECK(freed == 0);

    EXPECT_CODE(subetha_shared_arc_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_shared_arc_strong_count(h, &count), SUBETHA_OK);
    CHECK(count == 1);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    subetha_unlink_report report;
    EXPECT_CODE(subetha_shared_arc_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1);
}

static void test_epoch_barrier(const char *scratch_prefix)
{
    char path[1024], beats_path[1024];
    snprintf(path, sizeof path, "%s-barrier.bin", scratch_prefix);
    snprintf(beats_path, sizeof beats_path, "%s-barrier-beats.bin", scratch_prefix);
    subetha_handle beats = SUBETHA_HANDLE_NONE, h = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_heartbeat_create(beats_path, 8, SUBETHA_MODE_STRICT, &beats), SUBETHA_OK);

    /* The barrier is built from the heartbeat HANDLE, so a handle of
     * another kind is refused rather than opening a second view. */
    EXPECT_CODE(subetha_epoch_barrier_create(path, h, 0, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_HANDLE);
    EXPECT_CODE(subetha_epoch_barrier_create(path, beats, 0, SUBETHA_MODE_STRICT, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_epoch_barrier_create(path, beats, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_EPOCH_BARRIER);

    /* A grace of zero takes the default rather than meaning "no window",
     * which would make every slot stale the moment it stopped beating. */
    subetha_epoch_barrier_stats stats;
    EXPECT_CODE(subetha_epoch_barrier_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.mode == SUBETHA_MODE_STRICT);
    CHECK(stats.grace_epochs == SUBETHA_BARRIER_GRACE_DEFAULT);
    CHECK(stats.epoch == 0 && stats.arrived == 0);
    CHECK(stats.live_peers == 0);

    uint32_t slot = 0, live = 99, epoch = 99;
    EXPECT_CODE(subetha_heartbeat_register(beats, 4242, &slot), SUBETHA_OK);
    EXPECT_CODE(subetha_epoch_barrier_live_peers(h, &live), SUBETHA_OK);
    CHECK(live == 1);
    EXPECT_CODE(subetha_epoch_barrier_epoch(h, &epoch), SUBETHA_OK);
    CHECK(epoch == 0);
    EXPECT_CODE(subetha_epoch_barrier_live_peers(h, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_epoch_barrier_wait_quorum(h, 0, 0), SUBETHA_E_INVALID_ARGUMENT);

    /* A second slot nobody beats for: the wait has someone to wait for
     * and gives the deadline back instead of returning at once. */
    uint32_t other = 0;
    EXPECT_CODE(subetha_heartbeat_register(beats, 4243, &other), SUBETHA_OK);
    EXPECT_CODE(subetha_epoch_barrier_wait_timeout(h, 0, 60), SUBETHA_E_TIMEOUT);

    EXPECT_CODE(subetha_epoch_barrier_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(beats), SUBETHA_OK);

    subetha_unlink_report report;
    EXPECT_CODE(subetha_epoch_barrier_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1);
    EXPECT_CODE(subetha_heartbeat_unlink(beats_path, &report), SUBETHA_OK);
}

static void test_fence_clock(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-fence.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_fence_clock_create(path, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_fence_clock_open(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_fence_clock_create(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_FENCE_CLOCK);
    subetha_fence_clock_stats stats;
    EXPECT_CODE(subetha_fence_clock_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.mode == SUBETHA_MODE_STRICT);

    /* Slots are plain indexes, so registering allocates nothing and a
     * slot past the capacity is refused rather than resolved. */
    uint32_t a = 99, b = 99;
    EXPECT_CODE(subetha_fence_clock_register(h, 11, &a), SUBETHA_OK);
    EXPECT_CODE(subetha_fence_clock_register(h, 22, &b), SUBETHA_OK);
    CHECK(a != b);
    EXPECT_CODE(subetha_fence_clock_register(h, 33, NULL), SUBETHA_E_INVALID_ARGUMENT);
    subetha_hlc sent, got, local;
    EXPECT_CODE(subetha_fence_clock_tick(h, 4, &sent), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_fence_clock_tick(h, a, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* A merge orders after the value it took in, which is what makes the
     * order total across processes. */
    EXPECT_CODE(subetha_fence_clock_tick(h, a, &sent), SUBETHA_OK);
    EXPECT_CODE(subetha_fence_clock_merge(h, b, sent, &got), SUBETHA_OK);
    CHECK(got.physical_us > sent.physical_us
          || (got.physical_us == sent.physical_us && got.logical > sent.logical));
    EXPECT_CODE(subetha_fence_clock_get_local(h, b, &local), SUBETHA_OK);
    CHECK(local.physical_us == got.physical_us && local.logical == got.logical);

    /* The fence is the latest clock across registered slots, so every
     * participant's work stands at or below it. */
    subetha_hlc fence, published, read_back;
    EXPECT_CODE(subetha_fence_clock_compute_global_fence(h, &fence), SUBETHA_OK);
    EXPECT_CODE(subetha_fence_clock_get_local(h, a, &local), SUBETHA_OK);
    CHECK(fence.physical_us > local.physical_us
          || (fence.physical_us == local.physical_us && fence.logical >= local.logical));
    EXPECT_CODE(subetha_fence_clock_get_local(h, b, &local), SUBETHA_OK);
    CHECK(fence.physical_us == local.physical_us && fence.logical == local.logical);

    /* Publishing makes the same value readable without walking slots. */
    EXPECT_CODE(subetha_fence_clock_publish_global_fence(h, &published), SUBETHA_OK);
    EXPECT_CODE(subetha_fence_clock_read_global_fence(h, &read_back), SUBETHA_OK);
    CHECK(published.physical_us == read_back.physical_us && published.logical == read_back.logical);
    EXPECT_CODE(subetha_fence_clock_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.fence_epoch >= 1 && stats.shared_clock_us > 0);

    /* A slot answers with the pid that took it; an unregistered one says
     * so rather than answering with stale bytes. */
    subetha_fence_clock_slot snapshot;
    bool registered = false;
    EXPECT_CODE(subetha_fence_clock_read_slot(h, a, &snapshot, &registered), SUBETHA_OK);
    CHECK(registered && snapshot.pid == 11);
    EXPECT_CODE(subetha_fence_clock_read_slot(h, a, NULL, &registered), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_fence_clock_unregister(h, a), SUBETHA_OK);
    EXPECT_CODE(subetha_fence_clock_read_slot(h, a, &snapshot, &registered), SUBETHA_OK);
    CHECK(!registered);
    EXPECT_CODE(subetha_fence_clock_unregister(h, 4), SUBETHA_E_INVALID_ARGUMENT);

    /* Registering and unregistering in a run costs nothing that has to be
     * closed, which is the point of a slot rather than a handle: four
     * slots carry two hundred rounds. */
    for (int i = 0; i < 200; i++) {
        uint32_t slot = 99;
        EXPECT_CODE(subetha_fence_clock_register(h, 44, &slot), SUBETHA_OK);
        EXPECT_CODE(subetha_fence_clock_unregister(h, slot), SUBETHA_OK);
    }

    /* A second handle shares the slots; another capacity is refused. */
    EXPECT_CODE(subetha_fence_clock_open(path, 4, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_fence_clock_open(path, 8, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_fence_clock_read_slot(again, b, &snapshot, &registered), SUBETHA_OK);
    CHECK(registered && snapshot.pid == 22);
    EXPECT_CODE(subetha_fence_clock_flush(h), SUBETHA_OK);
    uint8_t byte = 0;
    EXPECT_CODE(subetha_ring_try_push(h, 0, &byte, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A reset drops every registration. */
    EXPECT_CODE(subetha_fence_clock_reset(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_fence_clock_read_slot(h, b, &snapshot, &registered), SUBETHA_OK);
    CHECK(!registered);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_fence_clock_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
}

static void test_named_releases(const char *scratch_prefix)
{
    char lock_path[1024], sem_path[1024], epoch_path[1024];
    snprintf(lock_path, sizeof lock_path, "%s-relock", scratch_prefix);
    snprintf(sem_path, sizeof sem_path, "%s-resem", scratch_prefix);
    snprintf(epoch_path, sizeof epoch_path, "%s-reepoch.bin", scratch_prefix);

    /* The lock is free again the instant the unlock returns, and the
     * token names nothing. */
    subetha_handle l = SUBETHA_HANDLE_NONE;
    uint64_t w = 0, w2 = 0;
    EXPECT_CODE(subetha_rwlock_create(lock_path, SUBETHA_MODE_STRICT, &l), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_try_write(l, &w), SUBETHA_OK);
    subetha_rwlock_stats lstats;
    EXPECT_CODE(subetha_rwlock_read_stats(l, &lstats), SUBETHA_OK);
    CHECK(lstats.has_writer);
    EXPECT_CODE(subetha_rwlock_try_write(l, &w2), SUBETHA_E_WOULD_BLOCK);
    EXPECT_CODE(subetha_rwlock_unlock(l, w), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_read_stats(l, &lstats), SUBETHA_OK);
    CHECK(!lstats.has_writer);
    EXPECT_CODE(subetha_rwlock_try_write(l, &w2), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_unlock(l, w2), SUBETHA_OK);

    /* A spent token is refused, by every route into it. Because the two
     * writes above reused the one write slot, the first token is now
     * stale rather than merely unheld - which a bare slot index could not
     * have told apart. */
    uint32_t hold_kind = 99;
    EXPECT_CODE(subetha_rwlock_hold_kind(l, w, &hold_kind), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_rwlock_unlock(l, w), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_rwlock_unlock(l, w2), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_rwlock_unlock(l, 0), SUBETHA_E_INVALID_ARGUMENT);

    /* A live token answers with the kind it was taken as. */
    EXPECT_CODE(subetha_rwlock_try_write(l, &w), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_hold_kind(l, w, &hold_kind), SUBETHA_OK);
    CHECK(hold_kind == SUBETHA_LOCK_WRITE);
    EXPECT_CODE(subetha_rwlock_unlock(l, w), SUBETHA_OK);
    EXPECT_CODE(subetha_rwlock_read_stats(l, &lstats), SUBETHA_OK);
    CHECK(!lstats.has_writer);

    /* Readers have no ceiling in the lock, so the table grows rather than
     * refusing: two hundred at once, all of them live at the same time. */
    uint64_t readers[200];
    for (int i = 0; i < 200; i++) {
        EXPECT_CODE(subetha_rwlock_try_read(l, &readers[i]), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_rwlock_read_stats(l, &lstats), SUBETHA_OK);
    CHECK(lstats.readers == 200);
    for (int i = 0; i < 200; i++) {
        EXPECT_CODE(subetha_rwlock_hold_kind(l, readers[i], &hold_kind), SUBETHA_OK);
        CHECK(hold_kind == SUBETHA_LOCK_READ);
        EXPECT_CODE(subetha_rwlock_unlock(l, readers[i]), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_rwlock_read_stats(l, &lstats), SUBETHA_OK);
    CHECK(lstats.readers == 0 && !lstats.has_writer);
    EXPECT_CODE(subetha_handle_destroy(l), SUBETHA_OK);

    /* A permit comes back exactly once, so the count does not grow past
     * the ceiling and no overflow is recorded. */
    subetha_handle s = SUBETHA_HANDLE_NONE;
    uint64_t p = 0;
    bool holds = false;
    uint64_t held = 99;
    EXPECT_CODE(subetha_semaphore_create(sem_path, 2, 2, SUBETHA_MODE_STRICT, &s), SUBETHA_OK);
    subetha_semaphore_stats sstats;
    EXPECT_CODE(subetha_semaphore_try_acquire(s, &p), SUBETHA_OK);
    EXPECT_CODE(subetha_semaphore_read_stats(s, &sstats), SUBETHA_OK);
    CHECK(sstats.available == 1);
    EXPECT_CODE(subetha_semaphore_holds(s, p, &holds), SUBETHA_OK);
    CHECK(holds);
    EXPECT_CODE(subetha_semaphore_held(s, &held), SUBETHA_OK);
    CHECK(held == 1);
    EXPECT_CODE(subetha_semaphore_release(s, p), SUBETHA_OK);
    EXPECT_CODE(subetha_semaphore_read_stats(s, &sstats), SUBETHA_OK);
    CHECK(sstats.available == 2 && sstats.release_overflows == 0);
    EXPECT_CODE(subetha_semaphore_holds(s, p, &holds), SUBETHA_OK);
    CHECK(!holds);

    /* The second release is refused, so the count stays at the ceiling
     * rather than the semaphore lending a permit it never had. */
    EXPECT_CODE(subetha_semaphore_release(s, p), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_semaphore_release(s, 0), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_semaphore_read_stats(s, &sstats), SUBETHA_OK);
    CHECK(sstats.available == 2 && sstats.release_overflows == 0);
    EXPECT_CODE(subetha_semaphore_held(s, &held), SUBETHA_OK);
    CHECK(held == 0);
    for (int i = 0; i < 200; i++) {
        uint64_t permit = 0;
        EXPECT_CODE(subetha_semaphore_try_acquire(s, &permit), SUBETHA_OK);
        EXPECT_CODE(subetha_semaphore_release(s, permit), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_semaphore_read_stats(s, &sstats), SUBETHA_OK);
    CHECK(sstats.available == 2 && sstats.release_overflows == 0);
    EXPECT_CODE(subetha_semaphore_held(s, &held), SUBETHA_OK);
    /* Every token taken was given back. */
    CHECK(held == 0);
    EXPECT_CODE(subetha_handle_destroy(s), SUBETHA_OK);

    /* A pin lets the reclaim horizon go the instant it is released, not
     * when its slot is swept. */
    subetha_handle e = SUBETHA_HANDLE_NONE;
    uint64_t pin = 0;
    EXPECT_CODE(subetha_epochs_create(epoch_path, 4, SUBETHA_MODE_STRICT, &e), SUBETHA_OK);
    uint64_t stamped = 0, horizon = 0, pinned = 0;
    EXPECT_CODE(subetha_epochs_advance(e, &stamped), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_pin(e, &pin), SUBETHA_OK);
    EXPECT_CODE(subetha_pin_epoch(e, pin, &pinned), SUBETHA_OK);
    CHECK(pinned == stamped);
    uint64_t later = 0;
    EXPECT_CODE(subetha_epochs_advance(e, &later), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_reclaim_horizon(e, &horizon), SUBETHA_OK);
    CHECK(horizon == stamped);
    EXPECT_CODE(subetha_pin_release(e, pin), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_reclaim_horizon(e, &horizon), SUBETHA_OK);
    CHECK(horizon == later);
    EXPECT_CODE(subetha_pin_epoch(e, pin, &pinned), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_pin_release(e, pin), SUBETHA_E_INVALID_ARGUMENT);
    /* Four pin slots against two hundred rounds: a slot comes back on
     * release, or this exhausts the table. */
    for (int i = 0; i < 200; i++) {
        uint64_t token = 0;
        EXPECT_CODE(subetha_epochs_pin(e, &token), SUBETHA_OK);
        EXPECT_CODE(subetha_pin_release(e, token), SUBETHA_OK);
    }
    subetha_epochs_stats estats;
    EXPECT_CODE(subetha_epochs_read_stats(e, &estats), SUBETHA_OK);
    CHECK(estats.live_pins == 0);
    uint64_t held_pins = 99, held_tickets = 99;
    EXPECT_CODE(subetha_epochs_held(e, &held_pins, &held_tickets), SUBETHA_OK);
    CHECK(held_pins == 0 && held_tickets == 0);

    /* A ticket publishes at once and exactly once, and its slot comes
     * back on the release rather than on the sweep - four slots against
     * two hundred rounds would otherwise run out. */
    uint64_t t = 0, reserved = 0, now = 0;
    EXPECT_CODE(subetha_epochs_begin(e, &t), SUBETHA_OK);
    EXPECT_CODE(subetha_ticket_epoch(e, t, &reserved), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_now(e, &now), SUBETHA_OK);
    CHECK(now == reserved - 1);
    EXPECT_CODE(subetha_ticket_publish(e, t), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_now(e, &now), SUBETHA_OK);
    CHECK(now == reserved);
    EXPECT_CODE(subetha_ticket_publish(e, t), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ticket_epoch(e, t, &reserved), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_epochs_read_stats(e, &estats), SUBETHA_OK);
    CHECK(estats.open_tickets == 0);
    for (int i = 0; i < 200; i++) {
        uint64_t token = 0;
        EXPECT_CODE(subetha_epochs_begin(e, &token), SUBETHA_OK);
        EXPECT_CODE(subetha_ticket_publish(e, token), SUBETHA_OK);
    }
    EXPECT_CODE(subetha_epochs_read_stats(e, &estats), SUBETHA_OK);
    CHECK(estats.open_tickets == 0);
    EXPECT_CODE(subetha_handle_destroy(e), SUBETHA_OK);

    subetha_unlink_report report;
    EXPECT_CODE(subetha_rwlock_unlink(lock_path, &report), SUBETHA_OK);
    EXPECT_CODE(subetha_semaphore_unlink(sem_path, &report), SUBETHA_OK);
    EXPECT_CODE(subetha_epochs_unlink(epoch_path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
}

static void test_semaphore(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-sem", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_semaphore_create(path, 1, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_semaphore_create(path, 3, 2, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_semaphore_open(path, 2, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_semaphore_create(path, 2, 2, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_SEMAPHORE);
    subetha_semaphore_stats stats;
    EXPECT_CODE(subetha_semaphore_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.available == 2 && stats.max_permits == 2 && stats.waiters == 0);
    CHECK(stats.mode == SUBETHA_MODE_STRICT && stats.timeouts == 0 && stats.release_overflows == 0);

    /* Permits run out, and giving one back lets the next caller take it. */
    uint64_t p1 = 0, p2 = 0, p3 = 0;
    EXPECT_CODE(subetha_semaphore_try_acquire(h, &p1), SUBETHA_OK);
    uint64_t held = 0;
    EXPECT_CODE(subetha_semaphore_held(h, &held), SUBETHA_OK);
    CHECK(held == 1);
    EXPECT_CODE(subetha_semaphore_try_acquire(h, &p2), SUBETHA_OK);
    EXPECT_CODE(subetha_semaphore_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.available == 0);
    EXPECT_CODE(subetha_semaphore_try_acquire(h, &p3), SUBETHA_E_WOULD_BLOCK);

    /* A bounded wait against an empty count gives up and says so, and
     * leaves the waiter count back where it was. */
    EXPECT_CODE(subetha_semaphore_acquire(h, 20, &p3), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_semaphore_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.timeouts == 1 && stats.waiters == 0 && stats.available == 0);
    EXPECT_CODE(subetha_semaphore_try_acquire(h, NULL), SUBETHA_E_INVALID_ARGUMENT);

    EXPECT_CODE(subetha_semaphore_release(h, p2), SUBETHA_OK);
    EXPECT_CODE(subetha_semaphore_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.available == 1);
    EXPECT_CODE(subetha_semaphore_acquire(h, 1000, &p3), SUBETHA_OK);
    EXPECT_CODE(subetha_semaphore_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.available == 0);

    /* A second handle shares the count. Destroying the handle that lent
     * the permits gives both back, rather than leaving the count short
     * with nothing able to return them - a token belongs to its handle,
     * so nothing else could. */
    EXPECT_CODE(subetha_semaphore_open(path, 2, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_semaphore_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.available == 0 && stats.max_permits == 2);
    uint64_t spare = 0;
    EXPECT_CODE(subetha_semaphore_try_acquire(again, &spare), SUBETHA_E_WOULD_BLOCK);
    EXPECT_CODE(subetha_semaphore_held(h, &held), SUBETHA_OK);
    CHECK(held == 2);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    EXPECT_CODE(subetha_semaphore_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.available == 2 && stats.release_overflows == 0);

    uint8_t byte = 0;
    EXPECT_CODE(subetha_ring_try_push(again, 0, &byte, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_semaphore_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 3 && report.failed == 0);
    EXPECT_CODE(subetha_semaphore_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 3);
}

static void test_condvar(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-cv", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_condvar_create(path, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_condvar_open(path, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_condvar_create(path, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_CONDVAR);
    subetha_condvar_stats stats;
    EXPECT_CODE(subetha_condvar_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.generation == 0 && stats.max_waiters == 8 && stats.timeouts == 0 && stats.woken == 0);
    CHECK(stats.mode == SUBETHA_MODE_STRICT);

    /* A notify with nobody parked wakes nobody but still moves the
     * generation, which is what a later waiter compares against. */
    uint64_t seen = 99;
    EXPECT_CODE(subetha_condvar_generation(h, &seen), SUBETHA_OK);
    CHECK(seen == 0);
    uint32_t woken = 99;
    EXPECT_CODE(subetha_condvar_notify_all(h, &woken), SUBETHA_OK);
    CHECK(woken == 0);
    EXPECT_CODE(subetha_condvar_generation(h, &seen), SUBETHA_OK);
    CHECK(seen == 1);

    /* A wait from a generation already passed returns at once: the
     * caller read 0, the notify moved it to 1, and nothing parks. This
     * is the lost-notify window closing. */
    EXPECT_CODE(subetha_condvar_wait(h, 0, 0), SUBETHA_OK);

    /* A wait from the current generation has nothing to wake it, so it
     * gives up on its deadline. */
    EXPECT_CODE(subetha_condvar_wait(h, seen, 20), SUBETHA_E_TIMEOUT);
    EXPECT_CODE(subetha_condvar_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.timeouts == 1 && stats.generation == 1);
    EXPECT_CODE(subetha_condvar_generation(h, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* notify_one moves the generation too, and a null out-parameter is
     * allowed for a caller that does not care how many woke. */
    EXPECT_CODE(subetha_condvar_notify_one(h, &woken), SUBETHA_OK);
    CHECK(woken == 0);
    EXPECT_CODE(subetha_condvar_notify_one(h, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_condvar_generation(h, &seen), SUBETHA_OK);
    CHECK(seen == 3);

    /* A second handle shares the generation. */
    EXPECT_CODE(subetha_condvar_open(path, 8, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    uint64_t from_second = 0;
    EXPECT_CODE(subetha_condvar_generation(again, &from_second), SUBETHA_OK);
    CHECK(from_second == 3);
    EXPECT_CODE(subetha_condvar_notify_all(again, NULL), SUBETHA_OK);
    EXPECT_CODE(subetha_condvar_generation(h, &seen), SUBETHA_OK);
    CHECK(seen == 4);
    EXPECT_CODE(subetha_condvar_wait(h, 3, 0), SUBETHA_OK);

    uint8_t byte = 0;
    EXPECT_CODE(subetha_ring_try_push(h, 0, &byte, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_condvar_generation(SUBETHA_HANDLE_NONE, &seen), SUBETHA_E_INVALID_HANDLE);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_condvar_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 2 && report.failed == 0);
    EXPECT_CODE(subetha_condvar_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 2);
}

static void test_owner_lease(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-lease.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE;
    const uint8_t initial[8] = {1, 1, 1, 1, 1, 1, 1, 1};
    EXPECT_CODE(subetha_owner_lease_create(path, initial, sizeof initial, 0, SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_owner_lease_create(path, initial, sizeof initial, SUBETHA_LEASE_PAYLOAD_MAX + 1,
                                           SUBETHA_MODE_STRICT, &h),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_owner_lease_open(path, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_owner_lease_create(path, initial, sizeof initial, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_OWNER_LEASE);
    subetha_owner_lease_stats stats;
    EXPECT_CODE(subetha_owner_lease_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.payload_size == 8 && stats.owner_pid == SUBETHA_LEASE_NO_OWNER && stats.lease_term == 0);
    CHECK(stats.mode == SUBETHA_MODE_STRICT && stats.global_epoch == 0);

    /* pid 0 is the value that means nobody, so every call taking a pid
     * refuses it rather than reading it as a release. */
    bool got = true;
    EXPECT_CODE(subetha_owner_lease_try_acquire(h, 0, 3, &got), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_owner_lease_release(h, 0, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_owner_lease_beat(h, 0, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* Only the holder reads or writes the payload. */
    uint8_t out[8];
    size_t len = 0;
    EXPECT_CODE(subetha_owner_lease_read(h, 100, out, sizeof out, &len), SUBETHA_E_NOT_OWNER);
    EXPECT_CODE(subetha_owner_lease_write(h, 100, initial, sizeof initial), SUBETHA_E_NOT_OWNER);
    EXPECT_CODE(subetha_owner_lease_try_acquire(h, 100, 3, &got), SUBETHA_OK);
    CHECK(got);
    EXPECT_CODE(subetha_owner_lease_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.owner_pid == 100 && stats.lease_term == 1);
    EXPECT_CODE(subetha_owner_lease_read(h, 100, out, 7, &len), SUBETHA_E_BUFFER_TOO_SMALL);
    EXPECT_CODE(subetha_owner_lease_read(h, 100, out, sizeof out, &len), SUBETHA_OK);
    CHECK(len == 8 && out[0] == 1);
    const uint8_t second[8] = {2, 2, 2, 2, 2, 2, 2, 2};
    EXPECT_CODE(subetha_owner_lease_write(h, 100, second, 9), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_owner_lease_write(h, 100, second, sizeof second), SUBETHA_OK);
    EXPECT_CODE(subetha_owner_lease_read(h, 200, out, sizeof out, &len), SUBETHA_E_NOT_OWNER);

    /* A higher pid waits; a lower one preempts and finds the payload the
     * last holder left. */
    EXPECT_CODE(subetha_owner_lease_try_acquire(h, 200, 3, &got), SUBETHA_OK);
    CHECK(!got);
    EXPECT_CODE(subetha_owner_lease_try_acquire(h, 50, 3, &got), SUBETHA_OK);
    CHECK(got);
    EXPECT_CODE(subetha_owner_lease_read(h, 50, out, sizeof out, &len), SUBETHA_OK);
    CHECK(out[0] == 2);
    EXPECT_CODE(subetha_owner_lease_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.owner_pid == 50 && stats.lease_term == 2);

    /* Nothing advances the epoch on its own, so a holder that never
     * beats keeps the lease until something ticks past the window. */
    EXPECT_CODE(subetha_owner_lease_try_acquire(h, 200, 2, &got), SUBETHA_OK);
    CHECK(!got);
    uint64_t epoch = 0;
    for (int i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_owner_lease_tick_epoch(h, &epoch), SUBETHA_OK);
    }
    CHECK(epoch == 3);
    EXPECT_CODE(subetha_owner_lease_try_acquire(h, 200, 2, &got), SUBETHA_OK);
    CHECK(got);
    EXPECT_CODE(subetha_owner_lease_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.owner_pid == 200 && stats.heartbeat_epoch == 3);

    /* A beat puts the holder back out of reach. */
    EXPECT_CODE(subetha_owner_lease_tick_epoch(h, &epoch), SUBETHA_OK);
    bool still = false;
    EXPECT_CODE(subetha_owner_lease_beat(h, 200, &still), SUBETHA_OK);
    CHECK(still);
    EXPECT_CODE(subetha_owner_lease_beat(h, 300, &still), SUBETHA_OK);
    CHECK(!still);
    EXPECT_CODE(subetha_owner_lease_try_acquire(h, 300, 2, &got), SUBETHA_OK);
    CHECK(!got);

    /* A second handle shares the region; another payload size does not. */
    EXPECT_CODE(subetha_owner_lease_open(path, 8, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_owner_lease_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.owner_pid == 200 && stats.payload_size == 8);
    subetha_handle wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_owner_lease_open(path, 4, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    bool released = false;
    EXPECT_CODE(subetha_owner_lease_release(again, 200, &released), SUBETHA_OK);
    CHECK(released);
    EXPECT_CODE(subetha_owner_lease_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.owner_pid == SUBETHA_LEASE_NO_OWNER);

    uint32_t mine = 0;
    EXPECT_CODE(subetha_current_pid(&mine), SUBETHA_OK);
    CHECK(mine != 0);
    EXPECT_CODE(subetha_current_pid(NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_ring_try_push(h, 0, out, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_owner_lease_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    /* A reset strips whatever a holder left. */
    EXPECT_CODE(subetha_owner_lease_reset(path, initial, sizeof initial, 8, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_owner_lease_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.owner_pid == SUBETHA_LEASE_NO_OWNER && stats.lease_term == 0 && stats.global_epoch == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_owner_lease_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_owner_lease_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_leader_election(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-leader.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_leader_open(path, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_leader_create(path, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_LEADER);
    subetha_leader_stats stats;
    EXPECT_CODE(subetha_leader_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.leader_pid == SUBETHA_LEADER_NONE && stats.election_term == 0 && stats.global_epoch == 0);
    CHECK(stats.mode == SUBETHA_MODE_STRICT);

    /* pid 0 means nobody, so every call taking a pid refuses it. */
    bool got = true;
    EXPECT_CODE(subetha_leader_try_claim(h, 0, 3, &got), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_leader_beat(h, 0, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_leader_step_down(h, 0, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_leader_try_claim(h, 100, 3, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* The lowest live id leads, and the term records every handover. */
    EXPECT_CODE(subetha_leader_try_claim(h, 100, 3, &got), SUBETHA_OK);
    CHECK(got);
    EXPECT_CODE(subetha_leader_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.leader_pid == 100 && stats.election_term == 1);
    EXPECT_CODE(subetha_leader_try_claim(h, 200, 3, &got), SUBETHA_OK);
    CHECK(!got);
    EXPECT_CODE(subetha_leader_try_claim(h, 50, 3, &got), SUBETHA_OK);
    CHECK(got);
    EXPECT_CODE(subetha_leader_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.leader_pid == 50 && stats.election_term == 2);
    EXPECT_CODE(subetha_leader_try_claim(h, 50, 3, &got), SUBETHA_OK);
    CHECK(got);
    EXPECT_CODE(subetha_leader_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.election_term == 2);

    /* Nothing advances the epoch on its own, so a leader that never
     * beats keeps the role until something ticks past the window. */
    EXPECT_CODE(subetha_leader_try_claim(h, 200, 2, &got), SUBETHA_OK);
    CHECK(!got);
    uint64_t epoch = 0;
    for (int i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_leader_tick_epoch(h, &epoch), SUBETHA_OK);
    }
    CHECK(epoch == 3);
    EXPECT_CODE(subetha_leader_try_claim(h, 200, 2, &got), SUBETHA_OK);
    CHECK(got);
    EXPECT_CODE(subetha_leader_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.leader_pid == 200 && stats.leader_heartbeat == 3 && stats.election_term == 3);

    /* A beat keeps the role; a follower's beat does nothing. */
    EXPECT_CODE(subetha_leader_tick_epoch(h, &epoch), SUBETHA_OK);
    EXPECT_CODE(subetha_leader_tick_epoch(h, &epoch), SUBETHA_OK);
    EXPECT_CODE(subetha_leader_tick_epoch(h, &epoch), SUBETHA_OK);
    bool still = false;
    EXPECT_CODE(subetha_leader_beat(h, 200, &still), SUBETHA_OK);
    CHECK(still);
    EXPECT_CODE(subetha_leader_beat(h, 300, &still), SUBETHA_OK);
    CHECK(!still);
    EXPECT_CODE(subetha_leader_try_claim(h, 300, 2, &got), SUBETHA_OK);
    CHECK(!got);

    /* A second handle shares the election, and a step-down hands over at
     * once rather than waiting out the window. */
    EXPECT_CODE(subetha_leader_open(path, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_leader_read_stats(again, &stats), SUBETHA_OK);
    CHECK(stats.leader_pid == 200);
    bool stepped = false;
    EXPECT_CODE(subetha_leader_step_down(again, 200, &stepped), SUBETHA_OK);
    CHECK(stepped);
    EXPECT_CODE(subetha_leader_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.leader_pid == SUBETHA_LEADER_NONE);
    EXPECT_CODE(subetha_leader_try_claim(h, 300, 2, &got), SUBETHA_OK);
    CHECK(got);
    EXPECT_CODE(subetha_leader_step_down(h, 300, &stepped), SUBETHA_OK);
    CHECK(stepped);

    uint8_t byte = 0;
    EXPECT_CODE(subetha_ring_try_push(h, 0, &byte, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_leader_flush(h), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    EXPECT_CODE(subetha_leader_reset(path, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_leader_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.leader_pid == SUBETHA_LEADER_NONE && stats.election_term == 0 && stats.global_epoch == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_leader_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_leader_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_heartbeat_table(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-hb.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_heartbeat_create(path, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_heartbeat_open(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_heartbeat_create(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_HEARTBEAT);
    subetha_heartbeat_stats stats;
    EXPECT_CODE(subetha_heartbeat_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.registered == 0 && stats.global_epoch == 0);
    CHECK(stats.mode == SUBETHA_MODE_STRICT);

    /* A slot is taken by pid, and every later call names its index. */
    uint32_t first = 99, second = 99;
    EXPECT_CODE(subetha_heartbeat_register(h, 0, &first), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_heartbeat_register(h, 100, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_heartbeat_register(h, 100, &first), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_register(h, 200, &second), SUBETHA_OK);
    CHECK(first == 0 && second == 1);
    EXPECT_CODE(subetha_heartbeat_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.registered == 2);

    /* A slot nobody holds reads as empty rather than as an error, so a
     * watcher walks every index. */
    subetha_heartbeat_slot slot;
    EXPECT_CODE(subetha_heartbeat_read_slot(h, 3, &slot), SUBETHA_OK);
    CHECK(slot.pid == SUBETHA_HEARTBEAT_EMPTY_PID);
    EXPECT_CODE(subetha_heartbeat_read_slot(h, 4, &slot), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_heartbeat_read_slot(h, 0, NULL), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_heartbeat_beat(h, 4), SUBETHA_E_OUT_OF_BOUNDS);

    /* One process keeps beating while the other goes quiet, and the gap
     * between their epochs is what names the one to check on. */
    uint64_t epoch = 0;
    for (int i = 0; i < 3; i++) {
        EXPECT_CODE(subetha_heartbeat_tick_epoch(h, &epoch), SUBETHA_OK);
        EXPECT_CODE(subetha_heartbeat_beat(h, first), SUBETHA_OK);
    }
    CHECK(epoch == 3);
    EXPECT_CODE(subetha_heartbeat_read_slot(h, first, &slot), SUBETHA_OK);
    CHECK(slot.pid == 100 && slot.last_seen_epoch == 3);
    EXPECT_CODE(subetha_heartbeat_read_slot(h, second, &slot), SUBETHA_OK);
    CHECK(slot.pid == 200 && slot.last_seen_epoch == 0);

    /* The work a process took rides on its slot, so a watcher knows what
     * to reassign when it goes. */
    EXPECT_CODE(subetha_heartbeat_mark_in_flight(h, second, 0), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_mark_in_flight(h, second, 63), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_mark_in_flight(h, second, SUBETHA_HEARTBEAT_IN_FLIGHT), SUBETHA_E_OUT_OF_BOUNDS);
    EXPECT_CODE(subetha_heartbeat_read_slot(h, second, &slot), SUBETHA_OK);
    CHECK(slot.in_flight_bitmap == ((uint64_t)1 << 63 | 1));
    EXPECT_CODE(subetha_heartbeat_clear_in_flight(h, second, 0), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_read_slot(h, second, &slot), SUBETHA_OK);
    CHECK(slot.in_flight_bitmap == (uint64_t)1 << 63);

    /* A second handle sees the same slots; another capacity is refused. */
    EXPECT_CODE(subetha_heartbeat_open(path, 4, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_open(path, 8, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_heartbeat_read_slot(again, first, &slot), SUBETHA_OK);
    CHECK(slot.pid == 100 && slot.last_seen_epoch == 3);

    /* A freed slot is handed out again, and a full table refuses. */
    EXPECT_CODE(subetha_heartbeat_unregister(again, second), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.registered == 1);
    uint32_t reused = 99;
    EXPECT_CODE(subetha_heartbeat_register(h, 300, &reused), SUBETHA_OK);
    CHECK(reused == second);
    uint32_t spare = 99;
    EXPECT_CODE(subetha_heartbeat_register(h, 400, &spare), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_register(h, 500, &spare), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_register(h, 600, &spare), SUBETHA_E_RING_FULL);

    uint8_t byte = 0;
    EXPECT_CODE(subetha_ring_try_push(h, 0, &byte, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    EXPECT_CODE(subetha_heartbeat_reset(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_heartbeat_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.registered == 0 && stats.global_epoch == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_heartbeat_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_heartbeat_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

static void test_holder_table(const char *scratch_prefix)
{
    char path[1024];
    snprintf(path, sizeof path, "%s-holders.bin", scratch_prefix);
    subetha_handle h = SUBETHA_HANDLE_NONE, again = SUBETHA_HANDLE_NONE, wrong = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_holders_create(path, 0, SUBETHA_MODE_STRICT, &h), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_holders_open(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_E_RING_IO);
    EXPECT_CODE(subetha_holders_create(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    uint32_t kind = 0;
    EXPECT_CODE(subetha_handle_kind(h, &kind), SUBETHA_OK);
    CHECK(kind == SUBETHA_KIND_HOLDERS);
    subetha_holders_stats stats;
    EXPECT_CODE(subetha_holders_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.capacity == 4 && stats.live == 0 && stats.mode == SUBETHA_MODE_STRICT);

    /* The two values the table reads for itself are refused as payloads,
     * since either makes a held slot read as something else. */
    uint32_t slot = 99;
    EXPECT_CODE(subetha_holders_claim(h, SUBETHA_HOLDER_FREE, &slot), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_holders_claim(h, SUBETHA_HOLDER_RESERVED, &slot), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_holders_claim(h, 7, NULL), SUBETHA_E_INVALID_ARGUMENT);

    /* A claim carries the payload and the process that set it. */
    EXPECT_CODE(subetha_holders_claim(h, 7, &slot), SUBETHA_OK);
    CHECK(slot == 0);
    uint64_t state = 0;
    uint32_t pid = 0, mine = 0;
    EXPECT_CODE(subetha_current_pid(&mine), SUBETHA_OK);
    EXPECT_CODE(subetha_holders_read_slot(h, slot, &state, &pid), SUBETHA_OK);
    CHECK(state == 7 && pid == mine);
    EXPECT_CODE(subetha_holders_read_slot(h, 3, &state, &pid), SUBETHA_OK);
    CHECK(state == SUBETHA_HOLDER_FREE);
    EXPECT_CODE(subetha_holders_read_slot(h, 4, &state, &pid), SUBETHA_E_OUT_OF_BOUNDS);

    /* A reservation is visible as held but carries no payload yet, which
     * is what stops a reader acting on a claim that is not finished. */
    uint32_t held = 99;
    EXPECT_CODE(subetha_holders_reserve(h, &held), SUBETHA_OK);
    CHECK(held == 1);
    EXPECT_CODE(subetha_holders_read_slot(h, held, &state, NULL), SUBETHA_OK);
    CHECK(state == SUBETHA_HOLDER_RESERVED);
    EXPECT_CODE(subetha_holders_publish(h, held, SUBETHA_HOLDER_FREE), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_holders_publish(h, held, 13), SUBETHA_OK);
    EXPECT_CODE(subetha_holders_read_slot(h, held, &state, NULL), SUBETHA_OK);
    CHECK(state == 13);
    EXPECT_CODE(subetha_holders_publish(h, 4, 13), SUBETHA_E_OUT_OF_BOUNDS);

    /* A caller that wants one slot in particular says so, and is told
     * whether it was free. */
    bool claimed = true;
    EXPECT_CODE(subetha_holders_try_claim_slot(h, held, 17, &claimed), SUBETHA_OK);
    CHECK(!claimed);
    EXPECT_CODE(subetha_holders_try_claim_slot(h, 2, 17, &claimed), SUBETHA_OK);
    CHECK(claimed);
    EXPECT_CODE(subetha_holders_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.live == 3);

    /* Filling the last slot is what makes a claim have nowhere to go. */
    uint32_t last = 99;
    EXPECT_CODE(subetha_holders_claim(h, 19, &last), SUBETHA_OK);
    CHECK(last == 3);
    uint32_t spare = 99;
    EXPECT_CODE(subetha_holders_claim(h, 21, &spare), SUBETHA_E_RING_FULL);
    EXPECT_CODE(subetha_holders_reserve(h, &spare), SUBETHA_E_RING_FULL);

    /* This process is alive, so a reap leaves every slot alone. */
    uint32_t freed = 99;
    EXPECT_CODE(subetha_holders_reap_dead(h, &freed), SUBETHA_OK);
    CHECK(freed == 0);
    EXPECT_CODE(subetha_holders_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.live == 4);

    /* A second handle shares the claims; another capacity does not. */
    EXPECT_CODE(subetha_holders_open(path, 4, SUBETHA_MODE_STRICT, &again), SUBETHA_OK);
    EXPECT_CODE(subetha_holders_open(path, 8, SUBETHA_MODE_STRICT, &wrong), SUBETHA_E_RING_LAYOUT_MISMATCH);
    EXPECT_CODE(subetha_holders_read_slot(again, 0, &state, &pid), SUBETHA_OK);
    CHECK(state == 7 && pid == mine);
    EXPECT_CODE(subetha_holders_release(again, 0), SUBETHA_OK);
    EXPECT_CODE(subetha_holders_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.live == 3);
    EXPECT_CODE(subetha_holders_claim(h, 23, &spare), SUBETHA_OK);
    CHECK(spare == 0);
    EXPECT_CODE(subetha_holders_release(h, 4), SUBETHA_E_OUT_OF_BOUNDS);

    uint8_t byte = 0;
    EXPECT_CODE(subetha_ring_try_push(h, 0, &byte, 1), SUBETHA_E_WRONG_KIND);
    EXPECT_CODE(subetha_handle_destroy(again), SUBETHA_OK);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);

    EXPECT_CODE(subetha_holders_reset(path, 4, SUBETHA_MODE_STRICT, &h), SUBETHA_OK);
    EXPECT_CODE(subetha_holders_read_stats(h, &stats), SUBETHA_OK);
    CHECK(stats.live == 0);
    EXPECT_CODE(subetha_handle_destroy(h), SUBETHA_OK);
    subetha_unlink_report report;
    EXPECT_CODE(subetha_holders_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 1 && report.failed == 0);
    EXPECT_CODE(subetha_holders_unlink(path, &report), SUBETHA_OK);
    CHECK(report.removed == 0 && report.missing == 1);
}

/* The batch forms for the shared-state families, whose items are not the
 * rings' uniform payloads: the vec and the arena each answer with
 * something per item, and the map's item is two runs of bytes rather than
 * one. */
static void test_batch_shared_state(const char *scratch_prefix)
{
    char vec_path[1024], arena_path[1024], map_path[1024];
    snprintf(vec_path, sizeof vec_path, "%s-bvec.bin", scratch_prefix);
    snprintf(arena_path, sizeof arena_path, "%s-barena.bin", scratch_prefix);
    snprintf(map_path, sizeof map_path, "%s-bmap.bin", scratch_prefix);
    const subetha_element_layout layout = {16, 1, 0x494e4445583136ULL};

    /* Elements in the caller's own array, laid out with a gap, so a
     * stride wider than the item is what the batch reads. */
    struct padded {
        uint8_t element[16];
        uint8_t gap[8];
    };
    struct padded items[4];
    memset(items, 0, sizeof items);
    for (int i = 0; i < 4; i++) {
        memset(items[i].element, 'a' + i, 16);
    }

    /* The vec answers with the index each element landed at. */
    subetha_handle v = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_vec_create(vec_path, 6, &layout, SUBETHA_MODE_STRICT, &v), SUBETHA_OK);
    uint64_t indices[4] = {99, 99, 99, 99};
    size_t done = 99;
    EXPECT_CODE(subetha_vec_push_back_many(v, (const uint8_t *)items, sizeof items[0], 16, 4, indices, &done),
                SUBETHA_OK);
    CHECK(done == 4);
    CHECK(indices[0] == 0 && indices[1] == 1 && indices[2] == 2 && indices[3] == 3);
    uint8_t read[4][16];
    memset(read, 0, sizeof read);
    EXPECT_CODE(subetha_vec_get_many(v, indices, (uint8_t *)read, 16, 4, &done), SUBETHA_OK);
    CHECK(done == 4);
    CHECK(read[0][0] == 'a' && read[3][0] == 'd');

    /* A vec with room for two more takes two and reports what landed. */
    EXPECT_CODE(subetha_vec_push_back_many(v, (const uint8_t *)items, sizeof items[0], 16, 4, indices, &done),
                SUBETHA_OK);
    CHECK(done == 2);
    CHECK(indices[0] == 4 && indices[1] == 5);
    EXPECT_CODE(subetha_vec_push_back_many(v, (const uint8_t *)items, sizeof items[0], 16, 1, indices, &done),
                SUBETHA_E_RING_FULL);
    CHECK(done == 0);

    /* The argument rules the shared helper enforces. */
    EXPECT_CODE(subetha_vec_push_back_many(v, (const uint8_t *)items, sizeof items[0], 16, 4, indices, NULL),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_vec_push_back_many(v, NULL, sizeof items[0], 16, 4, indices, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_vec_push_back_many(v, (const uint8_t *)items, 8, 16, 4, indices, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_vec_push_back_many(v, (const uint8_t *)items, sizeof items[0], 16, 4, NULL, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_vec_push_back_many(v, (const uint8_t *)items, 15, 16, 0, NULL, &done), SUBETHA_OK);
    CHECK(done == 0);
    EXPECT_CODE(subetha_vec_push_back_many(v, (const uint8_t *)items, sizeof items[0], 15, 1, indices, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    uint64_t past[1] = {99};
    EXPECT_CODE(subetha_vec_get_many(v, past, (uint8_t *)read, 16, 1, &done), SUBETHA_E_OUT_OF_BOUNDS);
    CHECK(done == 0);
    EXPECT_CODE(subetha_vec_get_many(v, NULL, (uint8_t *)read, 16, 1, &done), SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_handle_destroy(v), SUBETHA_OK);

    /* The arena answers with the reference naming each value. */
    subetha_handle a = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_arena_create(arena_path, 4096, SUBETHA_MODE_STRICT, &a), SUBETHA_OK);
    uint64_t refs[4] = {0, 0, 0, 0};
    EXPECT_CODE(subetha_arena_intern_many(a, (const uint8_t *)items, sizeof items[0], 16, 4, refs, &done),
                SUBETHA_OK);
    CHECK(done == 4);
    for (int i = 0; i < 4; i++) {
        uint8_t out[16];
        size_t len = 0;
        EXPECT_CODE(subetha_arena_get(a, refs[i], out, sizeof out, &len), SUBETHA_OK);
        CHECK(len == 16 && out[0] == 'a' + i);
    }
    /* The arena appends rather than deduplicating - the name says intern
     * but the contract says append - so the same bytes a second time get
     * their own references, and both sets read back the same values. */
    uint64_t again[4] = {0, 0, 0, 0};
    EXPECT_CODE(subetha_arena_intern_many(a, (const uint8_t *)items, sizeof items[0], 16, 4, again, &done),
                SUBETHA_OK);
    CHECK(done == 4);
    CHECK(memcmp(refs, again, sizeof refs) != 0);
    for (int i = 0; i < 4; i++) {
        uint8_t first[16], second[16];
        size_t flen = 0, slen = 0;
        EXPECT_CODE(subetha_arena_get(a, refs[i], first, sizeof first, &flen), SUBETHA_OK);
        EXPECT_CODE(subetha_arena_get(a, again[i], second, sizeof second, &slen), SUBETHA_OK);
        CHECK(flen == slen && memcmp(first, second, flen) == 0);
    }
    EXPECT_CODE(subetha_handle_destroy(a), SUBETHA_OK);

    /* The map's item is a key and a value, kept in the caller's own two
     * arrays rather than packed into pairs. */
    subetha_handle m = SUBETHA_HANDLE_NONE;
    EXPECT_CODE(subetha_hashmap_create(map_path, 64, 4, 8, SUBETHA_MODE_STRICT, &m), SUBETHA_OK);
    uint8_t keys[4][4];
    uint8_t values[4][8];
    memset(keys, 0, sizeof keys);
    memset(values, 0, sizeof values);
    for (int i = 0; i < 4; i++) {
        keys[i][0] = (uint8_t)(i + 1);
        values[i][0] = (uint8_t)((i + 1) * 10);
    }
    EXPECT_CODE(subetha_hashmap_insert_many(m, (const uint8_t *)keys, 4, (const uint8_t *)values, 8, 4, &done),
                SUBETHA_OK);
    CHECK(done == 4);
    uint8_t got[4][8];
    memset(got, 0, sizeof got);
    EXPECT_CODE(subetha_hashmap_get_many(m, (const uint8_t *)keys, 4, (uint8_t *)got, 8, 4, &done), SUBETHA_OK);
    CHECK(done == 4);
    CHECK(got[0][0] == 10 && got[3][0] == 40);

    /* A key the map does not hold stops the run where it is. */
    uint8_t missing[2][4];
    memset(missing, 0, sizeof missing);
    missing[0][0] = 1;
    missing[1][0] = 200;
    EXPECT_CODE(subetha_hashmap_get_many(m, (const uint8_t *)missing, 4, (uint8_t *)got, 8, 2, &done),
                SUBETHA_OK);
    CHECK(done == 1);
    memset(missing, 0, sizeof missing);
    missing[0][0] = 200;
    EXPECT_CODE(subetha_hashmap_get_many(m, (const uint8_t *)missing, 4, (uint8_t *)got, 8, 1, &done),
                SUBETHA_E_MAP_KEY_ABSENT);
    CHECK(done == 0);

    /* A stride that spans the key but not the value names the value. */
    EXPECT_CODE(subetha_hashmap_insert_many(m, (const uint8_t *)keys, 4, (const uint8_t *)values, 4, 4, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hashmap_insert_many(m, (const uint8_t *)keys, 2, (const uint8_t *)values, 8, 4, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hashmap_insert_many(m, (const uint8_t *)keys, 4, NULL, 8, 4, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_hashmap_get_many(m, (const uint8_t *)keys, 2, (uint8_t *)got, 8, 4, &done),
                SUBETHA_E_INVALID_ARGUMENT);
    EXPECT_CODE(subetha_handle_destroy(m), SUBETHA_OK);

    subetha_unlink_report report;
    EXPECT_CODE(subetha_vec_unlink(vec_path, &report), SUBETHA_OK);
    CHECK(report.removed == 1);
    EXPECT_CODE(subetha_arena_unlink(arena_path, &report), SUBETHA_OK);
    CHECK(report.removed == 1);
    EXPECT_CODE(subetha_hashmap_unlink(map_path, &report), SUBETHA_OK);
    CHECK(report.removed == 1);
}

int subetha_ctest_run(const char *scratch_prefix)
{
    failures = 0;
    test_version_and_init();
    test_codes_and_handles();
    test_anon_ring();
    test_managed_ring_morphs();
    test_file_ring_two_handles(scratch_prefix);
    test_ring_last_holder(scratch_prefix);
    test_shm_ring();
    test_frames_on_the_ring();
    test_spsc_ring();
    test_spsc_file_two_handles(scratch_prefix);
    test_mpsc_pool(scratch_prefix);
    test_mpmc_grid(scratch_prefix);
    test_vyukov_ring(scratch_prefix);
    test_lamport_pair(scratch_prefix);
    test_broadcast_ring(scratch_prefix);
    test_pubsub_ring(scratch_prefix);
    test_stamped_ring_and_contract();
    test_capacity_ring(scratch_prefix);
    test_locale_ring(scratch_prefix);
    test_capacity_broadcast(scratch_prefix);
    test_capacity_pubsub(scratch_prefix);
    test_ordered_receiver();
    test_shared_stack(scratch_prefix);
    test_work_deque(scratch_prefix);
    test_notifier(scratch_prefix);
    test_shared_hashmap(scratch_prefix);
    test_string_arena(scratch_prefix);
    test_shared_vec(scratch_prefix);
    test_shared_slab(scratch_prefix);
    test_shared_region(scratch_prefix);
    test_shared_atomics(scratch_prefix);
    test_shared_list(scratch_prefix);
    test_batch_entry_points(scratch_prefix);
    test_shared_btree(scratch_prefix);
    test_shared_cell(scratch_prefix);
    test_frame_region(scratch_prefix);
    test_epoch_table(scratch_prefix);
    test_rwlock(scratch_prefix);
    test_semaphore(scratch_prefix);
    test_named_releases(scratch_prefix);
    test_fence_clock(scratch_prefix);
    test_epoch_barrier(scratch_prefix);
    test_shared_arc(scratch_prefix);
    test_waker(scratch_prefix);
    test_tcp_bridge(scratch_prefix);
    test_blocking_tcp_bridge(scratch_prefix);
    test_sens(scratch_prefix);
    test_sens_standalone_codes(scratch_prefix);
    test_endpoint_registry(scratch_prefix);
    test_qos_policy(scratch_prefix);
    test_bit_vec(scratch_prefix);
    test_hyper_log_log(scratch_prefix);
    test_nan_value(scratch_prefix);
    test_lru_cache(scratch_prefix);
    test_graph(scratch_prefix);
    test_time_point(scratch_prefix);
    test_umbra_pointer(scratch_prefix);
    test_universal(scratch_prefix);
    test_k_tower(scratch_prefix);
    test_condvar(scratch_prefix);
    test_owner_lease(scratch_prefix);
    test_leader_election(scratch_prefix);
    test_heartbeat_table(scratch_prefix);
    test_holder_table(scratch_prefix);
    test_batch_shared_state(scratch_prefix);
#if defined(SUBETHA_TEST_HOOKS)
    test_panic_poisons_only_its_handle();
#endif
    test_shutdown_closes_what_was_left();
    return failures;
}

int subetha_ctest_peer(const char *prefix, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_ring_open(prefix, 1, 1, 64, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint32_t cid = 0;
    rc = subetha_ring_register_consumer(h, &cid);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  peer: register_consumer -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        return 1;
    }
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_ring_pop_wait(h, cid, out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  peer: item %u: pop_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        char want[32];
        int wlen = snprintf(want, sizeof want, "%u", (unsigned)i);
        /* The slot is zero past the payload, so the decimal string ends
         * where the zero begins. */
        if (len != SUBETHA_RING_SLOT_BYTES || memcmp(out, want, (size_t)wlen) != 0 ||
            out[wlen] != 0) {
            fprintf(stderr, "  peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Every item a peer expects carries its index as a decimal string; the slot
 * is zero past it. */
static int slot_is_index(const uint8_t *out, size_t len, uint32_t i)
{
    char want[32];
    int wlen = snprintf(want, sizeof want, "%u", (unsigned)i);
    return len == SUBETHA_RING_SLOT_BYTES && memcmp(out, want, (size_t)wlen) == 0 && out[wlen] == 0;
}

int subetha_ctest_peer_spsc(const char *base, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_spsc_open(base, 64, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  spsc peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_spsc_pop_wait(h, out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  spsc peer: item %u: pop_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        if (!slot_is_index(out, len, i)) {
            fprintf(stderr, "  spsc peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Plays producer 1 of a two-producer pool the other process created and
 * drains: pushes `expect` items, parking when its ring is full. */
int subetha_ctest_peer_mpsc(const char *prefix, uint32_t expect)
{
    int problems = 0;
    subetha_handle producers[2] = {0, 0};
    subetha_handle consumer = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_mpsc_open_pool(prefix, 2, 64, SUBETHA_MODE_STRICT, producers, &consumer);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  mpsc peer: open_pool -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        char item[32];
        int n = snprintf(item, sizeof item, "%u", (unsigned)i);
        rc = subetha_mpsc_push_wait(producers[1], (const uint8_t *)item, (size_t)n, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  mpsc peer: item %u: push_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(producers[0]) != SUBETHA_OK || subetha_handle_destroy(producers[1]) != SUBETHA_OK ||
        subetha_handle_destroy(consumer) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Plays consumer 1 and producer 0 of a 2x2 grid the other process created:
 * first drains `expect` items the other process pushes on producer 1 (ring
 * 1 is consumer 1's), then pushes `expect` items on producer 0 for the
 * other process's consumer 0. The phases are ordered against the other
 * side's so neither waits on a ring nobody drains. */
int subetha_ctest_peer_mpmc(const char *prefix, uint32_t expect)
{
    int problems = 0;
    subetha_handle producers[2] = {0, 0};
    subetha_handle consumers[2] = {0, 0};
    int32_t rc = subetha_mpmc_open_grid(prefix, 2, 2, 64, SUBETHA_MODE_STRICT, producers, consumers);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  mpmc peer: open_grid -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_mpmc_pop_wait(consumers[1], out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  mpmc peer: item %u: pop_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        if (!slot_is_index(out, len, i)) {
            fprintf(stderr, "  mpmc peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        }
    }
    for (uint32_t i = 0; i < expect; i++) {
        char item[32];
        int n = snprintf(item, sizeof item, "%u", (unsigned)i);
        rc = subetha_mpmc_push_wait(producers[0], (const uint8_t *)item, (size_t)n, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  mpmc peer: item %u: push_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
    }
    for (int k = 0; k < 2; k++) {
        if (subetha_handle_destroy(producers[k]) != SUBETHA_OK || subetha_handle_destroy(consumers[k]) != SUBETHA_OK) {
            problems++;
        }
    }
    return problems;
}

/* Produces `expect` indexed items into the Vyukov ring the other process
 * created at `path` and drains. */
int subetha_ctest_peer_vyukov(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_vyukov_open(path, 64, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  vyukov peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        char item[32];
        int n = snprintf(item, sizeof item, "%u", (unsigned)i);
        rc = subetha_vyukov_push_wait(h, (const uint8_t *)item, (size_t)n, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  vyukov peer: item %u: push_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Registers as a consumer of the broadcast ring at `path` and reads
 * `expect` indexed items in order; the other process waits for the
 * registration before it pushes. */
int subetha_ctest_peer_broadcast(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_broadcast_open(path, 64, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  broadcast peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint32_t consumer = 0;
    rc = subetha_broadcast_register_consumer(h, &consumer);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  broadcast peer: register -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        return 1;
    }
    uint8_t out[SUBETHA_BROADCAST_PAYLOAD_BYTES];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_broadcast_recv_wait(h, consumer, out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  broadcast peer: item %u: recv_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        char want[32];
        int wlen = snprintf(want, sizeof want, "%u", (unsigned)i);
        if (len != SUBETHA_BROADCAST_PAYLOAD_BYTES || memcmp(out, want, (size_t)wlen) != 0 || out[wlen] != 0) {
            fprintf(stderr, "  broadcast peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        }
    }
    if (subetha_broadcast_unregister_consumer(h, consumer) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Attaches to the capacity ring the other process created under `base`,
 * registers as its consumer, and drains `expect` indexed items. */
int subetha_ctest_peer_capacity(const char *base, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_capacity_open(base, 1, 1, 64, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  capacity peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint32_t cid = 0;
    rc = subetha_capacity_register_consumer(h, &cid);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  capacity peer: register_consumer -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        return 1;
    }
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_capacity_pop_wait(h, cid, out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  capacity peer: item %u: pop_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        if (!slot_is_index(out, len, i)) {
            fprintf(stderr, "  capacity peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Attaches to the locale ring the other process created under `base` and
 * moved to the file locale, registers as its consumer, and drains `expect`
 * indexed items from there. */
int subetha_ctest_peer_locale(const char *base, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_locale_ring_open(base, 1, 1, 64, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  locale peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    subetha_locale_stats stats;
    rc = subetha_locale_ring_read_stats(h, &stats);
    if (rc != SUBETHA_OK || stats.current_locale != SUBETHA_LOCALE_FILE) {
        fprintf(stderr, "  locale peer: the ring is not in the file locale (rc %d, locale %u)\n", (int)rc,
                (unsigned)stats.current_locale);
        problems++;
    }
    uint32_t cid = 0;
    rc = subetha_locale_ring_register_consumer(h, &cid);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  locale peer: register_consumer -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        return 1;
    }
    uint8_t out[SUBETHA_RING_SLOT_BYTES];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_locale_ring_pop_wait(h, cid, out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  locale peer: item %u: pop_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        if (!slot_is_index(out, len, i)) {
            fprintf(stderr, "  locale peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Attaches to the ring at `prefix` as a producer and pushes `count`
 * indexed items, waiting for room; the other process watches a notifier
 * on the ring. */
int subetha_ctest_peer_notify(const char *prefix, uint32_t count)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_ring_open(prefix, 1, 1, 64, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  notify peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint32_t pid = 0;
    rc = subetha_ring_register_producer(h, &pid);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  notify peer: register_producer -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        return 1;
    }
    for (uint32_t i = 0; i < count; i++) {
        char item[32];
        int n = snprintf(item, sizeof item, "%u", (unsigned)i);
        rc = subetha_ring_push_wait(h, pid, (const uint8_t *)item, (size_t)n, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  notify peer: item %u: push_wait -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
    }
    if (subetha_ring_unregister_producer(h, pid) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the map at `path` (16-byte keys holding a decimal index, 8-byte
 * values holding the index doubled), checks the `expect` entries the
 * other process inserted, and inserts `expect` entries of its own at
 * indexes `expect` and up for the other process to check. */
int subetha_ctest_peer_hashmap(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_hashmap_open(path, 4096, 16, 8, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  hashmap peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint8_t key[16];
        memset(key, 0, sizeof key);
        snprintf((char *)key, sizeof key, "%u", (unsigned)i);
        uint8_t value[8];
        size_t len = 0;
        rc = subetha_hashmap_get(h, key, sizeof key, value, sizeof value, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  hashmap peer: key %u: get -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        uint64_t got = 0;
        for (int b = 0; b < 8; b++) {
            got |= (uint64_t)value[b] << (8 * b);
        }
        if (got != (uint64_t)i * 2) {
            fprintf(stderr, "  hashmap peer: key %u holds %llu\n", (unsigned)i, (unsigned long long)got);
            problems++;
        }
    }
    for (uint32_t i = expect; i < 2 * expect; i++) {
        uint8_t key[16];
        memset(key, 0, sizeof key);
        snprintf((char *)key, sizeof key, "%u", (unsigned)i);
        uint8_t value[8];
        uint64_t v = (uint64_t)i * 2;
        for (int b = 0; b < 8; b++) {
            value[b] = (uint8_t)(v >> (8 * b));
        }
        rc = subetha_hashmap_insert(h, key, sizeof key, value, sizeof value, NULL);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  hashmap peer: key %u: insert -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the arena at `path`, resolves the `expect` values the other
 * process interned first ("item-<i>", each at the offset the ones before
 * it add up to), and interns `expect` values of its own ("peer-<i>") for
 * the other process to resolve the same way. */
int subetha_ctest_peer_arena(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_arena_open(path, 1u << 20, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  arena peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint64_t offset = 0;
    for (uint32_t i = 0; i < expect; i++) {
        char want[32];
        int wlen = snprintf(want, sizeof want, "item-%u", (unsigned)i);
        uint64_t ref = 0;
        rc = subetha_arena_ref_pack(offset, (uint32_t)wlen, &ref);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  arena peer: value %u: pack -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        uint8_t out[32];
        size_t len = 0;
        rc = subetha_arena_get(h, ref, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  arena peer: value %u: get -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        if (len != (size_t)wlen || memcmp(out, want, len) != 0) {
            fprintf(stderr, "  arena peer: value %u holds %zu bytes, not \"%s\"\n", (unsigned)i, len, want);
            problems++;
        }
        offset += (uint64_t)wlen;
    }
    for (uint32_t i = 0; i < expect; i++) {
        char text[32];
        int tlen = snprintf(text, sizeof text, "peer-%u", (unsigned)i);
        uint64_t ref = 0;
        rc = subetha_arena_intern(h, (const uint8_t *)text, (size_t)tlen, &ref);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  arena peer: value %u: intern -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        if (subetha_arena_ref_offset(ref) != offset || subetha_arena_ref_len(ref) != (uint32_t)tlen) {
            fprintf(stderr, "  arena peer: value %u landed at %llu, not %llu\n", (unsigned)i,
                    (unsigned long long)subetha_arena_ref_offset(ref), (unsigned long long)offset);
            problems++;
        }
        offset += (uint64_t)tlen;
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Whether a sixteen-byte element holds the decimal index `i` and zeros
 * past it. */
static int sixteen_holds_index(const uint8_t *out, size_t len, uint32_t i)
{
    char want[32];
    int wlen = snprintf(want, sizeof want, "%u", (unsigned)i);
    return len == 16 && memcmp(out, want, (size_t)wlen) == 0 && out[wlen] == 0;
}

/* A sixteen-byte element holding the decimal index `i`, zero past it. */
static void sixteen_of_index(uint8_t *element, uint32_t i)
{
    memset(element, 0, 16);
    snprintf((char *)element, 16, "%u", (unsigned)i);
}

/* Opens the vec at `path` (sixteen-byte elements holding a decimal
 * index), checks the `expect` elements the other process pushed, and
 * pushes `expect` of its own, each landing at the index it names. */
int subetha_ctest_peer_vec(const char *path, uint32_t expect)
{
    int problems = 0;
    const subetha_element_layout layout = {16, 1, 0x494e4445583136ULL};
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_vec_open(path, 2 * expect, &layout, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  vec peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint8_t out[16];
        size_t len = 0;
        rc = subetha_vec_get(h, i, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  vec peer: element %u: get -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        if (!sixteen_holds_index(out, len, i)) {
            fprintf(stderr, "  vec peer: element %u does not hold its index\n", (unsigned)i);
            problems++;
        }
    }
    for (uint32_t i = expect; i < 2 * expect; i++) {
        uint8_t element[16];
        uint64_t index = 0;
        sixteen_of_index(element, i);
        rc = subetha_vec_push_back(h, element, sizeof element, &index);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  vec peer: element %u: push -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        if (index != i) {
            fprintf(stderr, "  vec peer: element %u landed at %llu\n", (unsigned)i, (unsigned long long)index);
            problems++;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the slab at `path` (sixteen-byte records holding a decimal
 * index), checks the records at the first `expect` slots the other
 * process wrote, and writes the next `expect` slots with their own
 * indexes for the other process to check. */
int subetha_ctest_peer_slab(const char *path, uint32_t expect)
{
    int problems = 0;
    const subetha_element_layout layout = {16, 1, 0x494e4445583136ULL};
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_slab_open(path, 2 * expect, &layout, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  slab peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint8_t out[16];
        size_t len = 0;
        rc = subetha_slab_get(h, i, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  slab peer: slot %u: get -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        if (!sixteen_holds_index(out, len, i)) {
            fprintf(stderr, "  slab peer: slot %u does not hold its index\n", (unsigned)i);
            problems++;
        }
    }
    for (uint32_t i = expect; i < 2 * expect; i++) {
        uint8_t record[16];
        sixteen_of_index(record, i);
        rc = subetha_slab_set(h, i, record, sizeof record);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  slab peer: slot %u: set -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the list at `path` (sixteen-byte values holding a decimal index),
 * walks the `expect` nodes the other process pushed and checks their
 * order, pops them all, and pushes `expect` of its own for the other
 * process to check. The two processes take turns: this one runs only
 * after the other has stopped writing. */
int subetha_ctest_peer_list(const char *path, uint32_t expect)
{
    int problems = 0;
    const subetha_element_layout layout = {16, 4, 0x494e4445583136ULL};
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_list_open(path, expect + 1, &layout, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  list peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint32_t at = SUBETHA_LIST_HEAD_INDEX, seen = 0;
    rc = subetha_list_first(h, &at);
    while (rc == SUBETHA_OK && at != SUBETHA_LIST_HEAD_INDEX) {
        uint8_t out[16];
        size_t len = 0;
        rc = subetha_list_get(h, at, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  list peer: node %u: get -> %d (%s)\n", (unsigned)seen, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        if (!sixteen_holds_index(out, len, seen)) {
            fprintf(stderr, "  list peer: node %u is out of order\n", (unsigned)seen);
            problems++;
        }
        seen++;
        rc = subetha_list_next(h, at, &at);
    }
    if (seen != expect) {
        fprintf(stderr, "  list peer: walked %u nodes, not %u\n", (unsigned)seen, (unsigned)expect);
        problems++;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint8_t out[16];
        size_t len = 0;
        rc = subetha_list_pop_front(h, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  list peer: pop %u -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    for (uint32_t i = expect; i < 2 * expect; i++) {
        uint8_t value[16];
        uint32_t index = 0;
        sixteen_of_index(value, i);
        rc = subetha_list_push_back(h, value, sizeof value, &index);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  list peer: push %u -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the eight-byte cell at `path`, waits for the other process to
 * publish each of `expect` values, and writes each one back with its
 * bytes reversed. The two take turns through the version: the writer's
 * value is even-numbered, the reply odd, so neither reads a value it
 * wrote itself. */
int subetha_ctest_peer_cell(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_cell_open(path, 8, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  cell peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        /* A write is two version steps, so a round of two writes is four,
         * and the creator's value lands on the second of this round's. */
        uint32_t want = i * 4 + 2;
        uint32_t version = 0;
        uint8_t value[8];
        size_t len = 0;
        /* Wait for the other process's write to land. */
        for (;;) {
            rc = subetha_cell_version(h, &version);
            if (rc != SUBETHA_OK) {
                fprintf(stderr, "  cell peer: version -> %d (%s)\n", (int)rc, subetha_strerror(rc));
                problems++;
                break;
            }
            if (version >= want) {
                break;
            }
            sleep_us(200);
        }
        if (problems > 0) {
            break;
        }
        rc = subetha_cell_get(h, value, sizeof value, &len);
        if (rc != SUBETHA_OK || len != 8) {
            fprintf(stderr, "  cell peer: round %u: get -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        uint8_t reply[8];
        for (int b = 0; b < 8; b++) {
            reply[b] = value[7 - b];
        }
        rc = subetha_cell_set(h, reply, sizeof reply);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  cell peer: round %u: set -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the B-tree at `path` (four-byte big-endian keys, eight-byte
 * values holding the key doubled), checks the `expect` entries the other
 * process inserted and their order, removes every other one, and inserts
 * `expect` more above them. The tree has one writer, so this runs only
 * after the other process has stopped writing. */
int subetha_ctest_peer_btree(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_btree_open(path, 512, 4, 8, 0x494e4445583332ULL, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  btree peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint8_t k[4], out[8];
        size_t len = 0;
        btree_key(k, i);
        rc = subetha_btree_get(h, k, sizeof k, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  btree peer: key %u: get -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        uint64_t got = 0;
        for (int b = 0; b < 8; b++) {
            got |= (uint64_t)out[b] << (8 * b);
        }
        if (got != (uint64_t)i * 2) {
            fprintf(stderr, "  btree peer: key %u holds %llu\n", (unsigned)i, (unsigned long long)got);
            problems++;
        }
    }
    /* The smallest key is the first one the other process inserted. */
    uint8_t first_key[4], first_value[8], want[4];
    size_t first_key_len = 0, first_value_len = 0;
    rc = subetha_btree_first(h, first_key, sizeof first_key, &first_key_len, first_value, sizeof first_value,
                             &first_value_len);
    btree_key(want, 0);
    if (rc != SUBETHA_OK || first_key_len != 4 || memcmp(first_key, want, 4) != 0) {
        fprintf(stderr, "  btree peer: first -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        problems++;
    }
    for (uint32_t i = 0; i < expect; i += 2) {
        uint8_t k[4], out[8];
        size_t len = 0;
        btree_key(k, i);
        rc = subetha_btree_remove(h, k, sizeof k, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  btree peer: key %u: remove -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    for (uint32_t i = expect; i < 2 * expect; i++) {
        uint8_t k[4], v[8];
        uint64_t doubled = (uint64_t)i * 2;
        btree_key(k, i);
        for (int b = 0; b < 8; b++) {
            v[b] = (uint8_t)(doubled >> (8 * b));
        }
        rc = subetha_btree_insert(h, k, sizeof k, v, sizeof v, NULL, 0, NULL, NULL);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  btree peer: key %u: insert -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Adds `expect` to the 64-bit counter at `<path>.u64` one at a time, then
 * raises the flag at `<path>.flag` so the other process knows the count
 * is in. */
int subetha_ctest_peer_atomic(const char *path, uint32_t expect)
{
    int problems = 0;
    char counter_path[1024], flag_path[1024];
    snprintf(counter_path, sizeof counter_path, "%s.u64", path);
    snprintf(flag_path, sizeof flag_path, "%s.flag", path);
    subetha_handle counter = SUBETHA_HANDLE_NONE, flag = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_atomic_u64_open(counter_path, SUBETHA_MODE_STRICT, &counter);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  atomic peer: counter open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    rc = subetha_atomic_bool_open(flag_path, SUBETHA_MODE_STRICT, &flag);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  atomic peer: flag open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        problems++;
    } else {
        for (uint32_t i = 0; i < expect; i++) {
            rc = subetha_atomic_u64_fetch_add_explicit(counter, 1, SUBETHA_ORDER_RELAXED, NULL);
            if (rc != SUBETHA_OK) {
                fprintf(stderr, "  atomic peer: add %u -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
                problems++;
                break;
            }
        }
        /* Release, so the count is visible to whoever acquires the flag. */
        rc = subetha_atomic_bool_store_explicit(flag, true, SUBETHA_ORDER_RELEASE);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  atomic peer: flag store -> %d (%s)\n", (int)rc, subetha_strerror(rc));
            problems++;
        }
        if (subetha_handle_destroy(flag) != SUBETHA_OK) {
            problems++;
        }
    }
    if (subetha_handle_destroy(counter) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the region at `path` (sixteen-byte elements holding a decimal
 * index), checks the `expect` slots the other process allocated, and
 * allocates `expect` of its own, each landing at the index it names. */
int subetha_ctest_peer_region(const char *path, uint32_t expect)
{
    int problems = 0;
    const subetha_element_layout layout = {16, 1, 0x494e4445583136ULL};
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_region_open(path, 2 * expect, &layout, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  region peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint8_t out[16];
        size_t len = 0;
        rc = subetha_region_get(h, i, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  region peer: slot %u: get -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        if (!sixteen_holds_index(out, len, i)) {
            fprintf(stderr, "  region peer: slot %u does not hold its index\n", (unsigned)i);
            problems++;
        }
    }
    for (uint32_t i = expect; i < 2 * expect; i++) {
        uint8_t element[16];
        uint32_t index = SUBETHA_REGION_NIL_INDEX;
        sixteen_of_index(element, i);
        rc = subetha_region_allocate(h, element, sizeof element, &index);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  region peer: slot %u: allocate -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
        if (index != i) {
            fprintf(stderr, "  region peer: slot %u landed at %u\n", (unsigned)i, (unsigned)index);
            problems++;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* The layout the stack and deque peers share with the Rust side: a
 * sixteen-byte element holding a decimal index, zero past it. */
static const subetha_element_layout index_layout = {16, 1, 0x494e4445583136ULL};

/* Whether a sixteen-byte element holds the decimal index `i`. */
static int element_is_index(const uint8_t *out, size_t len, uint32_t i)
{
    char want[32];
    int wlen = snprintf(want, sizeof want, "%u", (unsigned)i);
    return len == 16 && memcmp(out, want, (size_t)wlen) == 0 && out[wlen] == 0;
}

/* The decimal index a sixteen-byte element holds, or UINT32_MAX when it
 * holds something else. */
static uint32_t element_index(const uint8_t *out, size_t len)
{
    char text[17];
    if (len != 16) {
        return UINT32_MAX;
    }
    memcpy(text, out, 16);
    text[16] = 0;
    char *end = NULL;
    unsigned long idx = strtoul(text, &end, 10);
    if (end == text || *end != 0 || idx >= UINT32_MAX) {
        return UINT32_MAX;
    }
    return (uint32_t)idx;
}

/* Attaches to the stack at `path` and pops `expect` elements, each a
 * decimal index below `expect` and none twice; the order is the stack's. */
int subetha_ctest_peer_stack(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_stack_create(path, 64, &index_layout, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  stack peer: create -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint8_t *seen = calloc(expect, 1);
    if (seen == NULL) {
        fprintf(stderr, "  stack peer: no memory for %u flags\n", (unsigned)expect);
        subetha_handle_destroy(h);
        return 1;
    }
    uint8_t out[16];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_stack_pop_wait(h, out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  stack peer: item %u: pop_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        uint32_t idx = element_index(out, len);
        if (idx >= expect || seen[idx]) {
            fprintf(stderr, "  stack peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        } else {
            seen[idx] = 1;
        }
    }
    free(seen);
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the deque at `path` as a thief and steals `expect` elements, which
 * arrive in the order the owner pushed them. */
int subetha_ctest_peer_deque(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_deque_open_thief(path, &index_layout, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  deque peer: open_thief -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint8_t out[16];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_deque_steal_wait(h, out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  deque peer: item %u: steal_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        if (!element_is_index(out, len, i)) {
            fprintf(stderr, "  deque peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        }
    }
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Subscribes to the pub/sub ring at `path` with a position kept in
 * `<path>.pos`, created at zero, and reads `expect` indexed items; the
 * position file is left for the other process to inspect. */
int subetha_ctest_peer_pubsub(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_pubsub_open(path, 1024, &strict_options, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  pubsub peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    char pos_path[1024];
    snprintf(pos_path, sizeof pos_path, "%s.pos", path);
    subetha_handle sub = SUBETHA_HANDLE_NONE;
    rc = subetha_pubsub_subscribe_file(h, pos_path, 0, true, SUBETHA_MODE_STRICT, &sub);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  pubsub peer: subscribe_file -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint8_t out[SUBETHA_PUBSUB_PAYLOAD_BYTES];
    for (uint32_t i = 0; i < expect; i++) {
        size_t len = 0;
        rc = subetha_subscriber_next_wait(sub, out, sizeof out, &len, 10000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  pubsub peer: item %u: next_wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        char want[32];
        int wlen = snprintf(want, sizeof want, "%u", (unsigned)i);
        if (len != SUBETHA_PUBSUB_PAYLOAD_BYTES || memcmp(out, want, (size_t)wlen) != 0 || out[wlen] != 0) {
            fprintf(stderr, "  pubsub peer: item %u: got %.*s\n", (unsigned)i, (int)len, (const char *)out);
            problems++;
        }
    }
    if (subetha_handle_destroy(sub) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* The two processes sharing an epoch table take turns through a
 * step number in a shared cell beside it, because neither can see the
 * other's progress in the table itself: an open ticket holds the
 * published epoch down, so polling that would deadlock against the very
 * thing under test. */
static void epochs_step_set(subetha_handle cell, uint64_t step, const char *who)
{
    uint8_t bytes[8];
    for (int i = 0; i < 8; i++) {
        bytes[i] = (uint8_t)(step >> (8 * i));
    }
    int32_t rc = subetha_cell_set(cell, bytes, sizeof bytes);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  %s: step %u: set -> %d (%s)\n", who, (unsigned)step, (int)rc, subetha_strerror(rc));
    }
}

/* Waits for the other side to reach `step`. Returns 0 once it has and 1
 * if the cell stopped answering, so a caller can give up rather than
 * spin against a peer that died. */
static int epochs_step_wait(subetha_handle cell, uint64_t step, const char *who)
{
    for (uint32_t tries = 0; tries < 100000; tries++) {
        uint8_t bytes[8];
        size_t len = 0;
        int32_t rc = subetha_cell_get(cell, bytes, sizeof bytes, &len);
        if (rc != SUBETHA_OK || len != 8) {
            fprintf(stderr, "  %s: waiting for step %u: get -> %d (%s)\n", who, (unsigned)step, (int)rc,
                    subetha_strerror(rc));
            return 1;
        }
        uint64_t seen = 0;
        for (int i = 0; i < 8; i++) {
            seen |= (uint64_t)bytes[i] << (8 * i);
        }
        if (seen >= step) {
            return 0;
        }
        sleep_us(200);
    }
    fprintf(stderr, "  %s: step %u never arrived\n", who, (unsigned)step);
    return 1;
}

/* Opens the epoch table at `path` and plays the far side of the two
 * properties that only a second process can show: a pin taken here holds
 * the reclaim horizon the other process computes, and a ticket open here
 * holds the epoch the other process reads as published. The step cell at
 * `<path>.step` sequences the two. */
int subetha_ctest_peer_epochs(const char *path, uint32_t expect)
{
    int problems = 0;
    char step_path[1024];
    snprintf(step_path, sizeof step_path, "%s.step", path);
    subetha_handle h = SUBETHA_HANDLE_NONE, cell = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_epochs_open(path, 4, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  epochs peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    rc = subetha_cell_open(step_path, 8, SUBETHA_MODE_STRICT, &cell);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  epochs peer: step cell open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(h);
        return 1;
    }

    /* The pin half: take one, hold it while the other process advances
     * `expect` times, then let it go. */
    uint64_t pin = 0;
    if (epochs_step_wait(cell, 1, "epochs peer") != 0) {
        problems++;
    } else if ((rc = subetha_epochs_pin(h, &pin)) != SUBETHA_OK) {
        fprintf(stderr, "  epochs peer: pin -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        problems++;
    } else {
        epochs_step_set(cell, 2, "epochs peer");
        if (epochs_step_wait(cell, 3, "epochs peer") != 0) {
            problems++;
        }
        if ((rc = subetha_pin_release(h, pin)) != SUBETHA_OK) {
            fprintf(stderr, "  epochs peer: pin release -> %d (%s)\n", (int)rc, subetha_strerror(rc));
            problems++;
        }
        epochs_step_set(cell, 4, "epochs peer");
    }

    /* The ticket half: open one, hold it while the other process
     * advances `expect` times and finds its published epoch unmoved,
     * then publish. */
    uint64_t ticket = 0;
    if (problems == 0) {
        if (epochs_step_wait(cell, 5, "epochs peer") != 0) {
            problems++;
        } else if ((rc = subetha_epochs_begin(h, &ticket)) != SUBETHA_OK) {
            fprintf(stderr, "  epochs peer: begin -> %d (%s)\n", (int)rc, subetha_strerror(rc));
            problems++;
        } else {
            epochs_step_set(cell, 6, "epochs peer");
            if (epochs_step_wait(cell, 7, "epochs peer") != 0) {
                problems++;
            }
            if ((rc = subetha_ticket_publish(h, ticket)) != SUBETHA_OK) {
                fprintf(stderr, "  epochs peer: publish -> %d (%s)\n", (int)rc, subetha_strerror(rc));
                problems++;
            }
            epochs_step_set(cell, 8, "epochs peer");
        }
    }
    (void)expect;

    if (subetha_handle_destroy(cell) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Plays a worker that joins a fleet and then dies mid-job: it registers
 * in the heartbeat table at `<path>.hb`, takes leadership of the election
 * at `<path>.elect`, marks `expect` work units in flight, beats once, and
 * exits without unregistering or stepping down.
 *
 * That is the state a supervisor has to recover from, and it needs a
 * second process: within one, the worker is alive by construction, so
 * neither the grace window nor the abandoned in-flight bits ever have a
 * genuinely departed process to describe. */
int subetha_ctest_peer_fleet(const char *path, uint32_t expect)
{
    int problems = 0;
    char hb_path[1024], elect_path[1024];
    snprintf(hb_path, sizeof hb_path, "%s.hb", path);
    snprintf(elect_path, sizeof elect_path, "%s.elect", path);
    subetha_handle hb = SUBETHA_HANDLE_NONE, elect = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_heartbeat_open(hb_path, 4, SUBETHA_MODE_STRICT, &hb);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  fleet peer: heartbeat open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    rc = subetha_leader_open(elect_path, SUBETHA_MODE_STRICT, &elect);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  fleet peer: election open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(hb);
        return 1;
    }
    uint32_t mine = 0;
    rc = subetha_current_pid(&mine);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  fleet peer: pid -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        problems++;
    }
    uint32_t slot = 0;
    rc = subetha_heartbeat_register(hb, mine, &slot);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  fleet peer: register -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        problems++;
    } else {
        for (uint32_t i = 0; i < expect && i < SUBETHA_HEARTBEAT_IN_FLIGHT; i++) {
            rc = subetha_heartbeat_mark_in_flight(hb, slot, i);
            if (rc != SUBETHA_OK) {
                fprintf(stderr, "  fleet peer: mark %u -> %d (%s)\n", (unsigned)i, (int)rc, subetha_strerror(rc));
                problems++;
                break;
            }
        }
        rc = subetha_heartbeat_beat(hb, slot);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  fleet peer: beat -> %d (%s)\n", (int)rc, subetha_strerror(rc));
            problems++;
        }
    }
    bool got = false;
    rc = subetha_leader_try_claim(elect, mine, 2, &got);
    if (rc != SUBETHA_OK || !got) {
        fprintf(stderr, "  fleet peer: claim -> %d (%s), got=%d\n", (int)rc, subetha_strerror(rc), (int)got);
        report_detail();
        problems++;
    }
    /* The handles are destroyed so shutdown is clean, but the slot and
     * the leadership are deliberately left held: this process is about
     * to go, and that is the state under test. */
    if (subetha_handle_destroy(elect) != SUBETHA_OK || subetha_handle_destroy(hb) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the lease at `path`, takes it under this process's own pid, and
 * exits still holding it. Nothing releases it and nothing beats for it
 * afterwards, which is the failure a lock cannot survive and a lease is
 * built for: the creator ticks the epoch past the grace window and takes
 * it back.
 *
 * `expect` is the payload byte to leave behind, so the creator can check
 * that what a dead holder wrote is still there when the lease changes
 * hands. */
int subetha_ctest_peer_owner_lease(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_owner_lease_open(path, 8, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  lease peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint32_t mine = 0;
    rc = subetha_current_pid(&mine);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  lease peer: pid -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        problems++;
    }
    bool got = false;
    rc = subetha_owner_lease_try_acquire(h, mine, 2, &got);
    if (rc != SUBETHA_OK || !got) {
        fprintf(stderr, "  lease peer: acquire -> %d (%s), got=%d\n", (int)rc, subetha_strerror(rc), (int)got);
        report_detail();
        problems++;
    } else {
        uint8_t mark[8];
        memset(mark, (uint8_t)expect, sizeof mark);
        rc = subetha_owner_lease_write(h, mine, mark, sizeof mark);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  lease peer: write -> %d (%s)\n", (int)rc, subetha_strerror(rc));
            problems++;
        }
    }
    /* The handle is destroyed so shutdown is clean, but the lease itself
     * is deliberately left held: this process is about to go, and that
     * is the state under test. */
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the condition variable at `path` and plays the responder half of
 * a ping-pong against the shared counter at `<path>.turn`: it waits for
 * the counter to reach an odd number, raises it to the next even one and
 * notifies back, `expect` times.
 *
 * The generation is read before the counter is checked, and that reading
 * is what the wait is given, so a notify that lands in between is not
 * lost. A run that finishes at all is the evidence: with the window open
 * the two processes deadlock, each parked on a signal the other already
 * sent. */
int subetha_ctest_peer_condvar(const char *path, uint32_t expect)
{
    int problems = 0;
    char turn_path[1024];
    snprintf(turn_path, sizeof turn_path, "%s.turn", path);
    subetha_handle h = SUBETHA_HANDLE_NONE, turn = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_condvar_open(path, 8, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  condvar peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    rc = subetha_atomic_u32_open(turn_path, SUBETHA_MODE_STRICT, &turn);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  condvar peer: turn open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(h);
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint32_t want = i * 2 + 1;
        for (;;) {
            uint64_t seen = 0;
            rc = subetha_condvar_generation(h, &seen);
            if (rc != SUBETHA_OK) {
                fprintf(stderr, "  condvar peer: round %u: generation -> %d (%s)\n", (unsigned)i, (int)rc,
                        subetha_strerror(rc));
                problems++;
                break;
            }
            uint32_t at = 0;
            rc = subetha_atomic_u32_load(turn, &at);
            if (rc != SUBETHA_OK) {
                fprintf(stderr, "  condvar peer: round %u: turn load -> %d (%s)\n", (unsigned)i, (int)rc,
                        subetha_strerror(rc));
                problems++;
                break;
            }
            if (at >= want) {
                break;
            }
            rc = subetha_condvar_wait(h, seen, 30000);
            if (rc != SUBETHA_OK && rc != SUBETHA_E_TIMEOUT) {
                fprintf(stderr, "  condvar peer: round %u: wait -> %d (%s)\n", (unsigned)i, (int)rc,
                        subetha_strerror(rc));
                report_detail();
                problems++;
                break;
            }
        }
        if (problems > 0) {
            break;
        }
        rc = subetha_atomic_u32_store(turn, want + 1);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  condvar peer: round %u: turn store -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            break;
        }
        rc = subetha_condvar_notify_all(h, NULL);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  condvar peer: round %u: notify -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(turn) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Raises the peak at `peak` to `value` if it stands above it. A plain
 * load-then-store loses a raise from the other process; the loop retries
 * on whatever the compare-exchange found instead. */
static void raise_peak(subetha_handle peak, uint32_t value, const char *who)
{
    uint32_t current = 0;
    int32_t rc = subetha_atomic_u32_load(peak, &current);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  %s: peak load -> %d (%s)\n", who, (int)rc, subetha_strerror(rc));
        return;
    }
    while (value > current) {
        bool swapped = false;
        rc = subetha_atomic_u32_compare_exchange(peak, current, value, &current, &swapped);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  %s: peak swap -> %d (%s)\n", who, (int)rc, subetha_strerror(rc));
            return;
        }
        if (swapped) {
            return;
        }
    }
}

/* Waits at `counter` until both sides have reached it. Returns zero when
 * they met and nonzero when they did not.
 *
 * A peer starts a process spawn behind the side that launched it, so work
 * that is evidence about two processes acting at the same time has to
 * begin after both are there. Thirty seconds is a spawn on a host under
 * load; past it the other side is gone, and failing beats waiting. */
static int meet_at(subetha_handle counter, const char *who)
{
    uint32_t arrived = 0;
    int32_t rc = subetha_atomic_u32_fetch_add(counter, 1, &arrived);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  %s: rendezvous -> %d (%s)\n", who, (int)rc, subetha_strerror(rc));
        return 1;
    }
    for (uint32_t waited = 0; ; waited++) {
        uint32_t here = 0;
        rc = subetha_atomic_u32_load(counter, &here);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  %s: rendezvous read -> %d (%s)\n", who, (int)rc, subetha_strerror(rc));
            return 1;
        }
        if (here >= 2) {
            return 0;
        }
        if (waited >= 30000) {
            fprintf(stderr, "  %s: the other side did not reach the rendezvous\n", who);
            return 1;
        }
        sleep_us(1000);
    }
}

/* Opens the semaphore at `path` (two permits) and takes one `expect`
 * times, counting itself in and out of the shared holder count at
 * `<path>.live` and raising the peak at `<path>.peak` while it holds one.
 *
 * The other process does the same. A peak above two means the semaphore
 * let more processes in than it has permits; a peak of exactly two is
 * also what proves the two ran at the same time, so the bound is evidence
 * rather than an artifact of them never overlapping.
 *
 * Both sides meet at `<path>.ready` before either takes a permit. This
 * side starts a process spawn behind the other, and a side that has not
 * started cannot overlap with anything. */
int subetha_ctest_peer_semaphore(const char *path, uint32_t expect)
{
    int problems = 0;
    char live_path[1024], peak_path[1024], ready_path[1024];
    snprintf(live_path, sizeof live_path, "%s.live", path);
    snprintf(peak_path, sizeof peak_path, "%s.peak", path);
    snprintf(ready_path, sizeof ready_path, "%s.ready", path);
    subetha_handle h = SUBETHA_HANDLE_NONE, live = SUBETHA_HANDLE_NONE, peak = SUBETHA_HANDLE_NONE;
    subetha_handle ready = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_semaphore_open(path, 2, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  semaphore peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    if ((rc = subetha_atomic_u32_open(live_path, SUBETHA_MODE_STRICT, &live)) != SUBETHA_OK
        || (rc = subetha_atomic_u32_open(peak_path, SUBETHA_MODE_STRICT, &peak)) != SUBETHA_OK
        || (rc = subetha_atomic_u32_open(ready_path, SUBETHA_MODE_STRICT, &ready)) != SUBETHA_OK) {
        fprintf(stderr, "  semaphore peer: counter open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(h);
        return 1;
    }
    if (meet_at(ready, "semaphore peer") != 0) {
        problems++;
    }
    for (uint32_t i = 0; i < expect && problems == 0; i++) {
        uint64_t p = 0;
        rc = subetha_semaphore_acquire(h, 30000, &p);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  semaphore peer: round %u: acquire -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        uint32_t before = 0;
        rc = subetha_atomic_u32_fetch_add(live, 1, &before);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  semaphore peer: round %u: count in -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            subetha_semaphore_release(h, p);
            break;
        }
        raise_peak(peak, before + 1, "semaphore peer");
        /* Held long enough that the other process has a real chance to
         * be inside at the same moment. */
        sleep_us(200);
        uint32_t after = 0;
        rc = subetha_atomic_u32_fetch_sub(live, 1, &after);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  semaphore peer: round %u: count out -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            subetha_semaphore_release(h, p);
            break;
        }
        if (subetha_semaphore_release(h, p) != SUBETHA_OK) {
            fprintf(stderr, "  semaphore peer: round %u: the permit did not go back\n", (unsigned)i);
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(ready) != SUBETHA_OK || subetha_handle_destroy(peak) != SUBETHA_OK
        || subetha_handle_destroy(live) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the lock at `path` and takes the write hold `expect` times,
 * incrementing the shared counter at `<path>.count` under each one. The
 * other process does the same, so the counter lands at twice `expect`
 * only if the two never held it at once: the increment is a read, a
 * pause and a write, which loses one of any overlapping pair.
 *
 * The counter is an eight-byte cell, and a torn or lost update shows up
 * as a total below the sum.
 *
 * Both sides meet at `<path>.ready` before the rounds. Two sides that
 * never run at the same time keep the count whatever the lock does, so
 * without the rendezvous a correct total says nothing. */
int subetha_ctest_peer_rwlock(const char *path, uint32_t expect)
{
    int problems = 0;
    char count_path[1024], ready_path[1024];
    snprintf(count_path, sizeof count_path, "%s.count", path);
    snprintf(ready_path, sizeof ready_path, "%s.ready", path);
    subetha_handle h = SUBETHA_HANDLE_NONE, cell = SUBETHA_HANDLE_NONE;
    subetha_handle ready = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_rwlock_open(path, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  rwlock peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    rc = subetha_cell_open(count_path, 8, SUBETHA_MODE_STRICT, &cell);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  rwlock peer: counter open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(h);
        return 1;
    }
    rc = subetha_atomic_u32_open(ready_path, SUBETHA_MODE_STRICT, &ready);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  rwlock peer: rendezvous open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(cell);
        subetha_handle_destroy(h);
        return 1;
    }
    if (meet_at(ready, "rwlock peer") != 0) {
        problems++;
    }
    for (uint32_t i = 0; i < expect && problems == 0; i++) {
        uint64_t w = 0;
        rc = subetha_rwlock_write(h, 30000, &w);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  rwlock peer: round %u: write -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        uint8_t bytes[8];
        size_t len = 0;
        rc = subetha_cell_get(cell, bytes, sizeof bytes, &len);
        if (rc != SUBETHA_OK || len != 8) {
            fprintf(stderr, "  rwlock peer: round %u: get -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            subetha_rwlock_unlock(h, w);
            break;
        }
        uint64_t seen = 0;
        for (int b = 0; b < 8; b++) {
            seen |= (uint64_t)bytes[b] << (8 * b);
        }
        /* The pause is what makes an overlapping pair lose an update, so
         * the counter is evidence about the lock rather than about how
         * fast the two processes happen to run. */
        sleep_us(1);
        seen++;
        for (int b = 0; b < 8; b++) {
            bytes[b] = (uint8_t)(seen >> (8 * b));
        }
        rc = subetha_cell_set(cell, bytes, sizeof bytes);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  rwlock peer: round %u: set -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            subetha_rwlock_unlock(h, w);
            break;
        }
        if (subetha_rwlock_unlock(h, w) != SUBETHA_OK) {
            fprintf(stderr, "  rwlock peer: round %u: the hold did not release\n", (unsigned)i);
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(ready) != SUBETHA_OK || subetha_handle_destroy(cell) != SUBETHA_OK
        || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the waker at `path`, parks at each sequence from 1 to `expect`,
 * and is woken by the other process each time.
 *
 * A park woken by the same process proves only that the state machine
 * works. What this shows is that a thread parked here - down on the
 * platform's own futex, in the kernel - is woken by a call in a
 * different process, which is the only thing the primitive is for.
 *
 * The counter at `<path>.count` is the evidence rather than the wait
 * returning: this side raises it to `i` only after being woken at `i`,
 * and the other side will not wake `i + 1` until it reads `i`. A wait
 * that returned without a wake would run ahead of the counter and the
 * other side would time out.
 */
int subetha_ctest_peer_waker(const char *path, uint32_t expect)
{
    int problems = 0;
    char count_path[1024];
    snprintf(count_path, sizeof count_path, "%s.count", path);
    subetha_handle h = SUBETHA_HANDLE_NONE, count = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_waker_open(path, 8, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  waker peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    if ((rc = subetha_atomic_u32_open(count_path, SUBETHA_MODE_STRICT, &count)) != SUBETHA_OK) {
        fprintf(stderr, "  waker peer: counter open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(h);
        return 1;
    }
    for (uint32_t i = 1; i <= expect; i++) {
        uint64_t park = 0;
        if ((rc = subetha_waker_park(h, i, &park)) != SUBETHA_OK) {
            fprintf(stderr, "  waker peer: round %u: park -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        /* Say the park is up, so the other side knows to wake it. Doing
         * this after the park is what makes the wake land on a parker
         * rather than on an empty table. */
        if ((rc = subetha_atomic_u32_store(count, i * 2 - 1)) != SUBETHA_OK) {
            fprintf(stderr, "  waker peer: round %u: announce -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            subetha_waker_release(h, park);
            break;
        }
        if ((rc = subetha_waker_wait(h, park, 30000)) != SUBETHA_OK) {
            fprintf(stderr, "  waker peer: round %u: wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        /* Woken. Say so, and the other side parks its own next round. */
        if ((rc = subetha_atomic_u32_store(count, i * 2)) != SUBETHA_OK) {
            fprintf(stderr, "  waker peer: round %u: acknowledge -> %d (%s)\n", (unsigned)i,
                    (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(count) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the shared value at `path` (`expect` bytes), takes a holder slot,
 * and hands the two facts back through the atomic at `<path>.turn`.
 *
 * A holder table in one process would count the same whether or not the
 * other side existed, so what this shows is that it does not: the count
 * this side reads includes the creator, and the bytes this side writes are
 * the bytes the creator reads back. Neither is true of two processes each
 * holding their own copy of a file.
 */
int subetha_ctest_peer_shared_arc(const char *path, uint32_t expect)
{
    int problems = 0;
    char turn_path[1024];
    snprintf(turn_path, sizeof turn_path, "%s.turn", path);
    subetha_handle h = SUBETHA_HANDLE_NONE, turn = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_shared_arc_open(path, expect, 4, SUBETHA_ARC_KEEP, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  shared arc peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    if ((rc = subetha_atomic_u32_open(turn_path, SUBETHA_MODE_STRICT, &turn)) != SUBETHA_OK) {
        fprintf(stderr, "  shared arc peer: turn open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(h);
        return 1;
    }

    /* Both sides hold it now, so the creator is counted here. */
    uint64_t count = 0;
    if ((rc = subetha_shared_arc_strong_count(h, &count)) != SUBETHA_OK || count != 2) {
        fprintf(stderr, "  shared arc peer: strong count %llu, wanted 2 (rc %d)\n",
                (unsigned long long)count, (int)rc);
        problems++;
    }

    /* What the creator wrote is here, byte for byte. */
    uint8_t seen[64];
    size_t len = 0;
    if (expect > sizeof seen) {
        fprintf(stderr, "  shared arc peer: %u bytes is past this peer's buffer\n", (unsigned)expect);
        problems++;
    } else if ((rc = subetha_shared_arc_read(h, 0, expect, seen, sizeof seen, &len)) != SUBETHA_OK
               || len != expect) {
        fprintf(stderr, "  shared arc peer: read -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        problems++;
    } else {
        for (uint32_t i = 0; i < expect; i++) {
            if (seen[i] != (uint8_t)(i + 1)) {
                fprintf(stderr, "  shared arc peer: byte %u is %u, wanted %u\n", (unsigned)i,
                        (unsigned)seen[i], (unsigned)(i + 1));
                problems++;
                break;
            }
        }
    }

    /* Write the complement back for the creator to check, then say so. */
    uint8_t reply[64];
    for (uint32_t i = 0; i < expect && i < sizeof reply; i++) {
        reply[i] = (uint8_t)(0xFF - i);
    }
    if ((rc = subetha_shared_arc_write(h, 0, reply, expect)) != SUBETHA_OK
        || (rc = subetha_shared_arc_flush(h)) != SUBETHA_OK
        || (rc = subetha_atomic_u32_store(turn, 1)) != SUBETHA_OK) {
        fprintf(stderr, "  shared arc peer: reply -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        problems++;
    }

    /* Hold until the creator has read the reply and counted us, so the
     * count it sees is not a race against this process exiting. */
    uint32_t go = 0;
    for (uint32_t spin = 0; spin < 300000u; spin++) {
        if (subetha_atomic_u32_load(turn, &go) != SUBETHA_OK || go >= 2) {
            break;
        }
        sleep_us(100);
    }
    if (go < 2) {
        fprintf(stderr, "  shared arc peer: the creator never acknowledged, turn at %u\n",
                (unsigned)go);
        problems++;
    }

    if (subetha_handle_destroy(turn) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the fence clock at `path` and trades clock readings with the
 * other process for `expect` rounds, taking turns through the atomic at
 * `<path>.turn`.
 *
 * What this shows that one process cannot: the merge orders across a
 * process boundary. Each round the other side ticks and leaves its
 * reading in `<path>.hlc`; this side merges that reading and requires
 * the result to compare strictly above it. A merge that ignored the
 * value it took in, or that only ordered within one process, would give
 * back something at or below it.
 *
 * The turn counter also proves both sides really alternated: this side
 * only ever reads a reading written since its own last one.
 */
int subetha_ctest_peer_fence_clock(const char *path, uint32_t expect)
{
    int problems = 0;
    char hlc_path[1024], turn_path[1024];
    snprintf(hlc_path, sizeof hlc_path, "%s.hlc", path);
    snprintf(turn_path, sizeof turn_path, "%s.turn", path);
    subetha_handle h = SUBETHA_HANDLE_NONE, cell = SUBETHA_HANDLE_NONE, turn = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_fence_clock_open(path, 4, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  fence clock peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    if ((rc = subetha_cell_open(hlc_path, 16, SUBETHA_MODE_STRICT, &cell)) != SUBETHA_OK
        || (rc = subetha_atomic_u32_open(turn_path, SUBETHA_MODE_STRICT, &turn)) != SUBETHA_OK) {
        fprintf(stderr, "  fence clock peer: side channel open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(h);
        return 1;
    }
    uint32_t mine = 0, slot = 0;
    if ((rc = subetha_current_pid(&mine)) != SUBETHA_OK
        || (rc = subetha_fence_clock_register(h, mine, &slot)) != SUBETHA_OK) {
        fprintf(stderr, "  fence clock peer: register -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        problems++;
        expect = 0;
    }
    for (uint32_t i = 0; i < expect; i++) {
        /* The other side writes at even turns and this side at odd ones. */
        uint32_t want = 2 * i + 1, seen = 0;
        int ready = 0;
        for (uint32_t spin = 0; spin < 300000u; spin++) {
            if ((rc = subetha_atomic_u32_load(turn, &seen)) != SUBETHA_OK) {
                break;
            }
            if (seen >= want) {
                ready = 1;
                break;
            }
            sleep_us(100);
        }
        if (!ready) {
            fprintf(stderr, "  fence clock peer: round %u: the turn stuck at %u, wanted %u\n",
                    (unsigned)i, (unsigned)seen, (unsigned)want);
            problems++;
            break;
        }
        uint8_t bytes[16];
        size_t len = 0;
        if ((rc = subetha_cell_get(cell, bytes, sizeof bytes, &len)) != SUBETHA_OK || len != 16) {
            fprintf(stderr, "  fence clock peer: round %u: get -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            break;
        }
        subetha_hlc theirs;
        memcpy(&theirs.physical_us, bytes, 8);
        memcpy(&theirs.logical, bytes + 8, 8);

        subetha_hlc merged;
        if ((rc = subetha_fence_clock_merge(h, slot, theirs, &merged)) != SUBETHA_OK) {
            fprintf(stderr, "  fence clock peer: round %u: merge -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            break;
        }
        /* The whole point: what came in from another process orders below
         * what this process produced by merging it. */
        if (!(merged.physical_us > theirs.physical_us
              || (merged.physical_us == theirs.physical_us && merged.logical > theirs.logical))) {
            fprintf(stderr,
                    "  fence clock peer: round %u: merged (%llu,%llu) does not order above "
                    "the received (%llu,%llu)\n",
                    (unsigned)i, (unsigned long long)merged.physical_us,
                    (unsigned long long)merged.logical, (unsigned long long)theirs.physical_us,
                    (unsigned long long)theirs.logical);
            problems++;
            break;
        }
        memcpy(bytes, &merged.physical_us, 8);
        memcpy(bytes + 8, &merged.logical, 8);
        if ((rc = subetha_cell_set(cell, bytes, sizeof bytes)) != SUBETHA_OK
            || (rc = subetha_atomic_u32_store(turn, want + 1)) != SUBETHA_OK) {
            fprintf(stderr, "  fence clock peer: round %u: publish -> %d (%s)\n", (unsigned)i,
                    (int)rc, subetha_strerror(rc));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(turn) != SUBETHA_OK || subetha_handle_destroy(cell) != SUBETHA_OK
        || subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the barrier at `path` and rendezvous with the other process for
 * `expect` rounds, incrementing the shared counter at `<path>.count`
 * after each release.
 *
 * The counter is what makes this evidence rather than two processes
 * running to completion side by side. Each side reads the counter after
 * the barrier releases at round i and requires it to be at least 2*i:
 * a barrier that let one side run ahead would show a count still at the
 * previous round's value. A process that raced through every round
 * without waiting would fail on the first one it beat the other to.
 */
int subetha_ctest_peer_epoch_barrier(const char *path, uint32_t expect)
{
    int problems = 0;
    char beats_path[1024], count_path[1024];
    snprintf(beats_path, sizeof beats_path, "%s-beats.bin", path);
    snprintf(count_path, sizeof count_path, "%s.count", path);
    subetha_handle beats = SUBETHA_HANDLE_NONE, h = SUBETHA_HANDLE_NONE, count = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_heartbeat_open(beats_path, 8, SUBETHA_MODE_STRICT, &beats);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  barrier peer: heartbeat open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint32_t slot = 0, mine = 0;
    if ((rc = subetha_current_pid(&mine)) != SUBETHA_OK
        || (rc = subetha_heartbeat_register(beats, mine, &slot)) != SUBETHA_OK) {
        fprintf(stderr, "  barrier peer: register -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(beats);
        return 1;
    }
    if ((rc = subetha_epoch_barrier_open(path, beats, 0, SUBETHA_MODE_STRICT, &h)) != SUBETHA_OK
        || (rc = subetha_atomic_u32_open(count_path, SUBETHA_MODE_STRICT, &count)) != SUBETHA_OK) {
        fprintf(stderr, "  barrier peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        subetha_handle_destroy(beats);
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        rc = subetha_epoch_barrier_wait_timeout(h, i, 30000);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  barrier peer: round %u: wait -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            report_detail();
            problems++;
            break;
        }
        uint32_t before = 0;
        if ((rc = subetha_atomic_u32_fetch_add(count, 1, &before)) != SUBETHA_OK) {
            fprintf(stderr, "  barrier peer: round %u: count -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            break;
        }
        /* Both sides were released at round i, so at least 2*i increments
         * from earlier rounds have landed. */
        if (before + 1 < 2 * i) {
            fprintf(stderr, "  barrier peer: round %u: counter at %u, the other side had not arrived\n",
                    (unsigned)i, (unsigned)(before + 1));
            problems++;
            break;
        }
    }
    if (subetha_handle_destroy(count) != SUBETHA_OK || subetha_handle_destroy(h) != SUBETHA_OK
        || subetha_handle_destroy(beats) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}

/* Opens the frame region at `path` (sixteen-byte blocks, `expect` of them,
 * every one allocated by the other process and holding its own index in
 * decimal), checks each block, frees all of them, and takes them all back.
 * The re-allocation is what proves a free in this process reaches the
 * other one's allocator: it can only succeed on blocks this process put on
 * the free list. */
int subetha_ctest_peer_frame_region(const char *path, uint32_t expect)
{
    int problems = 0;
    subetha_handle h = SUBETHA_HANDLE_NONE;
    int32_t rc = subetha_frame_region_open(path, 16, expect, SUBETHA_MODE_STRICT, &h);
    if (rc != SUBETHA_OK) {
        fprintf(stderr, "  frame region peer: open -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        report_detail();
        return 1;
    }
    uint32_t spare = SUBETHA_FRAME_NO_BLOCK;
    rc = subetha_frame_region_alloc(h, &spare);
    if (rc != SUBETHA_E_RING_FULL) {
        fprintf(stderr, "  frame region peer: alloc on a full region -> %d (%s)\n", (int)rc, subetha_strerror(rc));
        problems++;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint8_t out[16];
        size_t len = 0;
        rc = subetha_frame_region_read(h, i, sizeof out, out, sizeof out, &len);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  frame region peer: block %u: read -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            break;
        }
        if (!sixteen_holds_index(out, len, i)) {
            fprintf(stderr, "  frame region peer: block %u does not hold its index\n", (unsigned)i);
            problems++;
        }
        rc = subetha_frame_region_free(h, i);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  frame region peer: block %u: free -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            break;
        }
    }
    uint8_t *taken = calloc(expect, 1);
    if (taken == NULL) {
        fprintf(stderr, "  frame region peer: no memory for %u flags\n", (unsigned)expect);
        subetha_handle_destroy(h);
        return 1;
    }
    for (uint32_t i = 0; i < expect; i++) {
        uint32_t block = SUBETHA_FRAME_NO_BLOCK;
        rc = subetha_frame_region_alloc(h, &block);
        if (rc != SUBETHA_OK) {
            fprintf(stderr, "  frame region peer: take %u: alloc -> %d (%s)\n", (unsigned)i, (int)rc,
                    subetha_strerror(rc));
            problems++;
            break;
        }
        if (block >= expect || taken[block]) {
            fprintf(stderr, "  frame region peer: take %u handed back block %u\n", (unsigned)i, (unsigned)block);
            problems++;
        } else {
            taken[block] = 1;
        }
    }
    free(taken);
    if (subetha_handle_destroy(h) != SUBETHA_OK) {
        problems++;
    }
    return problems;
}
