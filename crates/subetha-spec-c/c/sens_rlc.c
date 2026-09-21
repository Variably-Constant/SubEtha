/*
 * The Sens-O-Matic RLC wire format, section 4 of SENS_O_MATIC_WIRE.md.
 *
 * Read the header for what this is and what it deliberately does not
 * link against. Integers go on the wire byte at a time rather than
 * through a cast, so the code says little-endian rather than inheriting
 * whatever the host happens to be.
 */

#include "sens_rlc.h"

#include <string.h>

/* --------------------------------------------------------- coefficients */

/* Section 4.4, transcribed from the document's table, place 0 first. */
static const uint8_t SENS_TAP_TABLE[SENS_TAPS] = {
    0x01, 0x02, 0x03, 0x05, 0x07, 0x0b, 0x0d, 0x11,
    0x13, 0x17, 0x1d, 0x1f, 0x25, 0x29, 0x2b, 0x2f,
    0x35, 0x3b, 0x3d, 0x43, 0x47, 0x49, 0x4f, 0x53,
    0x59, 0x61, 0x65, 0x67, 0x6b, 0x6d, 0x71, 0x7f,
    0x83, 0x89, 0x8b, 0x95, 0x97, 0x9d, 0xa3, 0xa7,
    0xad, 0xb3, 0xb5, 0xbf, 0xc1, 0xc5, 0xc7, 0xd3,
    0xdf, 0xe3, 0xe5, 0xe9, 0xef, 0xf1, 0xf5, 0xf7,
    0xfb, 0xfd, 0x04, 0x08, 0x0e, 0x16, 0x1a, 0x22,
};

uint8_t sens_tap(unsigned place)
{
    return place < SENS_TAPS ? SENS_TAP_TABLE[place] : 0;
}

uint8_t sens_coefficient(unsigned place, unsigned density)
{
    if (place >= SENS_TAPS) {
        return 0;
    }
    if ((place % 16) <= density) {
        return SENS_TAP_TABLE[place];
    }
    return 0;
}

/* --------------------------------------------------------- little-endian */

static void put_u16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v & 0xff);
    p[1] = (uint8_t)((v >> 8) & 0xff);
}

static uint16_t get_u16(const uint8_t *p)
{
    return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8));
}

static void put_u32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++) {
        p[i] = (uint8_t)((v >> (8 * i)) & 0xff);
    }
}

static uint32_t get_u32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; i++) {
        v |= (uint32_t)p[i] << (8 * i);
    }
    return v;
}

static void put_u64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) {
        p[i] = (uint8_t)((v >> (8 * i)) & 0xff);
    }
}

static uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        v |= (uint64_t)p[i] << (8 * i);
    }
    return v;
}

/* --------------------------------------------------------------- symbols */

int sens_symbol_pack(uint8_t *symbol, size_t symbol_len, const uint8_t *item, size_t item_len)
{
    if (symbol_len < SENS_LEN_PREFIX) {
        return -1;
    }
    if (item_len + SENS_LEN_PREFIX > symbol_len) {
        return -1;
    }
    put_u16(symbol, (uint16_t)item_len);
    if (item_len > 0) {
        memcpy(symbol + SENS_LEN_PREFIX, item, item_len);
    }
    /* Section 4.3: the padding is zero so it contributes nothing to a
     * repair's sum. Left as anything else, two senders packing the same
     * item would produce different repairs. */
    memset(symbol + SENS_LEN_PREFIX + item_len, 0,
           symbol_len - SENS_LEN_PREFIX - item_len);
    return 0;
}

size_t sens_symbol_unpack(const uint8_t *symbol, size_t symbol_len, uint8_t *out, size_t out_cap)
{
    if (symbol_len < SENS_LEN_PREFIX) {
        return 0;
    }
    size_t stated = get_u16(symbol);
    /* Section 4.3: clamp to the buffer actually held, so a corrupt
     * length yields a short item rather than a read past the end. */
    size_t held = symbol_len - SENS_LEN_PREFIX;
    if (stated > held) {
        stated = held;
    }
    if (stated > out_cap) {
        stated = out_cap;
    }
    if (stated > 0) {
        memcpy(out, symbol + SENS_LEN_PREFIX, stated);
    }
    return stated;
}

/* ---------------------------------------------------------------- frames */

size_t sens_data_write(uint8_t *buf, size_t cap, uint64_t conn_id, uint32_t source_id,
                       uint32_t send_us, const uint8_t *symbol, size_t symbol_len)
{
    if (cap < SENS_DATA_HEADER + symbol_len) {
        return 0;
    }
    buf[0] = SENS_TYPE_DATA;
    put_u64(buf + 1, conn_id);
    put_u32(buf + 9, source_id);
    put_u32(buf + 13, send_us);
    memcpy(buf + SENS_DATA_HEADER, symbol, symbol_len);
    return SENS_DATA_HEADER + symbol_len;
}

int sens_data_read(const uint8_t *buf, size_t len, uint64_t *conn_id, uint32_t *source_id,
                   uint32_t *send_us, const uint8_t **symbol, size_t *symbol_len)
{
    if (len < SENS_DATA_HEADER || buf[0] != SENS_TYPE_DATA) {
        return -1;
    }
    *conn_id = get_u64(buf + 1);
    *source_id = get_u32(buf + 9);
    *send_us = get_u32(buf + 13);
    *symbol = buf + SENS_DATA_HEADER;
    *symbol_len = len - SENS_DATA_HEADER;
    return 0;
}

size_t sens_repair_write(uint8_t *buf, size_t cap, uint64_t conn_id, uint32_t repair_key,
                         uint32_t first_source_id, uint16_t window_size, uint8_t dt,
                         const uint8_t *symbols, size_t symbol_len)
{
    if ((dt >> 4) != SENS_GEN_PUBLISHED_TAPS) {
        return 0;
    }
    if (cap < SENS_REPAIR_HEADER + symbol_len) {
        return 0;
    }
    buf[0] = SENS_TYPE_REPAIR;
    put_u64(buf + 1, conn_id);
    put_u32(buf + 9, repair_key);
    put_u32(buf + 13, first_source_id);
    put_u16(buf + 17, window_size);
    buf[19] = dt;

    uint8_t *payload = buf + SENS_REPAIR_HEADER;
    memset(payload, 0, symbol_len);

    unsigned density = (unsigned)(dt & 0x0F);
    for (uint16_t i = 0; i < window_size; i++) {
        /* Section 4.4: place is distance from the newest symbol in the
         * window, and the caller lays them out newest last. */
        unsigned place = (unsigned)(window_size - 1 - i);
        uint8_t coef = sens_coefficient(place, density);
        if (coef == 0) {
            continue;
        }
        const uint8_t *src = symbols + (size_t)i * symbol_len;
        for (size_t b = 0; b < symbol_len; b++) {
            payload[b] ^= sens_gf_mul(coef, src[b]);
        }
    }
    return SENS_REPAIR_HEADER + symbol_len;
}

int sens_repair_read(const uint8_t *buf, size_t len, uint64_t *conn_id, uint32_t *repair_key,
                     uint32_t *first_source_id, uint16_t *window_size, uint8_t *dt,
                     const uint8_t **payload, size_t *payload_len)
{
    if (len < SENS_REPAIR_HEADER || buf[0] != SENS_TYPE_REPAIR) {
        return -1;
    }
    *conn_id = get_u64(buf + 1);
    *repair_key = get_u32(buf + 9);
    *first_source_id = get_u32(buf + 13);
    *window_size = get_u16(buf + 17);
    *dt = buf[19];
    *payload = buf + SENS_REPAIR_HEADER;
    *payload_len = len - SENS_REPAIR_HEADER;
    return 0;
}

int sens_repair_recover(uint8_t *out, size_t symbol_len, const uint8_t *payload,
                        const uint8_t *const *present, uint16_t window_size, uint8_t dt)
{
    /* Section 4.4: a receiver that cannot reproduce a repair's generator
     * must drop it. An equation with the wrong coefficients does not
     * fail to solve, it solves to bytes that were never sent. */
    if ((dt >> 4) != SENS_GEN_PUBLISHED_TAPS) {
        return -1;
    }

    unsigned density = (unsigned)(dt & 0x0F);

    /* Section 4.4: a coefficient of zero means the symbol does not
     * enter the equation. So what has to be missing exactly once is a
     * symbol the repair covers, which is not the same as a symbol in
     * the window. The two coincide at density 15 and nowhere else, and
     * reading them as the same gives up on every window a sparse repair
     * could still repair: at density 0 a window of 48 puts three
     * symbols in the equation and forty-five outside it. */
    long missing = -1;
    uint8_t missing_coef = 0;
    for (uint16_t i = 0; i < window_size; i++) {
        unsigned place = (unsigned)(window_size - 1 - i);
        uint8_t coef = sens_coefficient(place, density);
        if (coef == 0) {
            continue;
        }
        if (present[i] == NULL) {
            if (missing >= 0) {
                return -1; /* one equation recovers one unknown */
            }
            missing = (long)i;
            missing_coef = coef;
        }
    }
    if (missing < 0) {
        return -1;
    }

    memcpy(out, payload, symbol_len);
    for (uint16_t i = 0; i < window_size; i++) {
        if (present[i] == NULL) {
            continue;
        }
        unsigned place = (unsigned)(window_size - 1 - i);
        uint8_t coef = sens_coefficient(place, density);
        if (coef == 0) {
            continue;
        }
        for (size_t b = 0; b < symbol_len; b++) {
            out[b] ^= sens_gf_mul(coef, present[i][b]);
        }
    }

    uint8_t inv = sens_gf_inv(missing_coef);
    for (size_t b = 0; b < symbol_len; b++) {
        out[b] = sens_gf_mul(out[b], inv);
    }
    return 0;
}

/* ---------------------------------------------------- reverse-path frames */

size_t sens_ack_write(uint8_t *buf, size_t cap, uint32_t delivered_through, uint64_t sack)
{
    if (cap < 13) {
        return 0;
    }
    buf[0] = SENS_TYPE_ACK;
    put_u32(buf + 1, delivered_through);
    put_u64(buf + 5, sack);
    return 13;
}

int sens_ack_read(const uint8_t *buf, size_t len, uint32_t *delivered_through, uint64_t *sack)
{
    if (len < 13 || buf[0] != SENS_TYPE_ACK) {
        return -1;
    }
    *delivered_through = get_u32(buf + 1);
    *sack = get_u64(buf + 5);
    return 0;
}

size_t sens_nak_write(uint8_t *buf, size_t cap, const uint32_t *ids, size_t count)
{
    size_t need = 1 + count * 4;
    if (cap < need) {
        return 0;
    }
    buf[0] = SENS_TYPE_NAK;
    for (size_t i = 0; i < count; i++) {
        put_u32(buf + 1 + i * 4, ids[i]);
    }
    return need;
}

size_t sens_nak_read(const uint8_t *buf, size_t len, uint32_t *out, size_t out_cap)
{
    if (len < 1 || buf[0] != SENS_TYPE_NAK) {
        return 0;
    }
    size_t n = 0;
    size_t at = 1;
    /* Section 4.5: read until fewer than four bytes remain, so a
     * trailing partial id is ignored rather than making the frame
     * invalid. */
    while (len - at >= 4 && n < out_cap) {
        out[n++] = get_u32(buf + at);
        at += 4;
    }
    return n;
}

size_t sens_feedback_write(uint8_t *buf, size_t cap, uint8_t loss_q8, uint8_t burst_q8,
                           uint8_t cong_q8, uint16_t rate_q16, uint16_t cap_q16)
{
    if (cap < 8) {
        return 0;
    }
    buf[0] = SENS_TYPE_FEEDBACK;
    buf[1] = loss_q8;
    buf[2] = burst_q8;
    buf[3] = cong_q8;
    put_u16(buf + 4, rate_q16);
    put_u16(buf + 6, cap_q16);
    return 8;
}

int sens_feedback_read(const uint8_t *buf, size_t len, uint8_t *loss_q8, uint8_t *burst_q8,
                       uint8_t *cong_q8, uint16_t *rate_q16, uint16_t *cap_q16)
{
    if (len < 8 || buf[0] != SENS_TYPE_FEEDBACK) {
        return -1;
    }
    *loss_q8 = buf[1];
    *burst_q8 = buf[2];
    *cong_q8 = buf[3];
    *rate_q16 = get_u16(buf + 4);
    *cap_q16 = get_u16(buf + 6);
    return 0;
}
