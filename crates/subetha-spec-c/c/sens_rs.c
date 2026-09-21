/*
 * Section 5 of SENS_O_MATIC_WIRE.md: block Cauchy Reed-Solomon.
 *
 * Read the header for what this is and what it does not link against.
 * Integers go on the wire byte at a time rather than through a cast, so
 * the code says little-endian rather than inheriting the host's.
 */

#include "sens_rs.h"

#include <string.h>

/* ------------------------------------------------------------ the code */

uint8_t sens_rs_cauchy(unsigned k, unsigned j, unsigned c)
{
    /* Section 5.2: C[j][c] = inverse((k + j) XOR c). The parity index is
     * k + j and the data index is c, and the document's point is that
     * those two ranges are disjoint, so the XOR is never zero. An index
     * outside the code is not a legal entry and answers 0 rather than
     * inverting something meaningless. */
    if (k == 0 || k + j >= 256 || c >= k) {
        return 0;
    }
    unsigned parity_index = k + j;
    return sens_gf_inv((uint8_t)(parity_index ^ c));
}

static int shape_ok(unsigned k, unsigned r)
{
    /* Section 5.2: k and r must each be at least 1 and k + r must not
     * exceed 256. The further bound to SENS_RS_MAX_SHARDS is this
     * implementation's, and the header says so. */
    return k >= 1 && r >= 1 && k + r <= SENS_RS_MAX_SHARDS;
}

int sens_rs_parity(uint8_t *out, size_t shard_len, const uint8_t *data, unsigned k,
                   unsigned j)
{
    sens_field_init();
    if (k < 1 || k > SENS_RS_MAX_SHARDS || j >= SENS_RS_MAX_SHARDS) {
        return -1;
    }
    /* Section 5.2: parity shard j is the sum over c of C[j][c] times
     * data shard c. The sum is the field's addition, which is
     * exclusive-or. */
    memset(out, 0, shard_len);
    for (unsigned c = 0; c < k; c++) {
        uint8_t coef = sens_rs_cauchy(k, j, c);
        const uint8_t *src = data + (size_t)c * shard_len;
        for (size_t b = 0; b < shard_len; b++) {
            out[b] ^= sens_gf_mul(coef, src[b]);
        }
    }
    return 0;
}

/* Invert `n` by `n` in place by Gauss-Jordan over the field, with the
 * identity carried alongside. Returns 0, or -1 for a singular matrix.
 *
 * A singular matrix cannot happen on a submatrix of [I_k ; C], because
 * every square submatrix of a Cauchy matrix is invertible and that is
 * precisely what makes any k shards sufficient. It is reported rather
 * than asserted so a matrix built wrongly says so instead of answering
 * plausible bytes.
 */
static int invert(uint8_t *m, uint8_t *inv, unsigned n)
{
    for (unsigned i = 0; i < n; i++) {
        for (unsigned j = 0; j < n; j++) {
            inv[i * n + j] = (i == j) ? 1 : 0;
        }
    }

    for (unsigned col = 0; col < n; col++) {
        unsigned pivot = col;
        while (pivot < n && m[pivot * n + col] == 0) {
            pivot++;
        }
        if (pivot == n) {
            return -1;
        }
        if (pivot != col) {
            for (unsigned j = 0; j < n; j++) {
                uint8_t t = m[col * n + j];
                m[col * n + j] = m[pivot * n + j];
                m[pivot * n + j] = t;
                t = inv[col * n + j];
                inv[col * n + j] = inv[pivot * n + j];
                inv[pivot * n + j] = t;
            }
        }

        uint8_t scale = sens_gf_inv(m[col * n + col]);
        for (unsigned j = 0; j < n; j++) {
            m[col * n + j] = sens_gf_mul(m[col * n + j], scale);
            inv[col * n + j] = sens_gf_mul(inv[col * n + j], scale);
        }

        for (unsigned row = 0; row < n; row++) {
            if (row == col) {
                continue;
            }
            uint8_t factor = m[row * n + col];
            if (factor == 0) {
                continue;
            }
            for (unsigned j = 0; j < n; j++) {
                m[row * n + j] ^= sens_gf_mul(factor, m[col * n + j]);
                inv[row * n + j] ^= sens_gf_mul(factor, inv[col * n + j]);
            }
        }
    }
    return 0;
}

int sens_rs_recover(uint8_t *out, size_t shard_len, const uint8_t *const *present,
                    unsigned k, unsigned r)
{
    sens_field_init();
    if (!shape_ok(k, r)) {
        return -1;
    }

    /* Take the first k shards that arrived, whichever they are. Each
     * contributes one row of the system: a surviving data shard c is the
     * identity row with 1 at column c, because the code is systematic
     * and that shard is itself the value; a surviving parity shard j is
     * the Cauchy row C[j][*]. */
    uint8_t m[SENS_RS_MAX_SHARDS * SENS_RS_MAX_SHARDS];
    uint8_t inv[SENS_RS_MAX_SHARDS * SENS_RS_MAX_SHARDS];
    const uint8_t *rhs[SENS_RS_MAX_SHARDS];
    unsigned rows = 0;

    for (unsigned idx = 0; idx < k + r && rows < k; idx++) {
        if (present[idx] == NULL) {
            continue;
        }
        for (unsigned c = 0; c < k; c++) {
            if (idx < k) {
                m[rows * k + c] = (c == idx) ? 1 : 0;
            } else {
                m[rows * k + c] = sens_rs_cauchy(k, idx - k, c);
            }
        }
        rhs[rows] = present[idx];
        rows++;
    }
    if (rows < k) {
        return -1;
    }

    if (invert(m, inv, k) != 0) {
        return -1;
    }

    /* data = inverse(m) times the surviving shards, a byte column at a
     * time. */
    for (unsigned d = 0; d < k; d++) {
        uint8_t *dst = out + (size_t)d * shard_len;
        memset(dst, 0, shard_len);
        for (unsigned s = 0; s < k; s++) {
            uint8_t coef = inv[d * k + s];
            if (coef == 0) {
                continue;
            }
            for (size_t b = 0; b < shard_len; b++) {
                dst[b] ^= sens_gf_mul(coef, rhs[s][b]);
            }
        }
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

/* ---------------------------------------------------------------- shards */

int sens_rs_shard_pack(uint8_t *shard, size_t shard_len, const uint8_t *item,
                       size_t item_len)
{
    if (shard_len < SENS_RS_LEN_PREFIX) {
        return -1;
    }
    if (item_len + SENS_RS_LEN_PREFIX > shard_len) {
        return -1;
    }
    put_u16(shard, (uint16_t)item_len);
    if (item_len > 0) {
        memcpy(shard + SENS_RS_LEN_PREFIX, item, item_len);
    }
    /* The padding is zero so it contributes nothing to a parity shard's
     * combination, the same reason an RLC symbol pads with zero. */
    memset(shard + SENS_RS_LEN_PREFIX + item_len, 0,
           shard_len - SENS_RS_LEN_PREFIX - item_len);
    return 0;
}

size_t sens_rs_shard_unpack(const uint8_t *shard, size_t shard_len, uint8_t *out,
                            size_t out_cap)
{
    if (shard_len < SENS_RS_LEN_PREFIX) {
        return 0;
    }
    size_t stated = get_u16(shard);
    size_t held = shard_len - SENS_RS_LEN_PREFIX;
    if (stated > held) {
        stated = held;
    }
    if (stated > out_cap) {
        stated = out_cap;
    }
    if (stated > 0) {
        memcpy(out, shard + SENS_RS_LEN_PREFIX, stated);
    }
    return stated;
}

/* ---------------------------------------------------------------- frames */

size_t sens_rs_data_write(uint8_t *buf, size_t cap, uint32_t block_id,
                          uint8_t shard_index, uint8_t k, uint8_t r, uint8_t flags,
                          uint32_t epoch, const uint8_t *shard, size_t shard_len)
{
    if (cap < SENS_RS_DATA_HEADER + shard_len) {
        return 0;
    }
    buf[0] = SENS_RS_TYPE_DATA;
    put_u32(buf + 1, block_id);
    buf[5] = shard_index;
    buf[6] = k;
    buf[7] = r;
    buf[8] = flags;
    put_u32(buf + 9, epoch);
    memcpy(buf + SENS_RS_DATA_HEADER, shard, shard_len);
    return SENS_RS_DATA_HEADER + shard_len;
}

int sens_rs_data_read(const uint8_t *buf, size_t len, uint32_t *block_id,
                      uint8_t *shard_index, uint8_t *k, uint8_t *r, uint8_t *flags,
                      uint32_t *epoch, const uint8_t **shard, size_t *shard_len)
{
    if (len < SENS_RS_DATA_HEADER || buf[0] != SENS_RS_TYPE_DATA) {
        return -1;
    }
    *block_id = get_u32(buf + 1);
    *shard_index = buf[5];
    *k = buf[6];
    *r = buf[7];
    *flags = buf[8];
    *epoch = get_u32(buf + 9);
    *shard = buf + SENS_RS_DATA_HEADER;
    *shard_len = len - SENS_RS_DATA_HEADER;
    return 0;
}

/* ------------------------------------------------- outer-parity block id */

int sens_rs_outer_id_read(uint32_t id, unsigned *d, unsigned *r_outer,
                          unsigned *segment, unsigned *outer_index)
{
    /* Bit 31 set is what stops an outer block colliding with the
     * sequential data-block ids, so a clear bit means this is a data
     * block id and not an outer one. */
    if ((id & 0x80000000u) == 0) {
        return -1;
    }
    unsigned dd = (id >> 27) & 0xF;
    unsigned ro = (id >> 24) & 0x7;
    /* A receiver must discard an outer block whose d or r_outer is
     * zero: those describe a segment with no data blocks or no parity,
     * which names nothing decodable. */
    if (dd == 0 || ro == 0) {
        return -1;
    }
    *d = dd;
    *r_outer = ro;
    *segment = (id >> 8) & 0xFFFF;
    *outer_index = id & 0xFF;
    return 0;
}
