/*
 * Section 5 of SENS_O_MATIC_WIRE.md: the block Cauchy Reed-Solomon
 * variant, written from that document and from nothing else.
 *
 * The rule the whole crate lives by holds here too: this calls no
 * SubEtha code. The RLC half of section 4 already paid for itself by
 * reading the recovery condition the way the document's obvious reading
 * suggests and disagreeing with the vectors on one geometry, so the
 * point of doing it again for section 5 is that nobody has yet read
 * this half as an outsider.
 *
 * Covers 5.1, the `DATA` frame and shard packing, and 5.2, the code
 * itself. The control plane of 5.3 is a separate job.
 */

#ifndef SENS_RS_H
#define SENS_RS_H

#include <stddef.h>
#include <stdint.h>

#include "sens_field.h"

/* Section 5.1: the RS data frame's type byte and header size. */
#define SENS_RS_TYPE_DATA 0x01
#define SENS_RS_DATA_HEADER 13

/* Section 5.1: a data shard prefixes its item with a u16-le length, the
 * same way an RLC symbol does, so the largest item is shard_len - 2. */
#define SENS_RS_LEN_PREFIX 2

/* Section 5.1: the flags byte. */
#define SENS_RS_FLAG_PARITY 0x01
#define SENS_RS_FLAG_OUTER 0x02
#define SENS_RS_FLAG_RETRANSMIT 0x04

/* Section 5.2 permits `k + r` up to 256. This implementation stops at
 * 32, which is the bound the document says the reference keeps for its
 * arrival bitmap and is the widest the vectors exercise. It is a limit
 * of this code, not of the format, and it is here so the recovery
 * matrix fits on the stack. A wider block is refused rather than
 * truncated. */
#define SENS_RS_MAX_SHARDS 32

/* ------------------------------------------------------------ the code */

/* Section 5.2: the Cauchy entry for parity row `j` and data column `c`
 * of a `(k, r)` code.
 *
 *     C[j][c] = inverse( (k + j) XOR c )
 *
 * Data indices below k and parity indices at or above k are disjoint,
 * so the XOR is never zero and the inverse always exists. Answers 0 for
 * indices outside the code, which is not a legal entry and is why the
 * callers below check their arguments first.
 */
uint8_t sens_rs_cauchy(unsigned k, unsigned j, unsigned c);

/* Section 5.2: parity shard `j` over `k` data shards laid out in index
 * order, each `shard_len` bytes.
 *
 * Returns 0 on success, -1 when the shape is outside this
 * implementation's bounds. A parity shard carries no length prefix of
 * its own: it is the combination of the data shards' payloads, prefix
 * bytes included.
 */
int sens_rs_parity(uint8_t *out, size_t shard_len, const uint8_t *data,
                   unsigned k, unsigned j);

/* Section 5.2: rebuild every data shard from any `k` surviving shards.
 *
 * `present` is `k + r` pointers in shard-index order, data first then
 * parity, with a lost shard null. `out` receives `k` data shards of
 * `shard_len` each.
 *
 * Returns 0 on success, -1 when fewer than `k` shards survive or the
 * shape is outside this implementation's bounds. It cannot fail for a
 * singular matrix: every square submatrix of a Cauchy matrix is
 * invertible, which is the property that makes any `k` sufficient, so a
 * singular one would mean the matrix was not built as the document says.
 */
int sens_rs_recover(uint8_t *out, size_t shard_len, const uint8_t *const *present,
                    unsigned k, unsigned r);

/* ---------------------------------------------------------- the frame */

/* Section 5.1: pack an item into a data shard as a u16-le length, the
 * bytes, then zero padding. Returns 0, or -1 when the item does not
 * satisfy `length + 2 <= shard_len`. */
int sens_rs_shard_pack(uint8_t *shard, size_t shard_len, const uint8_t *item,
                       size_t item_len);

/* Section 5.1: the item a data shard holds, clamped to the buffer held.
 * Returns the length written into `out`. */
size_t sens_rs_shard_unpack(const uint8_t *shard, size_t shard_len, uint8_t *out,
                            size_t out_cap);

/* Section 5.1: write a `DATA` frame. Returns the bytes written, or 0
 * when the buffer is too small. */
size_t sens_rs_data_write(uint8_t *buf, size_t cap, uint32_t block_id,
                          uint8_t shard_index, uint8_t k, uint8_t r, uint8_t flags,
                          uint32_t epoch, const uint8_t *shard, size_t shard_len);

/* Section 5.1: read a `DATA` frame's header. Returns 0 on success, -1
 * when the frame is too short or its type byte is not 0x01. */
int sens_rs_data_read(const uint8_t *buf, size_t len, uint32_t *block_id,
                      uint8_t *shard_index, uint8_t *k, uint8_t *r, uint8_t *flags,
                      uint32_t *epoch, const uint8_t **shard, size_t *shard_len);

/* ------------------------------------------------- outer-parity block id */

/* Section 5.1: the four fields an outer-parity block id carries.
 *
 *     d = (id >> 27) & 0xF, r_outer = (id >> 24) & 0x7,
 *     segment = (id >> 8) & 0xFFFF, outer_index = id & 0xFF
 *
 * Returns 0 when the id names a decodable segment, -1 when bit 31 is
 * clear, which makes it a data block id rather than an outer one, or
 * when `d` or `r_outer` is zero. The document requires a receiver to
 * discard the latter: those values describe a segment with no data
 * blocks or no parity, which names nothing decodable, so both fields
 * are one-based on the wire.
 */
int sens_rs_outer_id_read(uint32_t id, unsigned *d, unsigned *r_outer,
                          unsigned *segment, unsigned *outer_index);

#endif /* SENS_RS_H */
