/*
 * A second implementation of the Sens-O-Matic RLC wire format, in C,
 * written from SENS_O_MATIC_WIRE.md and from nothing else.
 *
 * The one rule it lives by is that it does not call SubEtha. Not the C
 * ABI, not the Rust, not a header of theirs. A program written on top
 * of subetha-ffi would share every assumption and every mistake of the
 * first implementation and could not disagree with it about anything,
 * which is the whole reason for writing a second one. The value comes
 * from somebody reading the document and writing the encoder and
 * decoder from it, so the places the document is silent or reads two
 * ways surface as a program that does not interoperate.
 *
 * It implements section 4 only: the RLC frames, symbol packing, the
 * GF(2^8) field of section 2, and the published-taps generator. The RS
 * variant of section 5 is a separate job.
 *
 * Every declaration below cites the section it came from. Where the
 * document did not say and something had to be chosen, the comment says
 * so and says what was assumed, because that list is half of what this
 * exercise is for.
 */

#ifndef SENS_RLC_H
#define SENS_RLC_H

#include <stddef.h>
#include <stdint.h>

/* Section 2, shared with the RS variant of section 5: the field both
 * codes compute in, and `sens_field_init`, which everything here
 * expects to have been called. */
#include "sens_field.h"

/* Section 2: frame type bytes of the RLC variant. */
#define SENS_TYPE_DATA 0x0A
#define SENS_TYPE_REPAIR 0x0B
#define SENS_TYPE_NAK 0x0C
#define SENS_TYPE_ACK 0x0D
#define SENS_TYPE_FEEDBACK 0x0E

/* Section 4.1 and 4.2: header sizes, before the symbol or payload. */
#define SENS_DATA_HEADER 17
#define SENS_REPAIR_HEADER 20

/* Section 4.4: the tap table has 64 entries because a window may be
 * opened to 64 symbols, and a place at or past the end of the table
 * takes a coefficient of zero. */
#define SENS_TAPS 64

/* Section 4.3: the length prefix a symbol carries before its item. */
#define SENS_LEN_PREFIX 2

/* Section 4.4: generator ids. Zero is per-repair generation, which this
 * document does not define and a receiver must refuse. */
#define SENS_GEN_PER_REPAIR 0
#define SENS_GEN_PUBLISHED_TAPS 1

/* --------------------------------------------------------- coefficients */

/* Section 4.4: the tap at `place`, or 0 when `place` is past the table.
 * Exposed so a checker can compare the table itself against the
 * document rather than only its consequences. */
uint8_t sens_tap(unsigned place);

/* Section 4.4. `place` is the symbol's distance from the newest symbol
 * in the window, so the newest is 0. `density` is the low nibble of
 * `dt`, 0 to 15.
 *
 *     if place >= 64:                  0
 *     else if place mod 16 <= density: TAPS[place]
 *     else:                            0
 *
 * Density is a fraction of the window rather than a reach into it: a
 * density of 0 takes places 0, 16, 32 and 48, spread across the whole
 * window, not just its newest end.
 */
uint8_t sens_coefficient(unsigned place, unsigned density);

/* --------------------------------------------------------------- symbols */

/* Section 4.3: write `item` into a `symbol_len` symbol as a u16-le
 * length, the bytes, then zero padding.
 *
 * Returns 0 on success, -1 when `item_len + 2 > symbol_len`, which the
 * document states an item must satisfy.
 */
int sens_symbol_pack(uint8_t *symbol, size_t symbol_len, const uint8_t *item, size_t item_len);

/* Section 4.3: the item a symbol holds, clamped to the buffer actually
 * held so a corrupt length yields a short item rather than a read past
 * the end. Returns the item length written into `out`.
 */
size_t sens_symbol_unpack(const uint8_t *symbol, size_t symbol_len, uint8_t *out, size_t out_cap);

/* ---------------------------------------------------------------- frames */

/* Section 4.1: write a `DATA` frame. `buf` must hold 17 + symbol_len.
 * Returns the bytes written, or 0 when the buffer is too small. */
size_t sens_data_write(uint8_t *buf, size_t cap, uint64_t conn_id, uint32_t source_id,
                       uint32_t send_us, const uint8_t *symbol, size_t symbol_len);

/* Section 4.1: read a `DATA` frame's header. Returns 0 on success, -1
 * when the frame is too short or its type byte is not 0x0A. `symbol` is
 * set to point into `buf`. */
int sens_data_read(const uint8_t *buf, size_t len, uint64_t *conn_id, uint32_t *source_id,
                   uint32_t *send_us, const uint8_t **symbol, size_t *symbol_len);

/* Section 4.2: write a `REPAIR` frame over `window_size` symbols laid
 * out newest last, each `symbol_len` bytes.
 *
 * `dt` is the whole byte: density in the low nibble, generator id in the
 * high one. A generator other than 1 is refused with 0, because this
 * implementation can reproduce no other.
 */
size_t sens_repair_write(uint8_t *buf, size_t cap, uint64_t conn_id, uint32_t repair_key,
                         uint32_t first_source_id, uint16_t window_size, uint8_t dt,
                         const uint8_t *symbols, size_t symbol_len);

/* Section 4.2: read a `REPAIR` frame's header. Returns 0 on success, -1
 * when the frame is too short or its type byte is not 0x0B. */
int sens_repair_read(const uint8_t *buf, size_t len, uint64_t *conn_id, uint32_t *repair_key,
                     uint32_t *first_source_id, uint16_t *window_size, uint8_t *dt,
                     const uint8_t **payload, size_t *payload_len);

/* Section 4.2 and 4.4: recover the one missing symbol of a window from a
 * repair and every other symbol it covers.
 *
 * `present` is `window_size` pointers, newest last, with the missing
 * one null. Returns 0 on success, -1 when the generator is not 1 or
 * when the repair does not determine exactly one unknown.
 *
 * What must be missing exactly once is a symbol the repair covers,
 * which is not the same as a symbol in the window. Section 4.4 says a
 * coefficient of zero means the symbol does not enter the equation, so
 * a window may be full of holes and still be solvable as long as only
 * one of them falls where the coefficient is nonzero. The two readings
 * coincide at density 15 and nowhere else: at density 0 a window of 48
 * puts three symbols in the equation and forty-five outside it.
 */
int sens_repair_recover(uint8_t *out, size_t symbol_len, const uint8_t *payload,
                        const uint8_t *const *present, uint16_t window_size, uint8_t dt);

/* ---------------------------------------------------- reverse-path frames */

/* Section 4.6: write an `ACK`. 13 bytes. */
size_t sens_ack_write(uint8_t *buf, size_t cap, uint32_t delivered_through, uint64_t sack);

/* Section 4.6: read an `ACK`. Returns 0 on success, -1 otherwise. */
int sens_ack_read(const uint8_t *buf, size_t len, uint32_t *delivered_through, uint64_t *sack);

/* Section 4.5: write a `NAK` naming `count` missing source ids. */
size_t sens_nak_write(uint8_t *buf, size_t cap, const uint32_t *ids, size_t count);

/* Section 4.5: read a `NAK`. Ids are read until fewer than four bytes
 * remain, so a trailing partial id is ignored rather than refused.
 * Returns the number of ids written into `out`. */
size_t sens_nak_read(const uint8_t *buf, size_t len, uint32_t *out, size_t out_cap);

/* Section 4.7: write a `FEEDBACK`. 8 bytes. */
size_t sens_feedback_write(uint8_t *buf, size_t cap, uint8_t loss_q8, uint8_t burst_q8,
                           uint8_t cong_q8, uint16_t rate_q16, uint16_t cap_q16);

/* Section 4.7: read a `FEEDBACK`. Returns 0 on success, -1 otherwise. */
int sens_feedback_read(const uint8_t *buf, size_t len, uint8_t *loss_q8, uint8_t *burst_q8,
                       uint8_t *cong_q8, uint16_t *rate_q16, uint16_t *cap_q16);

#endif /* SENS_RLC_H */
