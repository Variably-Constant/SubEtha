/*
 * Section 2 of SENS_O_MATIC_WIRE.md: the field both codes compute in.
 *
 * GF(2^8) with the primitive polynomial x^8+x^4+x^3+x^2+1 (0x11D) and
 * generator 2. The document puts this in its own section because the
 * RLC variant of section 4 and the RS variant of section 5 share it,
 * and this file follows that shape so neither code owns the field.
 *
 * The document says any implementation of the same field
 * interoperates, so tables rather than a shift-and-reduce loop is a
 * free choice and not a claim about the format.
 */

#ifndef SENS_FIELD_H
#define SENS_FIELD_H

#include <stdint.h>

/* Build the tables. Every entry point in this library calls it, so a
 * caller that forgets does not get zeros out of the multiply. */
void sens_field_init(void);

uint8_t sens_gf_mul(uint8_t a, uint8_t b);

/* The multiplicative inverse. Zero is not in the group and answers 0,
 * which every caller here tests for before dividing. */
uint8_t sens_gf_inv(uint8_t a);

#endif /* SENS_FIELD_H */
