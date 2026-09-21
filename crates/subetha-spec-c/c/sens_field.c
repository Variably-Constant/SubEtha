/* Section 2: GF(2^8) under 0x11D with generator 2. */

#include "sens_field.h"

static uint8_t g_log[256];
static uint8_t g_exp[512];
static int g_ready = 0;

void sens_field_init(void)
{
    if (g_ready) {
        return;
    }
    uint16_t x = 1;
    for (int i = 0; i < 255; i++) {
        g_exp[i] = (uint8_t)x;
        g_log[(uint8_t)x] = (uint8_t)i;
        /* Multiply by the generator, 2, reducing by 0x11D on overflow. */
        x <<= 1;
        if (x & 0x100) {
            x ^= 0x11D;
        }
    }
    /* A second lap lets a sum of two logarithms index the table without
     * a modulus on the hot path. */
    for (int i = 255; i < 512; i++) {
        g_exp[i] = g_exp[i - 255];
    }
    /* log(0) is undefined and the multiply never reads it, because it
     * tests for a zero operand first. */
    g_log[0] = 0;
    g_ready = 1;
}

uint8_t sens_gf_mul(uint8_t a, uint8_t b)
{
    if (a == 0 || b == 0) {
        return 0;
    }
    return g_exp[(unsigned)g_log[a] + (unsigned)g_log[b]];
}

uint8_t sens_gf_inv(uint8_t a)
{
    if (a == 0) {
        return 0;
    }
    return g_exp[255 - g_log[a]];
}
