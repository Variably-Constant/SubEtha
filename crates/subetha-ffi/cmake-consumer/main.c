/* A C program taking SubEtha through the installed package: one ring in
 * anonymous memory, one item through it, and a clean shutdown. It prints
 * "subetha consumer: ok" on success and the failing call's code and
 * detail otherwise, so the packaging gate can read the outcome. */

#include <subetha.h>

#include <stdio.h>
#include <string.h>

static int fail(const char *what, int32_t rc)
{
    char detail[256];
    subetha_last_error_detail(detail, sizeof detail);
    fprintf(stderr, "subetha consumer: %s failed: %s (%s)\n", what, subetha_strerror(rc), detail);
    return 1;
}

int main(void)
{
    static const char payload[] = "packaged";
    /* Named fields, not positions: every other field takes the zero that
     * means "the library's default", and a field added to the struct
     * later does not silently land a value in the wrong place. */
    subetha_ring_options options = {.mode = SUBETHA_MODE_DEFAULT, .stamps = SUBETHA_STAMPS_NONE};
    subetha_handle ring;
    uint32_t producer;
    uint32_t consumer;
    uint8_t slot[SUBETHA_RING_SLOT_BYTES];
    size_t len = 0;
    int32_t rc;

    rc = subetha_init(SUBETHA_MODE_STRICT);
    if (rc != SUBETHA_OK) return fail("init", rc);

    rc = subetha_ring_create_anon(1, 1, 64, &options, &ring);
    if (rc != SUBETHA_OK) return fail("create", rc);

    rc = subetha_ring_register_producer(ring, &producer);
    if (rc != SUBETHA_OK) return fail("register producer", rc);
    rc = subetha_ring_register_consumer(ring, &consumer);
    if (rc != SUBETHA_OK) return fail("register consumer", rc);

    rc = subetha_ring_try_push(ring, producer, (const uint8_t *)payload, sizeof payload);
    if (rc != SUBETHA_OK) return fail("push", rc);

    rc = subetha_ring_try_pop(ring, consumer, slot, sizeof slot, &len);
    if (rc != SUBETHA_OK) return fail("pop", rc);
    if (len != SUBETHA_RING_SLOT_BYTES || memcmp(slot, payload, sizeof payload) != 0) {
        fprintf(stderr, "subetha consumer: the pop did not yield the pushed payload\n");
        return 1;
    }

    rc = subetha_handle_destroy(ring);
    if (rc != SUBETHA_OK) return fail("destroy", rc);
    rc = subetha_shutdown();
    if (rc != SUBETHA_OK) return fail("shutdown", rc);

    printf("subetha consumer: ok, crate %s, abi %s\n",
           subetha_crate_version_string(), subetha_abi_version_string());
    return 0;
}
