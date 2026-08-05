#pragma once

/*
 * MPack configuration for the Soulcloud device client.
 *
 * Enabled:  MPACK_READER  - streaming decode (mpack_reader_t)
 *           MPACK_EXPECT  - typed expect API (mpack_expect_*)
 *           MPACK_WRITER  - encode (mpack_writer_t)
 * Disabled: MPACK_NODE    - DOM API (requires malloc) - not used
 *           MPACK_STDIO   - FILE* helpers - not used
 *
 * The writer is always initialised with a caller-owned fixed buffer, so no
 * heap allocation happens on the encode path. Decoding is streaming and
 * allocation-free. newlib (MPACK_STDLIB) provides memcpy/memset/assert.
 */
#define MPACK_READER 1
#define MPACK_EXPECT 1
#define MPACK_WRITER 1
#define MPACK_NODE 0
#define MPACK_STDLIB 1
#define MPACK_STDIO 0
#define MPACK_MAX_DEPTH 32
