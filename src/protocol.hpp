#pragma once

/*
 * Soulcloud v1 wire protocol: topic constants, MessagePack payload builders
 * and strict parsers.
 *
 * Contract source: soulcloud.js/packages/core/src/protocol (command.ts etc.)
 * (command.ts, stat.ts, ota-result.ts, topic.ts) and broker ota-publish.ts.
 *
 * Design constraints (project rules):
 *   - zero heap allocation: all buffers are caller-owned; parsed strings
 *     point into the payload (valid only for the duration of the call)
 *   - strict parsing: duplicate map keys, wrong types, missing required
 *     fields and trailing bytes are rejected; unknown map keys are skipped
 *     (forward compatibility)
 *   - no ESP-IDF dependencies: this layer is host-compilable for tests
 *   - mpack_reader_t / mpack_writer_t are private members of the
 *     msgpack_reader / msgpack_writer classes (RAII: init in ctor,
 *     destroy in dtor, single error state)
 */

#include <cstddef>
#include <cstdint>

#include "mpack.h"

namespace soulcloud
{
    // ------------------------------------------------------------------ //
    // protocol errors (negative return codes)
    // ------------------------------------------------------------------ //

    enum protocol_err : int32_t {
        ERR_OK = 0,
        ERR_BAD_MSG = -1,        // malformed MessagePack or trailing bytes
        ERR_TRUNCATED = -2,      // payload too short
        ERR_TYPE = -3,           // unexpected MessagePack type
        ERR_DUP_KEY = -4,        // duplicate map key
        ERR_MISSING_FIELD = -5,  // required field absent
        ERR_OVERFLOW = -6,       // value/buffer limit exceeded
        ERR_FIELD_LEN = -7,      // field length invalid (e.g. id != 16 bytes)
    };

    // ------------------------------------------------------------------ //
    // strict MessagePack reader (streaming, allocation-free)
    // ------------------------------------------------------------------ //

    class msgpack_reader
    {
    public:
        explicit msgpack_reader(const uint8_t *data, size_t len);
        ~msgpack_reader();
        msgpack_reader(const msgpack_reader &) = delete;
        msgpack_reader &operator=(const msgpack_reader &) = delete;

        /** First error encountered, or ERR_OK. */
        int32_t err() const { return err_; }
        bool ok() const { return err_ == ERR_OK; }

        /** Type of the next value without consuming it. */
        mpack_type_t peek_type();

        // --- strict typed reads; on failure err() is set and a safe
        //     default is returned (caller must check ok()) ---------------

        uint32_t expect_map();
        uint32_t expect_array();
        uint64_t expect_u64();
        int64_t expect_int();
        int32_t expect_int32();
        bool expect_bool();
        void expect_nil();
        double expect_float();  // accepts float32 or float64

        /** Reads a string map key into buf (NUL-terminated). */
        int32_t read_key(char *buf, size_t cap, size_t *out_len);

        /**
         * Reads a string value in-place (points into the payload).
         * Rejects strings longer than max_len.
         */
        int32_t read_str(const char **ptr, uint32_t *len, uint32_t max_len);

        /** Reads a bin value in-place; rejects bins longer than max_len. */
        int32_t read_bin(const uint8_t **ptr, uint32_t *len, uint32_t max_len);

        /** Skips the next value (any type, depth-bounded by MPack). */
        void skip_value();

        /**
         * Verifies the whole payload was consumed. Returns err().
         * Must be called before the reader goes out of scope.
         */
        int32_t finish();

    private:
        mpack_reader_t r_;
        int32_t err_ = ERR_OK;

        void fail(int32_t e)
        {
            if (err_ == ERR_OK) err_ = e;
        }
    };

    // ------------------------------------------------------------------ //
    // MessagePack writer (fixed caller-owned buffer, allocation-free)
    // ------------------------------------------------------------------ //

    class msgpack_writer
    {
    public:
        msgpack_writer(uint8_t *buf, size_t cap);
        ~msgpack_writer();
        msgpack_writer(const msgpack_writer &) = delete;
        msgpack_writer &operator=(const msgpack_writer &) = delete;

        int32_t err() const { return err_; }
        bool ok() const { return err_ == ERR_OK; }

        void start_map(uint32_t count);
        void finish_map();
        void start_array(uint32_t count);
        void finish_array();

        void write_str(const char *s);  // NUL-terminated
        void write_str(const char *s, size_t len);
        void write_bin(const void *data, size_t len);
        void write_uint(uint64_t v);  // minimal encoding; the backend
                                       // accepts any uint width for seq/up
        void write_int(int64_t v);
        void write_nil();
        void write_bool(bool v);
        void write_double(double v);

        /** Bytes written so far (valid when ok()). */
        size_t bytes_written();

        /**
         * Finalises the writer (error check + destroy) and returns err().
         * Idempotent; also called by the destructor.
         */
        int32_t finish();

    private:
        mpack_writer_t w_;
        bool destroyed_ = false;
        int32_t err_ = ERR_OK;

        void fail(int32_t e)
        {
            if (err_ == ERR_OK) err_ = e;
        }
    };

    // ------------------------------------------------------------------ //
    // topics
    // ------------------------------------------------------------------ //

    void topic_cmd_exec(char *out, size_t cap, const char *device_uid);
    void topic_cmd_result(char *out, size_t cap, const char *device_uid);
    void topic_ota(char *out, size_t cap, const char *device_uid);
    void topic_ota_result(char *out, size_t cap, const char *device_uid);
    void topic_log(char *out, size_t cap, const char *device_uid);
    void topic_stat(char *out, size_t cap, const char *device_uid);

    // ------------------------------------------------------------------ //
    // command execution (decode of cmd/exec payloads)
    // ------------------------------------------------------------------ //

    /** One argument value. Strings/bins point into the payload. */
    struct cmd_arg_value
    {
        enum type_t : uint8_t {
            TYPE_NIL = 0,
            TYPE_BOOL = 1,
            TYPE_INT = 2,
            TYPE_UINT = 3,
            TYPE_FLOAT = 4,
            TYPE_STR = 5,
            TYPE_BIN = 6,
        };
        type_t type;
        union {
            bool b;
            int64_t i;
            uint64_t u;
            double f;
            struct {
                const char *ptr;
                uint32_t len;
            } str;
            struct {
                const uint8_t *ptr;
                uint32_t len;
            } bin;
        };
    };

    /** One named command argument (map with exactly one key). */
    struct cmd_arg
    {
        const char *key;  // NUL-terminated, valid while the payload lives
        uint32_t key_len;
        cmd_arg_value value;
    };

    /** A decoded command execution. All pointers reference the payload. */
    struct command_exec
    {
        const uint8_t *id;  // exactly 16 raw bytes
        uint64_t seq;
        const char *cmd;  // NUL-terminated copy is not made; use cmd_len
        uint32_t cmd_len;
        cmd_arg args[8];  // up to 8 arguments
        uint32_t arg_count;
        char key_storage[8][40];  // NUL-terminated copies of arg keys
    };

    /**
     * Strictly decodes a cmd/exec payload.
     * @returns ERR_OK or a negative protocol_err; on error *out is untouched.
     */
    int32_t decode_command_exec(const uint8_t *payload, size_t len, command_exec *out);

    // ------------------------------------------------------------------ //
    // command result (encode of cmd/result payloads)
    // ------------------------------------------------------------------ //

    struct command_result
    {
        const uint8_t *id;  // 16 raw bytes
        uint64_t seq;
        int32_t code;
        const cmd_arg *args;  // optional
        uint32_t arg_count;
    };

    int32_t encode_command_result(uint8_t *buf, size_t cap, size_t *out_len, const command_result *res);

    // ------------------------------------------------------------------ //
    // device status (encode of stat payloads)
    // ------------------------------------------------------------------ //

    struct device_stat
    {
        const char *sn;  // encoded as MessagePack bin
        size_t sn_len;
        const uint8_t *fw;  // 32 bytes ELF SHA-256, encoded as bin
        size_t fw_len;
        uint64_t up;  // uptime seconds
        const char *rst;  // reset reason string
    };

    int32_t encode_stat(uint8_t *buf, size_t cap, size_t *out_len, const device_stat *stat);

    // ------------------------------------------------------------------ //
    // OTA notice (decode of ota topic payloads)
    // ------------------------------------------------------------------ //

    struct ota_notice
    {
        char release_id[37];
        char job_id[37];
        char bin_sha256[65];
        uint32_t bin_size;
        char download_url[192];
        char download_token[512];
        char download_expires_at[48];
        bool has_version;
        char version[256];
    };

    /**
     * Strictly decodes an OTA notice. download.url/token are required.
     * @returns ERR_OK or a negative protocol_err.
     */
    int32_t decode_ota_notice(const uint8_t *payload, size_t len, ota_notice *out);

    // ------------------------------------------------------------------ //
    // OTA result (encode of ota/result payloads)
    // ------------------------------------------------------------------ //

    struct ota_result
    {
        const char *release_id;
        const char *job_id;
        const char *state;  // "downloaded" | "installed" | "failed"
        int32_t code;
        const char *message;  // optional; omitted when NULL
    };

    int32_t encode_ota_result(uint8_t *buf, size_t cap, size_t *out_len, const ota_result *res);
}  // namespace soulcloud
