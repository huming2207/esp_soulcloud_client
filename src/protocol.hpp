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

#include <soulcloud_types.hpp>

#include "mpack.h"

namespace soulcloud
{
    // ------------------------------------------------------------------ //
    // protocol errors (negative return codes)
    // ------------------------------------------------------------------ //

    enum protocol_err : int32_t {
        ERR_OK = 0,          /**< Success. */
        ERR_BAD_MSG = -1,    /**< Malformed MessagePack or trailing bytes. */
        ERR_TRUNCATED = -2,  /**< Payload too short. */
        ERR_TYPE = -3,       /**< Unexpected MessagePack type. */
        ERR_DUP_KEY = -4,    /**< Duplicate map key. */
        ERR_MISSING_FIELD = -5, /**< Required field absent. */
        ERR_OVERFLOW = -6,   /**< Value/buffer limit exceeded. */
        ERR_FIELD_LEN = -7,  /**< Field length invalid (e.g. id != 16 bytes). */
    };

    // ------------------------------------------------------------------ //
    // strict MessagePack reader (streaming, allocation-free)
    // ------------------------------------------------------------------ //

    /**
     * @brief Strict, allocation-free MessagePack reader.
     *
     * Wraps mpack_reader_t over a caller-owned buffer. Every typed read
     * validates the wire type; on the first error err() is latched and a
     * safe default is returned — callers must check ok()/err().
     */
    class msgpack_reader
    {
    public:
        /**
         * @brief Construct a reader over an immutable payload.
         * @param[in] data Payload bytes (borrowed, not copied).
         * @param[in] len  Payload length in bytes.
         */
        explicit msgpack_reader(const uint8_t *data, size_t len);
        ~msgpack_reader();
        msgpack_reader(const msgpack_reader &) = delete;
        msgpack_reader &operator=(const msgpack_reader &) = delete;

        /** @return First error encountered, or ERR_OK. */
        int32_t err() const { return err_; }

        /** @return true while no error has been latched. */
        bool ok() const { return err_ == ERR_OK; }

        /**
         * @brief Type of the next value without consuming it.
         * @return The MessagePack type of the next value.
         */
        mpack_type_t peek_type();

        // --- strict typed reads; on failure err() is set and a safe
        //     default is returned (caller must check ok()) ---------------

        /** @brief Expects and returns a map header. */
        uint32_t expect_map();

        /** @brief Expects and returns an array header. */
        uint32_t expect_array();

        /** @brief Expects an unsigned integer (any wire width). */
        uint64_t expect_u64();

        /** @brief Expects a signed integer (any wire width). */
        int64_t expect_int();

        /** @brief Expects a signed integer that fits int32. */
        int32_t expect_int32();

        /** @brief Expects a boolean. */
        bool expect_bool();

        /** @brief Expects nil. */
        void expect_nil();

        /** @brief Expects a float (float32 or float64 accepted). */
        double expect_float();

        /**
         * @brief Reads a string map key into buf.
         *
         * @param[out] buf      Destination buffer.
         * @param[in]  cap      Destination capacity (the key is
         *                      NUL-terminated, so cap must be > key length).
         * @param[out] out_len  Key length in bytes (excluding NUL).
         * @return ERR_OK, ERR_TYPE or ERR_OVERFLOW.
         */
        int32_t read_key(char *buf, size_t cap, size_t *out_len);

        /**
         * @brief Reads a string value in-place.
         *
         * @param[out] ptr     Receives a pointer INTO the payload
         *                      (not NUL-terminated; use len).
         * @param[out] len     String length in bytes.
         * @param[in]  max_len Reject strings longer than this.
         * @return ERR_OK, ERR_TYPE or ERR_OVERFLOW.
         */
        int32_t read_str(const char **ptr, uint32_t *len, uint32_t max_len);

        /**
         * @brief Reads a bin value in-place.
         *
         * @param[out] ptr     Receives a pointer INTO the payload.
         * @param[out] len     Binary length in bytes.
         * @param[in]  max_len Reject bins longer than this.
         * @return ERR_OK, ERR_TYPE or ERR_OVERFLOW.
         */
        int32_t read_bin(const uint8_t **ptr, uint32_t *len, uint32_t max_len);

        /**
         * @brief Skips the next value (any type, depth-bounded by MPack).
         *        Used for unknown map keys (forward compatibility).
         */
        void skip_value();

        /**
         * @brief Verifies the whole payload was consumed and returns err().
         *
         * @note Must be called before the reader goes out of scope; the
         *       destructor alone does not validate trailing bytes.
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

    /**
     * @brief MessagePack writer over a fixed caller-owned buffer.
     *
     * All writes are bounds-checked; on overflow err() is latched and
     * subsequent writes are no-ops. finish() must be called (or the
     * destructor runs) to validate and release the underlying writer.
     */
    class msgpack_writer
    {
    public:
        /**
         * @brief Construct a writer over a caller-owned buffer.
         * @param[in] buf Destination buffer (borrowed).
         * @param[in] cap Buffer capacity in bytes.
         */
        msgpack_writer(uint8_t *buf, size_t cap);
        ~msgpack_writer();
        msgpack_writer(const msgpack_writer &) = delete;
        msgpack_writer &operator=(const msgpack_writer &) = delete;

        /** @return First error encountered, or ERR_OK. */
        int32_t err() const { return err_; }

        /** @return true while no error has been latched. */
        bool ok() const { return err_ == ERR_OK; }

        /** @brief Starts a map with count entries (call finish_map()). */
        void start_map(uint32_t count);

        /** @brief Finishes the current map. */
        void finish_map();

        /** @brief Starts an array with count entries (call finish_array()). */
        void start_array(uint32_t count);

        /** @brief Finishes the current array. */
        void finish_array();

        /** @brief Writes a NUL-terminated string. */
        void write_str(const char *s);

        /** @brief Writes a string of len bytes (may contain NULs). */
        void write_str(const char *s, size_t len);

        /** @brief Writes a binary blob. */
        void write_bin(const void *data, size_t len);

        /**
         * @brief Writes an unsigned integer.
         * @note Minimal-width encoding; the backend accepts any uint
         *       width for seq/up.
         */
        void write_uint(uint64_t v);

        /** @brief Writes a signed integer. */
        void write_int(int64_t v);

        /** @brief Writes nil. */
        void write_nil();

        /** @brief Writes a boolean. */
        void write_bool(bool v);

        /** @brief Writes a float64. */
        void write_double(double v);

        /** @return Bytes written so far (valid when ok()). */
        size_t bytes_written();

        /**
         * @brief Finalises the writer (error check + destroy) and returns
         *        err(). Idempotent; also called by the destructor.
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

    /** @brief Builds `soulcloud/v1/devices/<uid>/cmd/exec` into out. */
    void topic_cmd_exec(char *out, size_t cap, const char *device_uid);
    /** @brief Builds `soulcloud/v1/devices/<uid>/cmd/result` into out. */
    void topic_cmd_result(char *out, size_t cap, const char *device_uid);
    /** @brief Builds `soulcloud/v1/devices/<uid>/ota` into out. */
    void topic_ota(char *out, size_t cap, const char *device_uid);
    /** @brief Builds `soulcloud/v1/devices/<uid>/ota/result` into out. */
    void topic_ota_result(char *out, size_t cap, const char *device_uid);
    /** @brief Builds `soulcloud/v1/devices/<uid>/log` into out. */
    void topic_log(char *out, size_t cap, const char *device_uid);
    /** @brief Builds `soulcloud/v1/devices/<uid>/stat` into out. */
    void topic_stat(char *out, size_t cap, const char *device_uid);

    /**
     * @brief Strictly decodes a `cmd/exec` payload.
     *
     * @param[in]  payload MessagePack payload.
     * @param[in]  len     Payload length.
     * @param[out] out     Filled execution; string/bin views point into
     *                     the payload (valid while payload lives).
     * @return ERR_OK or a negative protocol_err.
     */
    int32_t decode_command_exec(const uint8_t *payload, size_t len, command_exec *out);

    // ------------------------------------------------------------------ //
    // command result (encode of cmd/result payloads)
    // ------------------------------------------------------------------ //

    /**
     * @brief Encodes a `cmd/result` payload (4-field map; payload = nil
     *        when args are absent).
     *
     * @param[in]  buf     Destination buffer.
     * @param[in]  cap     Buffer capacity.
     * @param[out] out_len Encoded length on success.
     * @param[in]  res     Result to encode.
     * @return ERR_OK or a negative protocol_err (ERR_OVERFLOW if the
     *         encoded payload does not fit).
     */
    int32_t encode_command_result(uint8_t *buf, size_t cap, size_t *out_len, const command_result *res);

    // ------------------------------------------------------------------ //
    // device status (encode of stat payloads)
    // ------------------------------------------------------------------ //

    /**
     * @brief Fields of a `stat` report.
     */
    struct device_stat
    {
        const char *sn;      /**< Serial, encoded as MessagePack bin. */
        size_t sn_len;       /**< Serial length. */
        const uint8_t *fw;   /**< 32 bytes ELF SHA-256, encoded as bin. */
        size_t fw_len;       /**< Firmware hash length (32). */
        uint64_t up;         /**< Uptime in seconds. */
        const char *rst;     /**< Reset reason string. */
    };

    /**
     * @brief Encodes a `stat` payload (4-field map: sn, fw, up, rst).
     *
     * @param[in]  buf     Destination buffer.
     * @param[in]  cap     Buffer capacity.
     * @param[out] out_len Encoded length on success.
     * @param[in]  stat    Status to encode.
     * @return ERR_OK or a negative protocol_err (ERR_OVERFLOW if it does
     *         not fit).
     */
    int32_t encode_stat(uint8_t *buf, size_t cap, size_t *out_len, const device_stat *stat);

    // ------------------------------------------------------------------ //
    // OTA notice (decode of ota topic payloads)
    // ------------------------------------------------------------------ //

    /**
     * @brief Decoded OTA notice (copy of the payload strings, NUL-terminated).
     */
    struct ota_notice
    {
        char release_id[37];           /**< Release UUID string. */
        char job_id[37];               /**< OTA job UUID string. */
        char bin_sha256[65];           /**< Expected firmware SHA-256 (hex). */
        uint32_t bin_size;             /**< Expected image size in bytes. */
        char download_url[192];        /**< Relative download path. */
        char download_token[512];      /**< Bearer token for the download. */
        char download_expires_at[48];  /**< Token expiry (informational). */
        bool has_version;              /**< Whether version was present. */
        char version[256];             /**< Optional release version. */
    };

    /**
     * @brief Strictly decodes an OTA notice.
     *
     * download.url and download.token are required; version is optional.
     *
     * @param[in]  payload MessagePack payload.
     * @param[in]  len     Payload length.
     * @param[out] out     Filled notice (strings are copied and
     *                     NUL-terminated).
     * @return ERR_OK or a negative protocol_err.
     */
    int32_t decode_ota_notice(const uint8_t *payload, size_t len, ota_notice *out);

    // ------------------------------------------------------------------ //
    // OTA result (encode of ota/result payloads)
    // ------------------------------------------------------------------ //

    /**
     * @brief Fields of an `ota/result` report.
     */
    struct ota_result
    {
        const char *release_id; /**< Echo of the notice release_id. */
        const char *job_id;     /**< Echo of the notice job_id. */
        const char *state;      /**< "downloaded" | "installed" | "failed". */
        int32_t code;           /**< 0 = ok, negative = failure code. */
        const char *message;    /**< Optional message; omitted when NULL. */
    };

    /**
     * @brief Encodes an `ota/result` payload (4- or 5-field map).
     *
     * The message field is omitted entirely when NULL (the backend
     * schema rejects null but allows absence).
     *
     * @param[in]  buf     Destination buffer.
     * @param[in]  cap     Buffer capacity.
     * @param[out] out_len Encoded length on success.
     * @param[in]  res     Result to encode.
     * @return ERR_OK or a negative protocol_err.
     */
    int32_t encode_ota_result(uint8_t *buf, size_t cap, size_t *out_len, const ota_result *res);
}  // namespace soulcloud
