#pragma once

/*
 * Soulcloud wire types shared between the client core and application
 * command handlers. Pure value types with no framework dependencies
 * (host-compilable).
 *
 * Parsed strings/bins in command_exec point into the MQTT payload and are
 * valid only for the duration of the handler call.
 */

#include <cstdint>

namespace soulcloud
{
    // ------------------------------------------------------------------ //
    // command arguments
    // ------------------------------------------------------------------ //

    /**
     * @brief One decoded command argument value.
     *
     * Strings and bins are zero-copy views into the MQTT payload: the
     * pointer members are valid only while the payload buffer lives
     * (i.e. for the duration of the command handler call).
     */
    struct cmd_arg_value
    {
        /**
         * @brief Discriminator selecting the active union member.
         */
        enum type_t : uint8_t {
            TYPE_NIL = 0,    /**< MessagePack nil */
            TYPE_BOOL = 1,   /**< MessagePack bool */
            TYPE_INT = 2,    /**< signed integer (fits int64) */
            TYPE_UINT = 3,   /**< unsigned integer (fits uint64) */
            TYPE_FLOAT = 4,  /**< float32 or float64 */
            TYPE_STR = 5,    /**< string view (str) */
            TYPE_BIN = 6,    /**< binary view (bin) */
        };

        type_t type;  /**< Active union member. */

        union {
            bool b;                        /**< Valid when type == TYPE_BOOL. */
            int64_t i;                     /**< Valid when type == TYPE_INT. */
            uint64_t u;                    /**< Valid when type == TYPE_UINT. */
            double f;                      /**< Valid when type == TYPE_FLOAT. */
            struct
            {
                const char *ptr;           /**< In-place pointer into the payload. */
                uint32_t len;              /**< String length in bytes (not NUL-terminated). */
            } str;                         /**< Valid when type == TYPE_STR. */
            struct
            {
                const uint8_t *ptr;        /**< In-place pointer into the payload. */
                uint32_t len;              /**< Binary length in bytes. */
            } bin;                         /**< Valid when type == TYPE_BIN. */
        };
    };

    /**
     * @brief One named command argument.
     *
     * The wire format is a map with exactly one key: `{key: value}`.
     */
    struct cmd_arg
    {
        const char *key;  /**< NUL-terminated key copy; valid while the payload lives. */
        uint32_t key_len; /**< Key length in bytes (strlen(key)). */
        cmd_arg_value value; /**< Parsed argument value (zero-copy). */
    };

    /**
     * @brief A decoded `cmd/exec` message.
     *
     * All pointers reference the payload (or the internal key_storage)
     * and are valid only for the duration of the dispatch call.
     */
    struct command_exec
    {
        const uint8_t *id;  /**< Command UUID, exactly 16 raw bytes. */
        uint64_t seq;       /**< Per-device monotonic sequence; echo verbatim. */
        const char *cmd;    /**< Command name; NOT NUL-terminated, use cmd_len. */
        uint32_t cmd_len;   /**< Command name length in bytes. */
        cmd_arg args[8];    /**< Up to 8 arguments (wire limit). */
        uint32_t arg_count; /**< Number of valid entries in args[]. */
        char key_storage[8][40]; /**< NUL-terminated copies of the argument keys. */
    };

    /**
     * @brief A command result to be encoded into `cmd/result`.
     *
     * The dispatcher fills id/seq; the handler fills code and may attach
     * an args payload (optional).
     */
    struct command_result
    {
        const uint8_t *id;   /**< Command UUID, 16 raw bytes. */
        uint64_t seq;        /**< Echo of the received seq. */
        int32_t code;        /**< 0 = ok, negative = error (backend contract). */
        const cmd_arg *args; /**< Optional result payload; NULL when absent. */
        uint32_t arg_count;  /**< Number of entries in args[]. */
    };
}  // namespace soulcloud
