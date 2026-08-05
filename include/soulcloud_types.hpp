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

    /** A command result to be encoded; payload args are optional. */
    struct command_result
    {
        const uint8_t *id;  // 16 raw bytes
        uint64_t seq;
        int32_t code;
        const cmd_arg *args;  // optional
        uint32_t arg_count;
    };
}  // namespace soulcloud
