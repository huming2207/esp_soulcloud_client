#pragma once

/*
 * Command registry (singleton): named handlers + dispatch for cmd/exec
 * messages.
 *
 * Storage is static (no allocation): up to MAX_COMMANDS registered
 * handlers, and a small ring cache of recently answered command ids so a
 * QoS1 redelivery is answered from cache instead of re-executed.
 */

#include <cstdint>

#include "soulcloud.hpp"

namespace soulcloud
{
    class command_registry
    {
    public:
        static command_registry &instance()
        {
            static command_registry s_instance;
            return s_instance;
        }

        command_registry(const command_registry &) = delete;
        command_registry &operator=(const command_registry &) = delete;

        /** Registers a command name (up to 16). Duplicate names replaced. */
        esp_err_t register_command(const char *name, command_handler_t handler);

        /**
         * Decodes a cmd/exec payload, dispatches to the registered handler
         * and encodes the terminal result into out_buf.
         *
         * @returns the encoded result length (>0) or a negative protocol
         *          error / ESP error.
         */
        int32_t dispatch(const uint8_t *payload, size_t len,
                         uint8_t *out_buf, size_t out_cap);

    private:
        command_registry() = default;

        static constexpr char TAG[] = "soulcloud_cmd";

        struct command_entry
        {
            const char *name;
            command_handler_t handler;
        };

        struct recent_entry
        {
            uint8_t id[16];
            int32_t code;  // result code sent for this id
            bool valid;
        };

        static constexpr uint32_t MAX_COMMANDS = 16;
        static constexpr uint32_t RECENT_CACHE = 8;

        command_entry entries_[MAX_COMMANDS];
        uint32_t count_ = 0;
        recent_entry recent_[RECENT_CACHE];
        uint32_t recent_head_ = 0;

        recent_entry *recent_find(const uint8_t *id);
        void recent_put(const uint8_t *id, int32_t code);
    };
}  // namespace soulcloud
