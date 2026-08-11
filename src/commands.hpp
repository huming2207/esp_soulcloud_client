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
    /**
     * @brief Command registry (singleton).
     *
     * Maps command names to handlers and dispatches decoded `cmd/exec`
     * payloads. A ring cache of the last RECENT_CACHE command ids
     * suppresses re-execution of QoS1 redeliveries: a cached id is
     * answered with the stored result code instead of running the
     * handler again.
     *
     * @note dispatch() runs on the dedicated soulcloud core task (not
     *       the MQTT event task); handlers must still be quick and
     *       non-blocking.
     */
    class command_registry
    {
    public:
        /**
         * @brief Access the singleton instance.
         * @return Reference to the process-wide registry.
         */
        static command_registry &instance()
        {
            static command_registry s_instance;
            return s_instance;
        }

        /** Result code for a payload rejected by decode (malformed or
         *  over the device-side limits). The platform shows non-zero
         *  codes as failures, so a rejected command must not hang until
         *  timeout. */
        static constexpr int32_t CMD_RESULT_ERR_DECODE = -3;

        /** Result code for a command rejected because OTA is in progress
         *  (device busy); the platform records it as an explicit terminal
         *  failure instead of waiting for a result that never comes. */
        static constexpr int32_t CMD_RESULT_ERR_BUSY = -4;

        command_registry(const command_registry &) = delete;
        command_registry &operator=(const command_registry &) = delete;

        /**
         * @brief Register a command name (up to MAX_COMMANDS).
         *
         * Duplicate names are replaced.
         *
         * @param[in] name    Command name; the registry keeps the POINTER,
         *                    so it must be static or outlive the registry.
         * @param[in] handler Handler invoked for this command.
         * @param[in] ctx     Opaque user context passed to the handler on
         *                    every dispatch (may be NULL).
         * @return
         *  - ESP_OK
         *  - ESP_ERR_INVALID_ARG if name/handler is NULL or name empty
         *  - ESP_ERR_NO_MEM      if the registry is full
         */
        esp_err_t register_command(const char *name, command_handler_t handler, void *ctx = nullptr);

        /**
         * @brief Decode a `cmd/exec` payload, dispatch to the handler and
         *        encode the terminal result into out_buf.
         *
         * Unknown commands answer with code -1; handler failures map to
         * code -2. QoS1 redeliveries are answered from the recent-id
         * cache.
         *
         * @param[in]  payload Decoded payload bytes (borrowed).
         * @param[in]  len     Payload length.
         * @param[out] out_buf Result buffer.
         * @param[in]  out_cap Result buffer capacity.
         * @return The encoded result length (> 0), or a negative
         *         protocol_err / ESP error.
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
            void *ctx;  // opaque user context, passed to the handler
        };

        struct recent_entry
        {
            uint8_t id[16];  // command UUID
            int32_t code;    // result code sent for this id
            bool valid;
        };

        static constexpr uint32_t MAX_COMMANDS = 16;
        static constexpr uint32_t RECENT_CACHE = 8;

        command_entry entries[MAX_COMMANDS];
        uint32_t count = 0;
        recent_entry recent[RECENT_CACHE];
        uint32_t recent_head = 0;

        recent_entry *recent_find(const uint8_t *id);
        void recent_put(const uint8_t *id, int32_t code);
    };
}  // namespace soulcloud
