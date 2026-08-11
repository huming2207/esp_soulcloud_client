#pragma once

/*
 * esp-mqtt bridge: owns the esp_mqtt client handle and the event wiring.
 *
 * Events are forwarded to the client core through function-pointer
 * callbacks (no std::function, no allocation). The client core decides
 * what to subscribe and how to react.
 */

#include <cstddef>
#include <cstdint>
#include <atomic>

#include <esp_err.h>
#include <mqtt_client.h>

#include "soulcloud.hpp"

namespace soulcloud
{
    /**
     * @brief Event callbacks forwarded from the esp-mqtt event loop.
     *
     * All callbacks run on the esp-mqtt task and must be quick; the
     * payload/topic pointers are valid only for the duration of the call.
     */
    struct mqtt_callbacks {
        void (*on_connected)(void *ctx);    /**< MQTT session established. */
        void (*on_disconnected)(void *ctx); /**< MQTT session dropped. */
        void (*on_data)(void *ctx, const char *topic, size_t topic_len, const uint8_t *data,
                        size_t data_len); /**< Inbound PUBLISH (complete, reassembled). */
        void (*on_subscribed)(void *ctx, int msg_id, int return_code, bool failed);
        /**< SUBACK for a subscribe (return_code per topic, >=0x80 = rejected),
         *  or a SUBSCRIBE_FAILED error (failed=true, return_code=-1). */
        void (*on_error)(void *ctx, esp_mqtt_error_codes_t *err); /**< Transport error (may be NULL). */
        void *ctx;                                                /**< Opaque context passed to every callback. */
    };

    /**
     * @brief Thin wrapper over esp_mqtt_client.
     *
     * Configures the client from soulcloud::config, registers one event
     * handler and forwards events through mqtt_callbacks. Publish and
     * subscribe are thread-safe (esp-mqtt queues internally).
     */
    class mqtt_bridge
    {
    public:
        /**
         * @brief Configure and register the esp-mqtt client.
         *
         * @param[in] cfg Configuration (borrowed for the call only; the
         *                relevant fields are copied into the client
         *                config).
         * @param[in] cbs Callbacks (borrowed; the struct itself is
         *                copied, the function pointers must stay valid).
         * @return ESP_OK, or ESP_ERR_NO_MEM if esp_mqtt_client_init failed.
         */
        esp_err_t init(const config *cfg, const mqtt_callbacks *cbs);

        /** @brief Destroys the esp-mqtt client (idempotent). */
        void deinit();

        /**
         * @brief Start the client (connects; auto-reconnect enabled).
         * @return ESP_OK or an esp-mqtt error.
         */
        esp_err_t start();

        /** @brief Stop the client (disconnects). */
        esp_err_t stop();

        /** @return true while the MQTT session is established. */
        bool is_connected() const
        {
            return connected.load(std::memory_order_acquire);
        }

        /**
         * @brief Subscribe to a topic at the given QoS.
         *
         * May be called from an event callback or any task. Returns -1 if
         * the client is not connected (no queueing in esp-mqtt v1.1.0);
         * the core-task SUBACK watchdog retries.
         *
         * @return Message id, or a negative error.
         */
        int32_t subscribe(const char *topic, int qos);

        /**
         * @brief Publish a payload to a topic.
         *
         * Thread-safe; returns the message id or a negative error (the
         * payload is copied by esp-mqtt before returning).
         *
         * @return Message id, or a negative error.
         */
        int32_t publish(const char *topic, const uint8_t *data, size_t len, int qos);

        /**
         * @brief Force an immediate reconnect attempt.
         *
         * Call when the network interface (re)connects so the client
         * does not wait for the esp-mqtt reconnect timer.
         */
        void notify_wifi_connected();

    private:
        esp_mqtt_client_handle_t client = nullptr;
        esp_mqtt_client_config_t mqtt_cfg = {};
        std::atomic<bool> started{false};
        std::atomic<bool> connected{false};
        mqtt_callbacks _cbs = {};

        // ---- MQTT fragment reassembly ----
        // One large PUBLISH arrives as several MQTT_EVENT_DATA events
        // (total_data_len/current_data_offset); only the first carries
        // the topic. Reassemble here so the core always sees one complete
        // message (this also makes CONFIG_SOULCLOUD_INBOUND_MAX enforce
        // the WHOLE payload, not each fragment).
        struct frag_state {
            bool active = false;    // mid-stream
            uint32_t total = 0;     // total_data_len of the current message
            uint32_t received = 0;  // bytes buffered so far
            uint8_t *buf = nullptr; // reassembly buffer (PSRAM, init-allocated)
            char topic[160] = {};   // topic from the first fragment
            size_t topic_len = 0;
        } frag;

        static constexpr char TAG[] = "soulcloud_mqtt";

        static void event_handler(void *ctx, esp_event_base_t base, int32_t evt_id, void *evt_data);
        void handle_event(esp_mqtt_event_handle_t evt);
        void handle_publish(esp_mqtt_event_handle_t evt);
    };
} // namespace soulcloud
