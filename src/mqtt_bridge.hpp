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

#include <esp_err.h>
#include <mqtt_client.h>

#include "soulcloud.hpp"

namespace soulcloud
{
    struct mqtt_callbacks
    {
        void (*on_connected)(void *ctx);
        void (*on_disconnected)(void *ctx);
        void (*on_data)(void *ctx, const char *topic, size_t topic_len,
                        const uint8_t *data, size_t data_len);
        void (*on_error)(void *ctx, esp_mqtt_error_codes_t *err);
        void *ctx;
    };

    class mqtt_bridge
    {
    public:
        esp_err_t init(const config *cfg, const mqtt_callbacks *cbs);
        void deinit();

        esp_err_t start();
        esp_err_t stop();

        bool is_connected() const { return connected_; }

        /** Subscribes at QoS (must be called from an event callback or
         *  after start; esp-mqtt queues it otherwise). */
        int32_t subscribe(const char *topic, int qos);

        /** Publishes; returns the message id or a negative error. */
        int32_t publish(const char *topic, const uint8_t *data, size_t len, int qos);

        /** Forces an immediate reconnect attempt (e.g. WiFi back up). */
        void notify_wifi_connected();

    private:
        esp_mqtt_client_handle_t client_ = nullptr;
        esp_mqtt_client_config_t mqtt_cfg_ = {};
        bool started_ = false;
        bool connected_ = false;
        mqtt_callbacks cbs_ = {};

        static constexpr char TAG[] = "soulcloud_mqtt";

        static void event_handler(void *ctx, esp_event_base_t base, int32_t evt_id, void *evt_data);
        void handle_event(esp_mqtt_event_handle_t evt);
    };
}  // namespace soulcloud
