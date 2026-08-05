#pragma once

/*
 * on9log -> MQTT log sink (singleton).
 *
 * Installs an on9log sink that reassembles each encoded packet (header +
 * payload chunks) under a mutex and publishes it to the `log` topic at
 * QoS 0, throttled to the configured rate. Packets over the rate limit
 * are dropped silently (on9log core tracks its own overflow separately).
 *
 * Design notes:
 *  - Synchronous pass-through: sink callbacks run on the calling task of
 *    the log source (task-context ON9_LOGx, or the ISR drain task for
 *    ON9_ISR_LOGx), and publish immediately. There is deliberately no
 *    queue/ringbuffer between the sink and MQTT: logs are lossy
 *    telemetry, and buffering would add a second overflow semantics on
 *    top of the core's DROPPED accounting. The core's own ISR ringbuffer
 *    is the rate-mismatch buffer.
 *  - Publish is non-blocking for the caller (esp_mqtt_client_publish
 *    enqueues); the QoS 0 packet is dropped if the transport is busy or
 *    the throttle is exceeded.
 *
 * The UART/VFS sink (on9log_esp_vfs) can stay registered in parallel for
 * local debugging; both sinks receive the same packets.
 *
 * Memory rules: one static 4 KiB reassembly buffer; the mutex uses static
 * storage. No heap allocation, no queue, no task.
 */

#include <cstddef>
#include <cstdint>

#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>
#include <on9log.h>

#include "soulcloud.hpp"

namespace soulcloud
{
    class mqtt_bridge;

    class log_sender
    {
    public:
        static log_sender &instance()
        {
            static log_sender s_instance;
            return s_instance;
        }

        log_sender(const log_sender &) = delete;
        log_sender &operator=(const log_sender &) = delete;

        /**
         * Installs the on9log -> MQTT sink.
         *
         * @param cfg    Immutable configuration (borrowed, NOT copied); the
         *               caller must keep it alive for the lifetime of this
         *               singleton. Used for the uplink throttle
         *               (cfg->log_rate_per_s) and the publish topic
         *               (cfg->device_uid).
         * @param bridge MQTT bridge (borrowed, must outlive the sink);
         *               publishes go through bridge->publish().
         * @return ESP_OK on success; ESP_ERR_INVALID_STATE if already
         *         installed; ESP_ERR_NO_MEM if the static mutex could not
         *         be created; ESP_FAIL if the on9log sink table is full.
         *
         * @note Safe to call once from the app init path. The sink
         *       callbacks run on the log source's calling task and take a
         *       mutex; they must not be invoked from ISR context unless
         *       the source is the on9log ISR drain task (a task).
         */
        esp_err_t init(const config *cfg, mqtt_bridge *bridge);

        /** Removes the sink (idempotent). */
        void deinit();

    private:
        log_sender() = default;

        static constexpr char TAG[] = "soulcloud_log";
        static constexpr uint32_t PACKET_MAX = 4096;  // max assembled packet

        const config *cfg_ = nullptr;
        mqtt_bridge *bridge_ = nullptr;
        SemaphoreHandle_t sink_mutex_ = nullptr;
        StaticSemaphore_t sink_mutex_storage_ = {};
        // Long-lived sink descriptor handed to on9log (the core keeps the
        // pointer, not a copy).
        on9log_sink_t sink_ = {};

        uint8_t packet_[PACKET_MAX];
        size_t packet_len_ = 0;
        bool packet_active_ = false;
        bool overflow_ = false;

        uint64_t last_sent_us_ = 0;
        uint32_t dropped_count_ = 0;

        static void sink_start(const uint8_t *header, size_t header_len, void *ctx);
        static void sink_payload(const uint8_t *payload, size_t payload_len,
                                 size_t total_arg_cnt, size_t curr_arg_index, void *ctx);
        static void sink_end(void *ctx);

        void send_packet(const uint8_t *pkt, size_t len);
    };
}  // namespace soulcloud
