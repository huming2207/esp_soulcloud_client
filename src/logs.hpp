#pragma once

/*
 * on9log -> MQTT log sink (singleton).
 *
 * Installs an on9log sink that reassembles each encoded packet (header +
 * payload chunks) under a mutex and publishes it to the `log` topic at
 * QoS 0, throttled to the configured rate. Packets over the rate limit
 * are dropped silently (on9log core tracks its own overflow separately).
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

        /** Installs the sink. */
        esp_err_t init(const config *cfg, mqtt_bridge *bridge);

        /** Removes the sink. */
        void deinit();

    private:
        log_sender() = default;

        static constexpr char TAG[] = "soulcloud_log";
        static constexpr uint32_t PACKET_MAX = 4096;  // max assembled packet

        const config *cfg_ = nullptr;
        mqtt_bridge *bridge_ = nullptr;
        SemaphoreHandle_t sink_mutex_ = nullptr;
        StaticSemaphore_t sink_mutex_storage_ = {};

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
