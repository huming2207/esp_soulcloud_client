#pragma once

/*
 * on9log -> MQTT log sink (singleton).
 *
 * Installs an on9log sink that reassembles each encoded packet (header +
 * payload chunks) under a mutex and publishes it to the `log` topic at
 * QoS 0 (best-effort telemetry; drops are counted and surfaced via the
 * drop WARN), throttled to the configured rate. Packets over the rate
 * limit are dropped silently (on9log core tracks its own overflow
 * separately).
 * With cfg->log_batch_count > 1 packets are accumulated and published as
 * one aggregated container (PROTOCOL.log-packaging.md).
 *
 * Design notes:
 *  - Producer/consumer split: sink callbacks (running on the log source's
 *    task) only assemble the packet and enqueue it into a FreeRTOS ring
 *    buffer with a zero-tick wait (full buffer = drop, logs are lossy
 *    telemetry). The soulcloud core task consumes the ring buffer and
 *    publishes with throttling, so MQTT work never blocks the log
 *    producer and a burst is drained in order.
 *  - on9log core's own ISR ringbuffer remains the rate-mismatch buffer
 *    between ISR log sources and the drain task.
 *
 * The UART/VFS sink (on9log_esp_vfs) can stay registered in parallel for
 * local debugging; both sinks receive the same packets.
 *
 * Drop visibility: when packets are dropped device-side (ring buffer
 * full, throttle, or disconnected MQTT) a WARN packet is emitted through
 * on9log at most once per second, so the backend can tell "device logged
 * nothing" from "device dropped logs" (which the platform cannot see
 * otherwise). The notification itself is not counted as a drop (re-entrancy
 * guard), and its own ring-buffer drop is suppressed so it cannot trigger
 * another notification.
 *
 * Memory rules: one static 4 KiB reassembly buffer; the mutex uses static
 * storage; the ring buffer is heap-allocated once in init().
 */

#include <cstddef>
#include <cstdint>

#include <freertos/FreeRTOS.h>
#include <freertos/ringbuf.h>
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
         * Installs the on9log -> MQTT sink (producer side).
         *
         * @param cfg     Immutable configuration (borrowed, NOT copied); the
         *                caller must keep it alive for the lifetime of this
         *                singleton. Used for the uplink throttle
         *                (cfg->log_rate_per_s) and the publish topic
         *                (cfg->device_uid).
         * @param bridge  MQTT bridge (borrowed, must outlive the sink);
         *                publishes go through bridge->publish() from
         *                drain().
         * @return ESP_OK on success; ESP_ERR_INVALID_STATE if already
         *         installed; ESP_ERR_NO_MEM if the mutex or the log ring
         *         buffer could not be created; ESP_FAIL if the on9log
         *         sink table is full.
         *
         * @note Safe to call once from the app init path. The sink
         *       callbacks run on the log source's calling task and take a
         *       mutex; they must not be invoked from ISR context unless
         *       the source is the on9log ISR drain task (a task).
         */
        esp_err_t init(const config *cfg, mqtt_bridge *bridge);

        /** Removes the sink (idempotent). */
        void deinit();

        /**
         * Drains the log ring buffer and publishes queued packets
         * (consumer side, throttled to cfg->log_rate_per_s).
         *
         * @note Must be called from the soulcloud core task (or any task
         *       other than the sink callbacks); uses a zero-tick receive,
         *       so it never blocks.
         */
        void drain();

    private:
        log_sender() = default;

        static constexpr char TAG[] = "soulcloud_log";
        static constexpr uint32_t PACKET_MAX = 4096;  // max assembled packet

        const config *_cfg = nullptr;
        mqtt_bridge *_bridge = nullptr;
        SemaphoreHandle_t _sink_mutex = nullptr;
        StaticSemaphore_t _sink_mutex_storage = {};
        RingbufHandle_t log_rb = nullptr;  // producer -> consumer queue
        // Long-lived sink descriptor handed to on9log (the core keeps the
        // pointer, not a copy).
        on9log_sink_t _sink = {};

        uint8_t packet[PACKET_MAX];
        size_t packet_len = 0;
        bool packet_active = false;
        bool overflow = false;

        uint64_t last_sent_us = 0;
        uint32_t dropped_count = 0;
        bool drop_notify_inflight = false;  // re-entrancy guard for the drop WARN
        uint64_t last_drop_notify_us = 0;    // throttle: at most one WARN per second

        // ---- batching (PROTOCOL.log-packaging.md) ----
        // Batch buffer layout: [0..3] reserved for the container head
        // (0x01 + array16); elements (bin8/bin16 header + packet bytes)
        // accumulate from batch+4. On flush, <=15 elements compact the
        // head to 0x01 + fixarray (memmove) and 16+ keep array16.
        static constexpr uint32_t BATCH_MAX_BYTES = 4096;  // container budget
        static constexpr uint32_t BATCH_MAX_ELEMS = 128;   // doc-suggested cap
        uint8_t batch[4 + BATCH_MAX_BYTES] = {};
        size_t batch_len = 0;        // element bytes accumulated
        uint32_t batch_elems = 0;
        uint64_t batch_start_us = 0;  // when the batch started (timeout base)

        bool batch_append(const uint8_t *pkt, size_t len);
        void flush_batch();
        bool throttle_ok();

        static void sink_start(const uint8_t *header, size_t header_len, void *ctx);
        static void sink_payload(const uint8_t *payload, size_t payload_len,
                                 size_t total_arg_cnt, size_t curr_arg_index, void *ctx);
        static void sink_end(void *ctx);

        void send_packet(const uint8_t *pkt, size_t len);
    };
}  // namespace soulcloud
