/*
 * on9log -> MQTT log sink.
 */

#include "logs.hpp"

#include <cstring>

#include <esp_log.h>
#include <esp_timer.h>
#include <on9log.h>

#include "mqtt_bridge.hpp"
#include "protocol.hpp"

namespace soulcloud
{
    esp_err_t log_sender::init(const config *cfg, mqtt_bridge *bridge)
    {
        if (sink_mutex_ != nullptr) {
            return ESP_ERR_INVALID_STATE;
        }
        cfg_ = cfg;
        bridge_ = bridge;

        sink_mutex_ = xSemaphoreCreateMutexStatic(&sink_mutex_storage_);
        if (sink_mutex_ == nullptr) {
            return ESP_ERR_NO_MEM;
        }

        const on9log_sink_t sink = {
            .start_cb = sink_start,
            .payload_cb = sink_payload,
            .end_cb = sink_end,
        };
        if (on9log_add_sink(&sink, this) != ON9LOG_OK) {
            sink_mutex_ = nullptr;
            return ESP_FAIL;
        }
        ESP_LOGI(TAG, "on9log MQTT sink installed");
        return ESP_OK;
    }

    void log_sender::deinit()
    {
        if (sink_mutex_ == nullptr) {
            return;
        }
        const on9log_sink_t sink = {
            .start_cb = sink_start,
            .payload_cb = sink_payload,
            .end_cb = sink_end,
        };
        on9log_remove_sink(&sink, this);
        sink_mutex_ = nullptr;
        cfg_ = nullptr;
        bridge_ = nullptr;
        packet_active_ = false;
    }

    void log_sender::sink_start(const uint8_t *header, size_t header_len, void *ctx)
    {
        log_sender *self = static_cast<log_sender *>(ctx);
        if (self->sink_mutex_ == nullptr) {
            return;
        }
        xSemaphoreTake(self->sink_mutex_, portMAX_DELAY);

        self->packet_len_ = 0;
        self->packet_active_ = true;
        self->overflow_ = false;
        if (header_len > 0) {
            if (header_len <= PACKET_MAX) {
                memcpy(self->packet_, header, header_len);
                self->packet_len_ = header_len;
            } else {
                self->overflow_ = true;
            }
        }
    }

    void log_sender::sink_payload(const uint8_t *payload, size_t payload_len,
                                  size_t total_arg_cnt, size_t curr_arg_index, void *ctx)
    {
        (void)total_arg_cnt;
        (void)curr_arg_index;
        log_sender *self = static_cast<log_sender *>(ctx);
        if (!self->packet_active_ || self->overflow_) {
            return;
        }
        if (payload_len > PACKET_MAX - self->packet_len_) {
            self->overflow_ = true;
            return;
        }
        memcpy(self->packet_ + self->packet_len_, payload, payload_len);
        self->packet_len_ += payload_len;
    }

    void log_sender::sink_end(void *ctx)
    {
        log_sender *self = static_cast<log_sender *>(ctx);
        if (!self->packet_active_) {
            return;
        }
        self->packet_active_ = false;
        if (!self->overflow_) {
            self->send_packet(self->packet_, self->packet_len_);
        }
        xSemaphoreGive(self->sink_mutex_);
    }

    void log_sender::send_packet(const uint8_t *pkt, size_t len)
    {
        if (bridge_ == nullptr || cfg_ == nullptr || !bridge_->is_connected()) {
            dropped_count_++;
            return;
        }

        // throttle to the configured rate (msg/s)
        const uint64_t interval_us = 1000000ull / cfg_->log_rate_per_s;
        const uint64_t now = (uint64_t)esp_timer_get_time();
        if (now - last_sent_us_ < interval_us) {
            dropped_count_++;
            return;
        }
        last_sent_us_ = now;

        char topic[160] = {};
        topic_log(topic, sizeof(topic), cfg_->device_uid);
        const int32_t msg_id = bridge_->publish(topic, pkt, len, 0);
        if (msg_id < 0) {
            dropped_count_++;
        } else if (dropped_count_ > 0) {
            ESP_LOGD(TAG, "dropped %lu log packets under throttle", (unsigned long)dropped_count_);
            dropped_count_ = 0;
        }
    }
}  // namespace soulcloud
