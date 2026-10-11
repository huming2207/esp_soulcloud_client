#pragma once

#include <atomic>
#include <cstddef>
#include <functional>

#include <esp_err.h>
#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>
#include <mqtt_client.h>

class sc_credentials;
class sc_rpc;
struct sc_rpc_slot;

/** Optional MQTT 5 properties of one publish. */
struct sc_publish_props {
    const char *content_type = nullptr;
    const char *correlation = nullptr;
    size_t correlation_len = 0;
    const char *response_topic = nullptr;
};

/**
 * The MQTT 5 (over WebSocket) connection to the platform, built on esp-mqtt.
 *
 * On connect it subscribes to d/<id>/down/#. Requests (down/req/<method>)
 * are written straight into sc_rpc slots, answers to the device's own calls
 * (down/res) go to sc_rpc::deliver_response(); both run on the esp-mqtt task
 * and never publish from it.
 *
 * Publishing is serialised by a mutex so a publish and its MQTT 5 properties
 * stay together. Never call publish() from the esp-mqtt task.
 */
class sc_mqtt
{
public:
    esp_err_t init(const sc_credentials *creds, sc_rpc *_rpc, size_t out_buffer_size);
    esp_err_t start();
    esp_err_t stop();

    bool connected() const
    {
        return is_connected.load();
    }

    /** Called on the esp-mqtt task when the connection comes up (true) or drops (false). */
    void set_connection_cb(const std::function<void(bool)> &cb);

    /** Publish on d/<id>/up/<sub_topic>. Returns ESP_ERR_INVALID_STATE while offline. */
    esp_err_t publish(const char *sub_topic, const void *data, size_t len, int qos, const sc_publish_props *props = nullptr);

    /** Publish on a full topic, which must lie under d/<id>/up/. */
    esp_err_t publish_topic(const char *topic, const void *data, size_t len, int qos, const sc_publish_props *props = nullptr);

    /** "d/<id>/up/" */
    const char *up_prefix() const
    {
        return up_topic;
    }

    /** "d/<id>/down/" */
    const char *down_prefix() const
    {
        return down_topic;
    }

private:
    static void event_handler(void *ctx, esp_event_base_t base, int32_t event_id, void *event_data);
    void handle_data(const esp_mqtt_event_t *evt);
    void start_request(const esp_mqtt_event_t *evt, const char *method, size_t method_len);

    esp_mqtt_client_handle_t client = nullptr;
    sc_rpc *rpc = nullptr;
    SemaphoreHandle_t publish_lock = nullptr;
    std::atomic<bool> is_connected = false;
    std::function<void(bool)> connection_cb = {};
    char up_topic[32] = {};
    char down_topic[32] = {};
    size_t down_topic_len = 0;

    // Reassembly of a message split over several data events (esp-mqtt task only)
    sc_rpc_slot *rx_slot = nullptr;
    bool rx_response = false;
    char rx_correlation[64] = {};
    size_t rx_correlation_len = 0;

    static const constexpr int KEEPALIVE_SEC = 60;
    static const constexpr int RECONNECT_MS = 5000;
    static const constexpr uint32_t SESSION_EXPIRY_SEC = 3600;
    static const constexpr int PUBLISH_LOCK_MS = 10000;
    static const constexpr char *TAG = "sc_mqtt";
};
