#pragma once

#include <cstddef>
#include <cstdint>

#include <esp_err.h>
#include <freertos/FreeRTOS.h>
#include <freertos/event_groups.h>
#include <freertos/queue.h>

#include "soulcloud.hpp"

class sc_mqtt;

/** One request from the platform, written in place by the MQTT task. */
struct sc_rpc_slot {
    bool too_large = false;
    uint16_t correlation_len = 0;
    size_t payload_len = 0;
    char method[48] = {};
    char correlation[64] = {};
    char response_topic[96] = {};
    char payload[CONFIG_SOULCLOUD_RPC_PAYLOAD_MAX + 1] = {};
};

/**
 * Platform requests (cloud -> device) and calls to the platform
 * (device -> cloud), both over MQTT 5 request/response.
 *
 * Requests: slots move between two queues of slot indices. The MQTT task
 * takes a slot from free_slots, writes the request into it and puts it on
 * ready_slots; the request task takes it from there, runs the method
 * handler, publishes {"ok":true,"result":{...}} or {"ok":false,"error":...}
 * to the request's Response Topic with its Correlation Data, and returns the
 * slot to free_slots. A request that finds no free slot is dropped and the
 * platform marks it expired.
 *
 * Calls: call() publishes d/<id>/up/req/<method> and waits for the matching
 * d/<id>/down/res. Only the request task makes calls, one at a time.
 *
 * Memory: slots, queues and buffers are allocated once by init().
 */
class sc_rpc
{
public:
    esp_err_t init(sc_mqtt *_mqtt);

    /** Bits set in the request task's @p _events when a request is ready / a call is answered. */
    void set_events(EventGroupHandle_t _events, EventBits_t _request_bit, EventBits_t _call_bit);

    esp_err_t add_method(const char *name, const sc_method_handler &handler);

    // MQTT task side
    sc_rpc_slot *take_free_slot();
    void submit(sc_rpc_slot *slot);
    void give_back(sc_rpc_slot *slot);
    /** A fragment of d/<id>/down/res; @p offset / @p total as reported by esp-mqtt. */
    void deliver_response(const char *correlation, size_t correlation_len, const char *data, size_t len, size_t offset, size_t total);

    // Request task side
    /** Handle every request waiting in ready_slots. */
    void handle_ready();

    /**
     * Call @p method on the platform. On ESP_OK, @p doc holds the parsed
     * answer and @p result_tok its "result" value. ESP_ERR_TIMEOUT without an
     * answer, ESP_FAIL when the platform answered with an error.
     */
    esp_err_t call(const char *method, const char *params, size_t params_len, sc_json_reader &doc, int &result_tok, uint32_t timeout_ms);

private:
    static const constexpr size_t MAX_METHODS = 24;
    static const constexpr size_t CALL_BUF_SIZE = 1024;

    struct method_entry {
        const char *name = nullptr;
        sc_method_handler handler = {};
    };

    void handle(sc_rpc_slot *slot);
    void respond(const sc_rpc_slot *slot, const char *body, size_t len);
    const method_entry *find_method(const char *name) const;

    sc_mqtt *mqtt = nullptr;
    EventGroupHandle_t events = nullptr;
    EventBits_t request_bit = 0;
    EventBits_t call_bit = 0;

    sc_rpc_slot *slots = nullptr;        // CONFIG_SOULCLOUD_RPC_QUEUE_LEN slots
    QueueHandle_t free_slots = nullptr;  // uint8_t slot indices
    QueueHandle_t ready_slots = nullptr; // uint8_t slot indices, in arrival order
    char *resp_buf = nullptr;            // CONFIG_SOULCLOUD_RPC_RESULT_MAX

    // The one outstanding call(); guarded by call_lock against the MQTT task.
    portMUX_TYPE call_lock = portMUX_INITIALIZER_UNLOCKED;
    char *call_buf = nullptr;       // CALL_BUF_SIZE
    char call_correlation[16] = {}; // Empty while no call is waiting
    size_t call_len = 0;
    bool call_overflow = false;
    uint32_t call_counter = 0;

    method_entry methods[MAX_METHODS] = {};

    static const constexpr char *TAG = "sc_rpc";
};
