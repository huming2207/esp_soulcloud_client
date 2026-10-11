#include "sc_rpc.hpp"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <new>

#include <esp_heap_caps.h>
#include <esp_log.h>

#include "sc_codec.hpp"
#include "sc_mqtt.hpp"

void sc_rpc_call::fail(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    vsnprintf(error_msg, sizeof(error_msg), fmt, args);
    va_end(args);
    if (error_msg[0] == '\0') {
        strlcpy(error_msg, "failed", sizeof(error_msg));
    }
}

esp_err_t sc_rpc::init(sc_mqtt *_mqtt)
{
    mqtt = _mqtt;
    slots = static_cast<sc_rpc_slot *>(heap_caps_calloc(CONFIG_SOULCLOUD_RPC_QUEUE_LEN, sizeof(sc_rpc_slot), MALLOC_CAP_8BIT));
    resp_buf = static_cast<char *>(heap_caps_malloc(CONFIG_SOULCLOUD_RPC_RESULT_MAX, MALLOC_CAP_8BIT));
    call_buf = static_cast<char *>(heap_caps_malloc(CALL_BUF_SIZE, MALLOC_CAP_8BIT));
    free_slots = xQueueCreate(CONFIG_SOULCLOUD_RPC_QUEUE_LEN, sizeof(uint8_t));
    ready_slots = xQueueCreate(CONFIG_SOULCLOUD_RPC_QUEUE_LEN, sizeof(uint8_t));
    if (slots == nullptr || resp_buf == nullptr || call_buf == nullptr || free_slots == nullptr || ready_slots == nullptr) {
        ESP_LOGE(TAG, "init: out of memory");
        return ESP_ERR_NO_MEM;
    }

    for (uint8_t idx = 0; idx < CONFIG_SOULCLOUD_RPC_QUEUE_LEN; idx++) {
        new (&slots[idx]) sc_rpc_slot();
        xQueueSend(free_slots, &idx, 0);
    }
    return ESP_OK;
}

void sc_rpc::set_events(EventGroupHandle_t _events, EventBits_t _request_bit, EventBits_t _call_bit)
{
    events = _events;
    request_bit = _request_bit;
    call_bit = _call_bit;
}

esp_err_t sc_rpc::add_method(const char *name, const sc_method_handler &handler)
{
    if (name == nullptr || !handler) {
        return ESP_ERR_INVALID_ARG;
    }

    for (auto &entry : methods) {
        if (entry.name == nullptr || strcmp(entry.name, name) == 0) {
            entry.name = name;
            entry.handler = handler;
            return ESP_OK;
        }
    }
    return ESP_ERR_NO_MEM;
}

const sc_rpc::method_entry *sc_rpc::find_method(const char *name) const
{
    for (const auto &entry : methods) {
        if (entry.name != nullptr && strcmp(entry.name, name) == 0) {
            return &entry;
        }
    }
    return nullptr;
}

sc_rpc_slot *sc_rpc::take_free_slot()
{
    uint8_t idx = 0;
    if (xQueueReceive(free_slots, &idx, 0) != pdTRUE) {
        return nullptr;
    }

    sc_rpc_slot *slot = &slots[idx];
    slot->too_large = false;
    slot->correlation_len = 0;
    slot->payload_len = 0;
    slot->method[0] = '\0';
    slot->response_topic[0] = '\0';
    return slot;
}

void sc_rpc::submit(sc_rpc_slot *slot)
{
    auto idx = static_cast<uint8_t>(slot - slots);
    xQueueSend(ready_slots, &idx, 0); // Cannot fail: there are as many queue entries as slots
    xEventGroupSetBits(events, request_bit);
}

void sc_rpc::give_back(sc_rpc_slot *slot)
{
    auto idx = static_cast<uint8_t>(slot - slots);
    xQueueSend(free_slots, &idx, 0);
}

void sc_rpc::handle_ready()
{
    uint8_t idx = 0;
    while (xQueueReceive(ready_slots, &idx, 0) == pdTRUE) {
        handle(&slots[idx]);
        give_back(&slots[idx]);
    }
}

void sc_rpc::handle(sc_rpc_slot *slot)
{
    sc_json_writer out(resp_buf, CONFIG_SOULCLOUD_RPC_RESULT_MAX);
    char error[112] = {};
    const method_entry *entry = find_method(slot->method);

    // An empty payload means no parameters.
    const char *params_json = slot->payload_len > 0 ? slot->payload : "{}";
    size_t params_len = slot->payload_len > 0 ? slot->payload_len : 2;
    sc_json_doc<32> params;

    if (slot->too_large) {
        snprintf(error, sizeof(error), "request larger than %d bytes", CONFIG_SOULCLOUD_RPC_PAYLOAD_MAX);
    } else if (entry == nullptr) {
        snprintf(error, sizeof(error), "unknown method %s", slot->method);
    } else if (params.parse(params_json, params_len) != ESP_OK) {
        strlcpy(error, "params must be a JSON object", sizeof(error));
    } else {
        out.begin_object().add_bool("ok", true).begin_object("result");
        sc_rpc_call call(slot->method, params, out);
        auto ret = entry->handler(call);
        out.end_object().end_object();

        if (call.error() != nullptr) {
            strlcpy(error, call.error(), sizeof(error));
        } else if (ret != ESP_OK) {
            strlcpy(error, esp_err_to_name(ret), sizeof(error));
        } else if (!out.ok()) {
            snprintf(error, sizeof(error), "result larger than %d bytes", CONFIG_SOULCLOUD_RPC_RESULT_MAX);
        }
    }

    if (error[0] != '\0') {
        ESP_LOGW(TAG, "handle: %s failed: %s", slot->method, error);
        out.reset();
        out.begin_object().add_bool("ok", false).add_str("error", error).end_object();
    }
    respond(slot, out.c_str(), out.len());
}

void sc_rpc::respond(const sc_rpc_slot *slot, const char *body, size_t len)
{
    sc_publish_props props = {};
    props.content_type = sc_codec::CONTENT_TYPE;
    props.correlation = slot->correlation;
    props.correlation_len = slot->correlation_len;

    // Answer on the Response Topic the platform named; publish_topic() refuses foreign topics.
    esp_err_t ret = ESP_ERR_INVALID_ARG;
    if (slot->response_topic[0] != '\0') {
        ret = mqtt->publish_topic(slot->response_topic, body, len, 1, &props);
    }
    if (ret == ESP_ERR_INVALID_ARG) {
        ret = mqtt->publish("res", body, len, 1, &props);
    }
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "respond: %s answer not sent: 0x%x %s", slot->method, ret, esp_err_to_name(ret));
    }
}

void sc_rpc::deliver_response(const char *correlation, size_t correlation_len, const char *data, size_t len, size_t offset, size_t total)
{
    bool complete = false;
    portENTER_CRITICAL(&call_lock);
    bool ours =
        call_correlation[0] != '\0' && correlation_len == strlen(call_correlation) && memcmp(correlation, call_correlation, correlation_len) == 0;
    if (ours) {
        if (offset + len < CALL_BUF_SIZE) {
            memcpy(call_buf + offset, data, len);
            call_len = offset + len;
        } else {
            call_overflow = true;
        }
        complete = offset + len >= total;
    }
    portEXIT_CRITICAL(&call_lock);

    if (complete) {
        xEventGroupSetBits(events, call_bit);
    }
}

esp_err_t sc_rpc::call(const char *method, const char *params, size_t params_len, sc_json_reader &doc, int &result_tok, uint32_t timeout_ms)
{
    // Dotted method names travel as topic levels: ota.check -> up/req/ota/check
    char sub_topic[64] = {};
    snprintf(sub_topic, sizeof(sub_topic), "req/%s", method);
    for (char *c = sub_topic; *c != '\0'; c++) {
        *c = *c == '.' ? '/' : *c;
    }
    char response_topic[48] = {};
    snprintf(response_topic, sizeof(response_topic), "%sres", mqtt->down_prefix());

    char correlation[sizeof(call_correlation)] = {};
    snprintf(correlation, sizeof(correlation), "c%lu", static_cast<unsigned long>(++call_counter));
    portENTER_CRITICAL(&call_lock);
    strlcpy(call_correlation, correlation, sizeof(call_correlation));
    call_len = 0;
    call_overflow = false;
    portEXIT_CRITICAL(&call_lock);
    xEventGroupClearBits(events, call_bit);

    sc_publish_props props = {};
    props.content_type = sc_codec::CONTENT_TYPE;
    props.correlation = correlation;
    props.correlation_len = strlen(correlation);
    props.response_topic = response_topic;
    auto ret = mqtt->publish(sub_topic, params, params_len, 1, &props);
    if (ret == ESP_OK) {
        EventBits_t bits = xEventGroupWaitBits(events, call_bit, pdTRUE, pdTRUE, pdMS_TO_TICKS(timeout_ms));
        ret = (bits & call_bit) != 0 ? ESP_OK : ESP_ERR_TIMEOUT;
    }

    portENTER_CRITICAL(&call_lock);
    call_correlation[0] = '\0';
    bool overflow = call_overflow;
    size_t len = call_len;
    portEXIT_CRITICAL(&call_lock);

    if (ret != ESP_OK) {
        return ret;
    }
    if (overflow) {
        return ESP_ERR_INVALID_SIZE;
    }

    ret = doc.parse(call_buf, len);
    if (ret != ESP_OK) {
        return ret;
    }

    bool ok = false;
    if (!doc.get_bool(0, "ok", ok) || !ok) {
        char error[96] = {};
        doc.get_str(0, "error", error, sizeof(error));
        ESP_LOGW(TAG, "call: %s refused: %s", method, error);
        return ESP_FAIL;
    }
    result_tok = doc.find(0, "result");
    return ESP_OK;
}
