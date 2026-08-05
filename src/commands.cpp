/*
 * Command registry and dispatch.
 */

#include "commands.hpp"

#include <cstring>

#include <esp_log.h>

#include "protocol.hpp"

using namespace soulcloud;

esp_err_t soulcloud::command_registry::register_command(const char *name, command_handler_t handler)
{
    if (name == nullptr || name[0] == '\0' || handler == nullptr) {
        return ESP_ERR_INVALID_ARG;
    }
    for (uint32_t i = 0; i < count_; ++i) {
        if (strcmp(entries_[i].name, name) == 0) {
            entries_[i].handler = handler;  // replace
            return ESP_OK;
        }
    }
    if (count_ >= MAX_COMMANDS) {
        ESP_LOGE(TAG, "command registry full (%u)", MAX_COMMANDS);
        return ESP_ERR_NO_MEM;
    }
    entries_[count_].name = name;
    entries_[count_].handler = handler;
    count_++;
    return ESP_OK;
}

soulcloud::command_registry::recent_entry *soulcloud::command_registry::recent_find(const uint8_t *id)
{
    for (uint32_t i = 0; i < RECENT_CACHE; ++i) {
        if (recent_[i].valid && memcmp(recent_[i].id, id, 16) == 0) {
            return &recent_[i];
        }
    }
    return nullptr;
}

void soulcloud::command_registry::recent_put(const uint8_t *id, int32_t code)
{
    recent_entry *e = &recent_[recent_head_];
    recent_head_ = (recent_head_ + 1) % RECENT_CACHE;
    memcpy(e->id, id, 16);
    e->code = code;
    e->valid = true;
}

int32_t soulcloud::command_registry::dispatch(const uint8_t *payload, size_t len,
                                   uint8_t *out_buf, size_t out_cap)
{
    command_exec exec;
    const int32_t rc = decode_command_exec(payload, len, &exec);
    if (rc != ERR_OK) {
        ESP_LOGW(TAG, "decode cmd/exec failed: %d", rc);
        return rc;
    }

    command_result result;
    result.id = exec.id;
    result.seq = exec.seq;
    result.args = nullptr;
    result.arg_count = 0;

    // QoS1 redelivery: answer from cache, never re-execute
    if (recent_entry *cached = recent_find(exec.id); cached != nullptr) {
        ESP_LOGD(TAG, "command %.*s redelivered; answering from cache (code %ld)",
                 (int)exec.cmd_len, exec.cmd, (long)cached->code);
        result.code = cached->code;
        size_t out_len = 0;
        return soulcloud::encode_command_result(out_buf, out_cap, &out_len, &result) == ERR_OK
                   ? (int32_t)out_len
                   : ERR_OVERFLOW;
    }

    // find the handler
    command_handler_t handler = nullptr;
    for (uint32_t i = 0; i < count_; ++i) {
        const size_t nlen = strlen(entries_[i].name);
        if (nlen == exec.cmd_len && memcmp(entries_[i].name, exec.cmd, nlen) == 0) {
            handler = entries_[i].handler;
            break;
        }
    }

    if (handler == nullptr) {
        ESP_LOGW(TAG, "unknown command %.*s", (int)exec.cmd_len, exec.cmd);
        result.code = -1;  // unknown command
        recent_put(exec.id, -1);
        size_t out_len = 0;
        return soulcloud::encode_command_result(out_buf, out_cap, &out_len, &result) == ERR_OK
                   ? (int32_t)out_len
                   : ERR_OVERFLOW;
    }

    const esp_err_t err = handler(&exec, &result);
    result.code = (err == ESP_OK) ? result.code : -2;  // internal error
    recent_put(exec.id, result.code);

    size_t out_len = 0;
    const int32_t enc = encode_command_result(out_buf, out_cap, &out_len, &result);
    return enc == ERR_OK ? (int32_t)out_len : enc;
}
