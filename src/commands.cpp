/*
 * Command registry and dispatch.
 */

#include "commands.hpp"

#include <cstring>

#include <esp_log.h>

#include "protocol.hpp"

using namespace soulcloud;

esp_err_t soulcloud::command_registry::register_command(const char *name, command_handler_t handler, void *ctx)
{
    if (name == nullptr || name[0] == '\0' || handler == nullptr) {
        return ESP_ERR_INVALID_ARG;
    }
    for (uint32_t i = 0; i < count; ++i) {
        if (strcmp(entries[i].name, name) == 0) {
            entries[i].handler = handler;  // replace
            entries[i].ctx = ctx;
            return ESP_OK;
        }
    }
    if (count >= MAX_COMMANDS) {
        ESP_LOGE(TAG, "command registry full (%u)", MAX_COMMANDS);
        return ESP_ERR_NO_MEM;
    }
    entries[count].name = name;
    entries[count].handler = handler;
    entries[count].ctx = ctx;
    count++;
    return ESP_OK;
}

soulcloud::command_registry::recent_entry *soulcloud::command_registry::recent_find(const uint8_t *id)
{
    for (uint32_t i = 0; i < RECENT_CACHE; ++i) {
        if (recent[i].valid && memcmp(recent[i].id, id, 16) == 0) {
            return &recent[i];
        }
    }
    return nullptr;
}

void soulcloud::command_registry::recent_put(const uint8_t *id, int32_t code)
{
    recent_entry *e = &recent[recent_head];
    recent_head = (recent_head + 1) % RECENT_CACHE;
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
        // Rejections must be visible to the platform: a command dropped
        // device-side would otherwise hang until the platform timeout,
        // with no way to tell "not received" from "rejected". Only
        // possible when the id is extractable; a payload without a
        // usable id cannot be answered at all.
        uint8_t id[16] = {};
        if (decode_command_id(payload, len, id) == ERR_OK) {
            command_result result = {};
            result.id = id;
            result.seq = 0;
            result.args = nullptr;
            result.arg_count = 0;
            result.code = CMD_RESULT_ERR_DECODE;
            size_t out_len = 0;
            return encode_command_result(out_buf, out_cap, &out_len, &result) == ERR_OK
                       ? (int32_t)out_len
                       : ERR_OVERFLOW;
        }
        ESP_LOGW(TAG, "decode cmd/exec failed: %d", rc);
        return rc;
    }

    command_result result = {};
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
    void *ctx = nullptr;
    for (uint32_t i = 0; i < count; ++i) {
        const size_t nlen = strlen(entries[i].name);
        if (nlen == exec.cmd_len && memcmp(entries[i].name, exec.cmd, nlen) == 0) {
            handler = entries[i].handler;
            ctx = entries[i].ctx;
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

    const esp_err_t err = handler(&exec, &result, ctx);
    result.code = (err == ESP_OK) ? result.code : -2;  // internal error
    recent_put(exec.id, result.code);

    size_t out_len = 0;
    const int32_t enc = encode_command_result(out_buf, out_cap, &out_len, &result);
    return enc == ERR_OK ? (int32_t)out_len : enc;
}
