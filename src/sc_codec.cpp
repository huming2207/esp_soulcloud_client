#include "sc_codec.hpp"

#include <cstring>

#include <esp_log.h>

static esp_err_t finish(const sc_json_writer &out, size_t *out_len)
{
    if (!out.ok()) {
        return ESP_ERR_NO_MEM;
    }
    *out_len = out.len();
    return ESP_OK;
}

esp_err_t sc_codec::encode_status(const sc_status_msg &msg, const sc_status_cb &extra, char *buf, size_t cap, size_t *out_len)
{
    sc_json_writer out(buf, cap);
    out.begin_object();
    if (msg.birth) {
        out.add_bool("birth", true);
    }
    out.add_uint("uptimeMs", msg.uptime_ms);
    out.add_uint("bootId", msg.boot_id);
    out.add_int("resetReason", msg.reset_reason);
    out.begin_object("fw");
    out.add_str("version", msg.fw_version);
    if (msg.elf_sha256 != nullptr) {
        out.add_hex("elfSha256", msg.elf_sha256, 32);
    }
    out.add_str("project", msg.project);
    out.add_str("sdk", msg.sdk_version);
    out.end_object();
    out.add_str("chip", msg.chip);
    out.begin_object("heap").add_uint("free", msg.heap_free).add_uint("minFree", msg.heap_min_free).end_object();
    if (extra) {
        extra(out);
    }
    out.end_object();
    return finish(out, out_len);
}

esp_err_t sc_codec::encode_ota_event(const sc_ota_event &evt, char *buf, size_t cap, size_t *out_len)
{
    sc_json_writer out(buf, cap);
    out.begin_object().add_str("taskId", evt.task_id).add_str("state", evt.state);
    if (evt.progress >= 0) {
        out.add_int("progress", evt.progress);
    }
    if (evt.error != nullptr) {
        out.add_str("error", evt.error);
    }
    out.end_object();
    return finish(out, out_len);
}

esp_err_t sc_codec::encode_enroll_request(const sc_enroll_request &req, char *buf, size_t cap, size_t *out_len)
{
    sc_json_writer out(buf, cap);
    out.begin_object();
    out.add_str("token", req.token).add_str("hardwareId", req.hardware_id).add_str("platform", req.platform);
    out.add_str("chip", req.chip).add_str("fwVersion", req.fw_version);
    if (req.name != nullptr && req.name[0] != '\0') {
        out.add_str("name", req.name);
    }
    out.end_object();
    return finish(out, out_len);
}

esp_err_t sc_codec::decode_enroll_response(const char *payload, size_t len, sc_enroll_response &out)
{
    sc_json_doc<48> doc;
    auto ret = doc.parse(payload, len);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "decode_enroll_response: bad JSON: 0x%x %s", ret, esp_err_to_name(ret));
        return ret;
    }

    int mqtt = doc.find(0, "mqtt");
    bool ok = doc.get_str(0, "deviceId", out.device_id, sizeof(out.device_id));
    ok = ok && doc.get_str(mqtt, "password", out.password, sizeof(out.password));
    ok = ok && doc.get_str(mqtt, "url", out.mqtt_url, sizeof(out.mqtt_url));
    return ok ? ESP_OK : ESP_ERR_INVALID_RESPONSE;
}

esp_err_t sc_codec::decode_ota_params(const sc_json_reader &doc, int object, sc_ota_params &out)
{
    int64_t size = 0;
    bool ok = doc.get_str(object, "url", out.url, sizeof(out.url));
    ok = ok && doc.get_str(object, "sha256", out.sha256, sizeof(out.sha256)) && strlen(out.sha256) == 64;
    ok = ok && doc.get_int(object, "size", size) && size > 0 && size <= UINT32_MAX;
    ok = ok && doc.get_str(object, "taskId", out.task_id, sizeof(out.task_id));
    if (!ok) {
        return ESP_ERR_INVALID_ARG;
    }

    out.size = static_cast<uint32_t>(size);
    // The version is informational: a missing or over-long one is left empty.
    if (!doc.get_str(object, "version", out.version, sizeof(out.version))) {
        out.version[0] = '\0';
    }
    return ESP_OK;
}

int sc_codec::level_from_name(const char *name)
{
    static const constexpr char *NAMES[] = {"none", "error", "warn", "info", "debug", "verbose"};
    for (int idx = 0; idx < 6; idx++) {
        if (strcmp(name, NAMES[idx]) == 0) {
            return idx;
        }
    }
    return -1;
}

esp_err_t sc_codec::decode_log_config(const sc_json_reader &doc, sc_log_config &out, void (*set_tag)(const char *tag, int level))
{
    char name[16] = {};
    if (doc.get_str(0, "level", name, sizeof(name))) {
        out.level = level_from_name(name);
        if (out.level < 0) {
            return ESP_ERR_INVALID_ARG;
        }
    }

    bool flag = false;
    if (doc.get_bool(0, "text", flag)) {
        out.text = flag ? 1 : 0;
    }
    if (doc.get_bool(0, "bin", flag)) {
        out.bin = flag ? 1 : 0;
    }

    int tags = doc.find(0, "tags");
    if (tags < 0) {
        return ESP_OK;
    }
    if (!doc.is_object(tags)) {
        return ESP_ERR_INVALID_ARG;
    }

    // Two passes over {"tag": "level", ...}: check every entry, then apply them all.
    for (int pass = 0; pass < 2; pass++) {
        int member = tags + 1;
        for (int idx = 0; idx < doc.child_count(tags) && member >= 0; idx++) {
            char tag[32] = {};
            if (!doc.str_at(member, tag, sizeof(tag)) || !doc.str_at(member + 1, name, sizeof(name)) || level_from_name(name) < 0) {
                return ESP_ERR_INVALID_ARG;
            }
            if (pass == 1) {
                set_tag(tag, level_from_name(name));
            }
            member = doc.skip(member + 1);
        }
    }
    return ESP_OK;
}
