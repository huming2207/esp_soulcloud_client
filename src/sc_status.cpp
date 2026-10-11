#include "sc_status.hpp"

#include <esp_app_desc.h>
#include <esp_heap_caps.h>
#include <esp_log.h>
#include <esp_system.h>
#include <esp_timer.h>

#include "sc_codec.hpp"
#include "sc_mqtt.hpp"

esp_err_t sc_status::init(sc_mqtt *_mqtt, uint32_t _boot_id)
{
    mqtt = _mqtt;
    boot_id = _boot_id;
    return ESP_OK;
}

void sc_status::set_extra_cb(const sc_status_cb &cb)
{
    extra_cb = cb;
}

esp_err_t sc_status::report(bool birth)
{
    const esp_app_desc_t *desc = esp_app_get_description();
    sc_status_msg msg = {};
    msg.birth = birth;
    msg.uptime_ms = esp_timer_get_time() / 1000;
    msg.boot_id = boot_id;
    msg.reset_reason = esp_reset_reason();
    msg.fw_version = desc->version;
    msg.project = desc->project_name;
    msg.sdk_version = desc->idf_ver;
    msg.elf_sha256 = desc->app_elf_sha256;
    msg.chip = CONFIG_IDF_TARGET;
    msg.heap_free = heap_caps_get_free_size(MALLOC_CAP_INTERNAL);
    msg.heap_min_free = heap_caps_get_minimum_free_size(MALLOC_CAP_INTERNAL);

    size_t len = 0;
    auto ret = sc_codec::encode_status(msg, extra_cb, buf, sizeof(buf), &len);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "report: status larger than %u bytes", sizeof(buf));
        return ret;
    }

    sc_publish_props props = {};
    props.content_type = sc_codec::CONTENT_TYPE;
    return mqtt->publish("status", buf, len, birth ? 1 : 0, &props);
}
