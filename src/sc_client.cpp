#include "soulcloud.hpp"

#include <cstring>
#include <ctime>
#include <new>
#include <sys/time.h>

#include <esp_app_desc.h>
#include <esp_heap_caps.h>
#include <esp_log.h>
#include <esp_ota_ops.h>
#include <esp_random.h>
#include <esp_system.h>
#include <on9log.h>
#include <on9log_port.h>
#include <psa/crypto.h>

#include "sc_codec.hpp"
#include "sc_credentials.hpp"
#include "sc_log_uplink.hpp"
#include "sc_mqtt.hpp"
#include "sc_ota.hpp"
#include "sc_rpc.hpp"
#include "sc_shell.hpp"
#include "sc_status.hpp"

esp_err_t sc_client::init(const sc_config &cfg)
{
    psa_crypto_init();
    boot = esp_random();

    creds = new (std::nothrow) sc_credentials();
    if (creds == nullptr) {
        return ESP_ERR_NO_MEM;
    }

    auto ret = creds->load();
    bool have_token = cfg.pairing_token != nullptr && cfg.pairing_token[0] != '\0';
    if ((ret == ESP_ERR_NOT_FOUND || cfg.re_enroll) && have_token) {
        const char *api_url = cfg.api_url != nullptr ? cfg.api_url : CONFIG_SOULCLOUD_API_URL;
        ret = creds->enroll(api_url, cfg.pairing_token, cfg.device_name);
    }
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "init: no credentials: 0x%x %s", ret, esp_err_to_name(ret));
        return ret;
    }
    ESP_LOGI(TAG, "init: device %s, broker %s", creds->device_id(), creds->mqtt_url());

    events = xEventGroupCreate();
    rpc = new (std::nothrow) sc_rpc();
    mqtt = new (std::nothrow) sc_mqtt();
    shell = new (std::nothrow) sc_shell();
    status = new (std::nothrow) sc_status();
    logs = new (std::nothrow) sc_log_uplink();
    ota = new (std::nothrow) sc_ota();
    shell_out = static_cast<char *>(heap_caps_malloc(SHELL_OUT_SIZE, MALLOC_CAP_8BIT));
    if (events == nullptr || rpc == nullptr || mqtt == nullptr || shell == nullptr || status == nullptr || logs == nullptr || ota == nullptr ||
        shell_out == nullptr) {
        ESP_LOGE(TAG, "init: out of memory");
        return ESP_ERR_NO_MEM;
    }

    esp_timer_create_args_t timer_args = {};
    timer_args.callback = [](void *) {
        esp_restart();
    };
    timer_args.name = "sc_restart";
    ret = esp_timer_create(&timer_args, &restart_timer);

    // The MQTT output buffer must hold the largest message we send in one piece.
    size_t out_size =
        CONFIG_SOULCLOUD_LOG_BATCH_SIZE > CONFIG_SOULCLOUD_RPC_RESULT_MAX ? CONFIG_SOULCLOUD_LOG_BATCH_SIZE : CONFIG_SOULCLOUD_RPC_RESULT_MAX;
    ret = ret ?: rpc->init(mqtt);
    ret = ret ?: mqtt->init(creds, rpc, out_size + 256);
    ret = ret ?: status->init(mqtt, boot);
    ret = ret ?: ota->init(mqtt);
    ret = ret ?: logs->init(mqtt, boot);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "init: setup failed: 0x%x %s", ret, esp_err_to_name(ret));
        return ret;
    }

    rpc->set_events(events, BIT_REQUEST, BIT_CALL_DONE);
    mqtt->set_connection_cb([this](bool connected) {
        xEventGroupSetBits(events, connected ? BIT_CONNECTED : BIT_DISCONNECTED);
    });
    shell->add_builtins();
    ret = register_builtin_methods();
    if (ret != ESP_OK) {
        return ret;
    }

    // Stack in internal RAM: handlers may write NVS or flash.
    if (xTaskCreate(task_handler, "soulcloud", CONFIG_SOULCLOUD_RPC_TASK_STACK, this, TASK_PRIORITY, &task) != pdPASS) {
        ESP_LOGE(TAG, "init: can't create task");
        return ESP_ERR_NO_MEM;
    }

    // Let the platform tie the binary logs that follow to this boot.
    on9log_emit_boot(boot, on9log_port_reset_reason());
    return ESP_OK;
}

static void apply_tag_level(const char *tag, int level)
{
    on9log_set_tag_level(tag, static_cast<on9log_level_t>(level));
    esp_log_level_set(tag, static_cast<esp_log_level_t>(level)); // Both use 0 = none ... 5 = verbose
}

esp_err_t sc_client::register_builtin_methods()
{
    auto ret = rpc->add_method("shell", [this](sc_rpc_call &call) {
        char line[520] = {};
        if (!call.params.get_str(0, "line", line, sizeof(line))) {
            call.fail("params.line must be a string of at most %u bytes", sizeof(line) - 1);
            return ESP_ERR_INVALID_ARG;
        }
        sc_shell_out out(shell_out, SHELL_OUT_SIZE);
        int code = shell->run(line, out);
        call.result.add_int("code", code).add_str("output", out.c_str(), out.len());
        if (out.truncated()) {
            call.result.add_bool("truncated", true);
        }
        return ESP_OK;
    });

    ret = ret ?: rpc->add_method("reboot", [this](sc_rpc_call &call) {
        int64_t delay_ms = 500;
        call.params.get_int(0, "delayMs", delay_ms);
        if (delay_ms < 200) {
            delay_ms = 200; // Leave time for the answer to go out
        }
        ESP_LOGW(TAG, "restart requested in %lld ms", delay_ms);
        return esp_timer_start_once(restart_timer, delay_ms * 1000);
    });

    ret = ret ?: rpc->add_method("info", [this](sc_rpc_call &call) {
        const esp_app_desc_t *desc = esp_app_get_description();
        const esp_partition_t *running = esp_ota_get_running_partition();
        auto &out = call.result;
        out.add_str("project", desc->project_name).add_str("version", desc->version).add_str("sdk", desc->idf_ver);
        out.add_fmt("buildTime", "%s %s", desc->date, desc->time);
        out.add_hex("elfSha256", desc->app_elf_sha256, sizeof(desc->app_elf_sha256));
        out.add_str("chip", CONFIG_IDF_TARGET).add_str("partition", running->label);
        out.add_uint("uptimeMs", esp_timer_get_time() / 1000).add_uint("bootId", boot).add_int("resetReason", esp_reset_reason());
        out.begin_object("heap");
        out.add_uint("free", heap_caps_get_free_size(MALLOC_CAP_INTERNAL));
        out.add_uint("minFree", heap_caps_get_minimum_free_size(MALLOC_CAP_INTERNAL));
        out.add_uint("largestBlock", heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL));
        out.add_uint("psramFree", heap_caps_get_free_size(MALLOC_CAP_SPIRAM));
        out.end_object();
        return ESP_OK;
    });

    ret = ret ?: rpc->add_method("log.config", [this](sc_rpc_call &call) {
        sc_log_config cfg = {};
        if (sc_codec::decode_log_config(call.params, cfg, apply_tag_level) != ESP_OK) {
            call.fail("bad log.config params");
            return ESP_ERR_INVALID_ARG;
        }
        if (cfg.level >= 0) {
            on9log_set_level(static_cast<on9log_level_t>(cfg.level));
            esp_log_level_set("*", static_cast<esp_log_level_t>(cfg.level));
        }
        if (cfg.text >= 0) {
            logs->set_text_enabled(cfg.text == 1);
        }
        if (cfg.bin >= 0) {
            logs->set_bin_enabled(cfg.bin == 1);
        }
        return ESP_OK;
    });

    ret = ret ?: rpc->add_method("ota.start", [this](sc_rpc_call &call) {
        sc_ota_params params = {};
        if (sc_codec::decode_ota_params(call.params, 0, params) != ESP_OK) {
            call.fail("bad ota.start params");
            return ESP_ERR_INVALID_ARG;
        }
        if (ota->start(params) == ESP_ERR_INVALID_STATE) {
            call.fail("an update is already running");
            return ESP_ERR_INVALID_STATE;
        }
        return ESP_OK;
    });

    ret = ret ?: rpc->add_method("ota.cancel", [this](sc_rpc_call &call) {
        char task_id[40] = {};
        call.params.get_str(0, "taskId", task_id, sizeof(task_id));
        if (ota->cancel(task_id) != ESP_OK) {
            call.fail("no such update running");
            return ESP_ERR_NOT_FOUND;
        }
        return ESP_OK;
    });

    ret = ret ?: shell->add("reset main_device", "Restart the device", [this](int, char **, sc_shell_out &out) {
        out.printf("Prepare to restart system!\n");
        esp_timer_start_once(restart_timer, 500 * 1000);
        return 0;
    });

    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "register_builtin_methods: 0x%x %s", ret, esp_err_to_name(ret));
    }
    return ret;
}

esp_err_t sc_client::start()
{
    return mqtt != nullptr ? mqtt->start() : ESP_ERR_INVALID_STATE;
}

esp_err_t sc_client::stop()
{
    return mqtt != nullptr ? mqtt->stop() : ESP_ERR_INVALID_STATE;
}

esp_err_t sc_client::erase_credentials()
{
    if (creds == nullptr) {
        sc_credentials temp;
        return temp.erase();
    }
    return creds->erase();
}

esp_err_t sc_client::register_method(const char *name, const sc_method_handler &handler)
{
    return rpc != nullptr ? rpc->add_method(name, handler) : ESP_ERR_INVALID_STATE;
}

esp_err_t sc_client::register_shell_cmd(const char *name, const char *help, const sc_shell_handler &handler)
{
    return shell != nullptr ? shell->add(name, help, handler) : ESP_ERR_INVALID_STATE;
}

void sc_client::set_status_cb(const sc_status_cb &cb)
{
    if (status != nullptr) {
        status->set_extra_cb(cb);
    }
}

void sc_client::set_connection_cb(const sc_connection_cb &cb)
{
    connection_cb = cb;
}

esp_err_t sc_client::publish_event(const char *name, const char *json, size_t len)
{
    if (mqtt == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }

    char sub_topic[48] = {};
    snprintf(sub_topic, sizeof(sub_topic), "evt/%s", name);
    sc_publish_props props = {};
    props.content_type = sc_codec::CONTENT_TYPE;
    return mqtt->publish(sub_topic, json, len, 1, &props);
}

void sc_client::request_status_report()
{
    if (events != nullptr) {
        xEventGroupSetBits(events, BIT_REPORT_STATUS);
    }
}

bool sc_client::is_connected() const
{
    return mqtt != nullptr && mqtt->connected();
}

const char *sc_client::device_id() const
{
    return creds != nullptr ? creds->device_id() : "";
}

void sc_client::task_handler(void *ctx)
{
    static_cast<sc_client *>(ctx)->run_task();
}

void sc_client::run_task()
{
    const TickType_t heartbeat = pdMS_TO_TICKS(CONFIG_SOULCLOUD_HEARTBEAT_INTERVAL_SEC * 1000);
    TickType_t next_report = xTaskGetTickCount() + heartbeat;

    while (true) {
        auto until_report = static_cast<int32_t>(next_report - xTaskGetTickCount());
        EventBits_t bits = xEventGroupWaitBits(
            events, BIT_REQUEST | BIT_CONNECTED | BIT_DISCONNECTED | BIT_REPORT_STATUS, pdTRUE, pdFALSE, until_report > 0 ? until_report : 0
        );

        if (bits & BIT_CONNECTED) {
            on_connected();
            next_report = xTaskGetTickCount() + heartbeat;
        }
        if ((bits & BIT_DISCONNECTED) && connection_cb) {
            connection_cb(false);
        }

        rpc->handle_ready();

        bool report_due = static_cast<int32_t>(next_report - xTaskGetTickCount()) <= 0;
        if ((bits & BIT_REPORT_STATUS) || report_due) {
            if (mqtt->connected()) {
                status->report(false);
            }
            next_report = xTaskGetTickCount() + heartbeat;
        }
    }
}

void sc_client::on_connected()
{
    auto ret = status->report(true);
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "birth report failed: 0x%x %s", ret, esp_err_to_name(ret));
    }

    if (first_connection) {
        first_connection = false;
        confirm_running_image();
        sync_time();
        check_for_update();
    }
    if (connection_cb) {
        connection_cb(true);
    }
}

void sc_client::confirm_running_image()
{
#if CONFIG_SOULCLOUD_MARK_APP_VALID
    esp_ota_img_states_t state = ESP_OTA_IMG_UNDEFINED;
    if (esp_ota_get_state_partition(esp_ota_get_running_partition(), &state) == ESP_OK && state == ESP_OTA_IMG_PENDING_VERIFY) {
        ESP_LOGI(TAG, "new firmware reached the platform; confirming it");
        esp_ota_mark_app_valid_cancel_rollback();
    }
#endif
}

void sc_client::sync_time()
{
    // Only fill in a clock nobody has set yet (SNTP or RTC take precedence).
    if (time(nullptr) > 1700000000) {
        return;
    }

    sc_json_doc<16> doc;
    int result = -1;
    int64_t epoch_ms = 0;
    if (rpc->call("time", "{}", 2, doc, result, CALL_TIMEOUT_MS) != ESP_OK || !doc.get_int(result, "epochMs", epoch_ms)) {
        ESP_LOGW(TAG, "sync_time: no time from the platform");
        return;
    }

    timeval now = {};
    now.tv_sec = static_cast<time_t>(epoch_ms / 1000);
    now.tv_usec = static_cast<suseconds_t>((epoch_ms % 1000) * 1000);
    settimeofday(&now, nullptr);
    ESP_LOGI(TAG, "sync_time: clock set from the platform");
}

void sc_client::check_for_update()
{
    sc_json_doc<24> doc;
    int result = -1;
    if (rpc->call("ota.check", "{}", 2, doc, result, CALL_TIMEOUT_MS) != ESP_OK) {
        ESP_LOGW(TAG, "check_for_update: no answer");
        return;
    }

    int update = doc.find(result, "update");
    if (!doc.is_object(update)) {
        return; // null: nothing pending
    }

    sc_ota_params params = {};
    if (sc_codec::decode_ota_params(doc, update, params) == ESP_OK) {
        ESP_LOGI(TAG, "check_for_update: update %s pending, starting", params.version);
        ota->start(params);
    }
}
