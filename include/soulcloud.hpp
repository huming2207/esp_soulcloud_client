#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

#include <esp_err.h>
#include <esp_timer.h>
#include <freertos/FreeRTOS.h>
#include <freertos/event_groups.h>
#include <freertos/task.h>

#include "sc_json.hpp"

/** Client settings passed to sc_client::init(). */
struct sc_config {
    /** Platform base URL for enrollment; defaults to CONFIG_SOULCLOUD_API_URL. */
    const char *api_url = nullptr;
    /**
     * Pairing token from the user's provisioning app. When set and the device
     * holds no credentials (or @c re_enroll is set), init() redeems it first.
     */
    const char *pairing_token = nullptr;
    /** Optional display name sent with enrollment. */
    const char *device_name = nullptr;
    /** Redeem @c pairing_token even when credentials are stored. */
    bool re_enroll = false;
};

/**
 * One request from the platform, handed to a method handler on the request
 * task. Read the parameters through @c params (root object = token 0) and add
 * result fields to @c result, which is already inside the result object.
 * Return an error, or call fail(), to answer {"ok":false,"error":...}.
 */
class sc_rpc_call
{
public:
    sc_rpc_call(const char *_method, const sc_json_reader &_params, sc_json_writer &_result) : method(_method), params(_params), result(_result) {}

    /** Fail the call with a message (printf format). */
    void fail(const char *fmt, ...) __attribute__((format(printf, 2, 3)));

    const char *error() const
    {
        return error_msg[0] != '\0' ? error_msg : nullptr;
    }

    const char *method;
    const sc_json_reader &params;
    sc_json_writer &result;

private:
    char error_msg[96] = {};
};

using sc_method_handler = std::function<esp_err_t(sc_rpc_call &call)>;

/** Text output of one shell command; truncated when full. */
class sc_shell_out
{
public:
    sc_shell_out(char *_buf, size_t _cap);

    void printf(const char *fmt, ...) __attribute__((format(printf, 2, 3)));
    void write(const char *str, size_t len);

    const char *c_str() const
    {
        return buf;
    }

    size_t len() const
    {
        return pos;
    }

    bool truncated() const
    {
        return full;
    }

private:
    char *buf = nullptr;
    size_t cap = 0;
    size_t pos = 0;
    bool full = false;
};

/**
 * A shell command. @p argv holds the words after the command name, split on
 * spaces in place. Return the exit code (0 = success).
 */
using sc_shell_handler = std::function<int(int argc, char **argv, sc_shell_out &out)>;

/** Adds fields to the status (birth/heartbeat) object, e.g. "rssi" or an "extra" object. */
using sc_status_cb = std::function<void(sc_json_writer &status)>;

/** Called on the request task when the platform connection comes up or goes down. */
using sc_connection_cb = std::function<void(bool connected)>;

class sc_credentials;
class sc_mqtt;
class sc_rpc;
class sc_shell;
class sc_status;
class sc_log_uplink;
class sc_ota;

/**
 * SoulCloud device client.
 *
 * Connects to the platform with MQTT 5 over WebSocket, answers its requests
 * (remote shell, reboot, info, log levels, OTA and app-defined methods),
 * reports status and uptime, and uploads ESP_LOG text and on9log binary logs.
 * See the platform's docs/device-protocol.md for the wire contract.
 *
 * Memory: every buffer is allocated once by init(). Request handling, status
 * reports and calls to the platform run on one request task; logs are
 * uploaded by their own task; an OTA download runs on a task created for it.
 *
 * Thread safety: register methods, shell commands and callbacks before
 * start(). Other calls may come from any task.
 */
class sc_client
{
public:
    static sc_client *instance()
    {
        static sc_client _instance;
        return &_instance;
    }

    sc_client(sc_client const &) = delete;
    void operator=(sc_client const &) = delete;

    /**
     * Load the stored credentials, or enroll with @p cfg's pairing token
     * (needs the network). ESP_ERR_NOT_FOUND when the device is not enrolled
     * and no token was given.
     */
    esp_err_t init(const sc_config &cfg);

    /** Connect to the platform; reconnects automatically afterwards. */
    esp_err_t start();
    esp_err_t stop();

    /** Forget the credentials; the device must be enrolled again. */
    esp_err_t erase_credentials();

    /** Answer requests for @p name (dotted, e.g. "gpio.read"). @p name must stay valid. */
    esp_err_t register_method(const char *name, const sc_method_handler &handler);

    /** Add a shell command; @p name may be several words ("get sensor"). Strings must stay valid. */
    esp_err_t register_shell_cmd(const char *name, const char *help, const sc_shell_handler &handler);

    void set_status_cb(const sc_status_cb &cb);
    void set_connection_cb(const sc_connection_cb &cb);

    /** Publish a JSON event on d/<id>/up/evt/<name>. */
    esp_err_t publish_event(const char *name, const char *json, size_t len);

    /** Send a status report now (outside the heartbeat schedule). */
    void request_status_report();

    bool is_connected() const;
    const char *device_id() const;
    uint32_t boot_id() const
    {
        return boot;
    }

private:
    sc_client() = default;

    enum event_bits : uint32_t {
        BIT_REQUEST = (1UL << 0UL),   // A platform request is waiting
        BIT_CALL_DONE = (1UL << 1UL), // The platform answered our call
        BIT_CONNECTED = (1UL << 2UL),
        BIT_DISCONNECTED = (1UL << 3UL),
        BIT_REPORT_STATUS = (1UL << 4UL),
    };

    esp_err_t register_builtin_methods();
    static void task_handler(void *ctx);
    void run_task();
    void on_connected();
    void sync_time();
    void check_for_update();
    static void confirm_running_image();

    sc_credentials *creds = nullptr;
    sc_mqtt *mqtt = nullptr;
    sc_rpc *rpc = nullptr;
    sc_shell *shell = nullptr;
    sc_status *status = nullptr;
    sc_log_uplink *logs = nullptr;
    sc_ota *ota = nullptr;
    char *shell_out = nullptr; // SHELL_OUT_SIZE, allocated once
    TaskHandle_t task = nullptr;
    EventGroupHandle_t events = nullptr;
    esp_timer_handle_t restart_timer = nullptr;
    sc_connection_cb connection_cb = {};
    uint32_t boot = 0;
    bool first_connection = true;

    static const constexpr size_t SHELL_OUT_SIZE = CONFIG_SOULCLOUD_RPC_RESULT_MAX / 2;
    static const constexpr uint32_t TASK_PRIORITY = tskIDLE_PRIORITY + 5;
    static const constexpr uint32_t CALL_TIMEOUT_MS = 10000;
    static const constexpr char TAG[] = "soulcloud";
};
