#pragma once

/*
 * Soulcloud device client: public API.
 *
 * Lifecycle: config_store::instance().load() -> soulcloud_client::init()
 *            -> start() -> (running) -> stop() -> deinit().
 *
 * Memory rules: all storage is static/stack; heap allocation happens only
 * inside init()/deinit() (the PIMPL and esp-mqtt internals) and nowhere
 * else.
 */

#include <esp_err.h>
#include <esp_system.h>
#include <cstdint>

#include <soulcloud_types.hpp>

namespace soulcloud
{
    // ------------------------------------------------------------------ //
    // configuration (Kconfig defaults, NVS overrides)
    // ------------------------------------------------------------------ //

    struct config
    {
        // Scalar tunables first (alignment-friendly; the string members
        // below are all 4-byte multiples so no padding is introduced).
        uint32_t stat_interval_s;          // stat report period (s)
        uint32_t log_rate_per_s;           // log uplink throttle (msg/s)
        uint32_t log_queue_len;            // reserved (log TX queue depth)
        uint32_t mqtt_buffer_in;           // esp-mqtt rx buffer (bytes)
        uint32_t mqtt_buffer_out;          // esp-mqtt tx buffer (bytes)
        uint32_t mqtt_keepalive_s;         // MQTT keepalive (s)
        uint32_t mqtt_reconnect_timeout_ms; // esp-mqtt auto-reconnect interval
        uint32_t ota_max_bytes;            // refuse absurd OTA image sizes
        uint32_t ota_timeout_s;            // per-download watchdog (s)

        char device_uid[128];      // MQTT username + client ID
        char device_password[128]; // MQTT credential
        char serial[32];           // stat.sn; empty = derive from MAC
        char broker_uri[256];      // ws://host:port/path (or wss://)
        char api_base_url[256];    // prefix for OTA download urls
    };

    /** Persistent configuration store (singleton): Kconfig defaults with
     *  NVS overrides. NVS namespace: "soulcloud". */
    class config_store
    {
    public:
        static config_store &instance()
        {
            static config_store s_instance;
            return s_instance;
        }

        config_store(const config_store &) = delete;
        config_store &operator=(const config_store &) = delete;

        /** Loads NVS over Kconfig defaults. */
        esp_err_t load(config *out);

        /** Persists one string setting (empty value erases the key). */
        esp_err_t set_string(const char *key, const char *value);

        /** Persists one u32 setting. */
        esp_err_t set_u32(const char *key, uint32_t value);

        // NVS keys (public for the setConfig command)
        static constexpr char KEY_UID[] = "uid";
        static constexpr char KEY_PASS[] = "pass";
        static constexpr char KEY_SERIAL[] = "serial";
        static constexpr char KEY_BROKER[] = "broker";
        static constexpr char KEY_API[] = "api";
        static constexpr char KEY_STAT_INT[] = "stat_int";
        static constexpr char KEY_LOG_RATE[] = "log_rate";
        static constexpr char KEY_LOG_Q[] = "log_q";
        static constexpr char KEY_MQTT_IN[] = "mqtt_in";
        static constexpr char KEY_MQTT_OUT[] = "mqtt_out";
        static constexpr char KEY_KA[] = "ka";
        static constexpr char KEY_RECONN[] = "reconn";
        static constexpr char KEY_OTA_MAX[] = "ota_max";
        static constexpr char KEY_OTA_TO[] = "ota_to";

        static constexpr char NVS_NAMESPACE[] = "soulcloud";

    private:
        config_store() = default;

        /** Fills the MAC-derived default serial into cfg->serial. */
        void derive_serial(char *out, size_t cap) const;
    };

    // ------------------------------------------------------------------ //
    // commands (registered by the application)
    // ------------------------------------------------------------------ //

    /**
     * Command handler: fills *out (code, optional args payload). The
     * dispatcher fills out->id/seq before calling. Must be quick and
     * non-blocking (runs on the MQTT event loop).
     */
    typedef esp_err_t (*command_handler_t)(const command_exec *cmd, command_result *out);

    // ------------------------------------------------------------------ //
    // client lifecycle
    // ------------------------------------------------------------------ //

    /** Connection state callback (invoked from the MQTT event loop). */
    typedef void (*connection_cb_t)(bool connected, void *ctx);

    /** Soulcloud device client (singleton). */
    class soulcloud_client
    {
    public:
        static soulcloud_client &instance()
        {
            static soulcloud_client s_instance;
            return s_instance;
        }

        soulcloud_client(const soulcloud_client &) = delete;
        soulcloud_client &operator=(const soulcloud_client &) = delete;

        esp_err_t init(const config *cfg);
        esp_err_t start();
        esp_err_t stop();
        esp_err_t deinit();

        bool is_connected() const { return connected_; }

        void set_connection_cb(connection_cb_t cb, void *ctx)
        {
            conn_cb_ = cb;
            conn_ctx_ = ctx;
        }

        /** Registers a command name (up to 16). Duplicate names replaced. */
        esp_err_t register_command(const char *name, command_handler_t handler);

        /** Reports stat immediately (also sent on every connect). */
        esp_err_t report_stat();

        /** Call when WiFi (re)connects so the client reconnects right away. */
        void notify_wifi_connected();

    private:
        soulcloud_client() = default;

        static constexpr char TAG[] = "soulcloud";
        static constexpr uint32_t FW_SHA256_LEN = 32;  // raw SHA-256 bytes

        class client_impl;  // PIMPL: mqtt_bridge + esp_timer (see soulcloud.cpp)
        client_impl *impl_ = nullptr;

        config cfg_ = {};
        bool inited_ = false;
        bool started_ = false;
        bool connected_ = false;
        connection_cb_t conn_cb_ = nullptr;
        void *conn_ctx_ = nullptr;

        void on_mqtt_connected();
        void on_mqtt_disconnected();
        void on_mqtt_data(const char *topic, size_t topic_len,
                          const uint8_t *data, size_t data_len);
        void build_stat(uint8_t *buf, size_t cap, size_t *out_len);

        static const char *reset_reason_str(esp_reset_reason_t r);
        static int hex_val(char c);
        static void hex_to_bin(const char *hex, uint8_t *bin, size_t len);
        static bool topic_matches(const char *topic, size_t topic_len, const char *expected);
    };
}  // namespace soulcloud
