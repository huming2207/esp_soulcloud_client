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

    /**
     * @brief Device configuration: Kconfig defaults overridable via NVS.
     *
     * Populated by config_store::load(). Scalar tunables are laid out
     * first (alignment-friendly); the string members are all 4-byte
     * multiples, so the struct has no padding.
     */
    struct config
    {
        uint32_t stat_interval_s;           /**< stat report period, seconds. */
        uint32_t log_rate_per_s;            /**< log uplink throttle, messages/second. */
        uint32_t log_queue_len;             /**< Reserved (log TX queue depth). */
        uint32_t mqtt_buffer_in;            /**< esp-mqtt receive buffer, bytes. */
        uint32_t mqtt_buffer_out;           /**< esp-mqtt transmit buffer, bytes. */
        uint32_t mqtt_keepalive_s;          /**< MQTT keepalive, seconds. */
        uint32_t mqtt_reconnect_timeout_ms; /**< esp-mqtt auto-reconnect interval, ms. */
        uint32_t ota_max_bytes;             /**< Reject OTA images larger than this. */
        uint32_t ota_timeout_s;             /**< Per-download watchdog, seconds. */

        char device_uid[128];      /**< MQTT username + client ID (no '/' '+' '#' or whitespace). */
        char device_password[128]; /**< MQTT credential (issued by the backend). */
        char serial[32];           /**< stat.sn; empty = derive from the MAC. */
        char broker_uri[256];      /**< MQTT-over-WS endpoint, e.g. ws://host:1883/mqtt. */
        char api_base_url[256];    /**< Prefix for OTA download URLs, e.g. http://host:8080. */
    };

    /**
     * @brief Persistent configuration store (singleton).
     *
     * Loads configuration from Kconfig defaults with NVS overrides and
     * persists runtime updates (credential rotation via setConfig).
     * NVS namespace: "soulcloud".
     */
    class config_store
    {
    public:
        /**
         * @brief Access the singleton instance.
         * @return Reference to the process-wide config_store.
         */
        static config_store &instance()
        {
            static config_store s_instance;
            return s_instance;
        }

        config_store(const config_store &) = delete;
        config_store &operator=(const config_store &) = delete;

        /**
         * @brief Load configuration into *out.
         *
         * Starts from Kconfig defaults, applies NVS overrides for each
         * key, and fills cfg->serial with the MAC-derived value when it
         * is left empty.
         *
         * @param[out] out Filled config (must be non-NULL).
         * @return
         *  - ESP_OK              on success
         *  - ESP_ERR_NVS_*       if the NVS partition cannot be opened
         *  - ESP_ERR_INVALID_ARG if out is NULL
         */
        esp_err_t load(config *out);

        /**
         * @brief Persist one string setting (empty value erases the key).
         *
         * @param[in] key   NVS key (see the KEY_* constants).
         * @param[in] value String value; empty string removes the key.
         * @return ESP_OK or an NVS error.
         */
        esp_err_t set_string(const char *key, const char *value);

        /**
         * @brief Persist one u32 setting.
         *
         * @param[in] key   NVS key (see the KEY_* constants).
         * @param[in] value Value to store.
         * @return ESP_OK or an NVS error.
         */
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
     * @brief Command handler for a registered command name.
     *
     * Called on the MQTT event loop with the decoded execution; must fill
     * *out (code and, optionally, an args payload — id/seq are filled by
     * the dispatcher before the call).
     *
     * @note Must be quick and non-blocking: it runs on the MQTT event
     *       task and blocks the whole MQTT stack while executing.
     *
     * @param[in]  cmd Decoded `cmd/exec` (payload views valid for the call).
     * @param[out] out Result to encode; args may reference cmd or storage
     *                 reachable from ctx (encoded before the call returns).
     * @param[in]  ctx Opaque user context from register_command() (may be
     *                 NULL); typically points at per-command state.
     * @return ESP_OK on handled; a non-OK value maps to result code -2.
     */
    typedef esp_err_t (*command_handler_t)(const command_exec *cmd, command_result *out, void *ctx);

    // ------------------------------------------------------------------ //
    // client lifecycle
    // ------------------------------------------------------------------ //

    /**
     * @brief Connection state callback.
     *
     * @param[in] connected true when the MQTT session is established,
     *                      false when it drops.
     * @param[in] ctx       User context from set_connection_cb().
     */
    typedef void (*connection_cb_t)(bool connected, void *ctx);

    /**
     * @brief Soulcloud device client (singleton).
     *
     * Owns the MQTT bridge, the stat timer, the log sink and the OTA
     * executor. All state is internal (PIMPL); the only heap allocation
     * happens in init()/deinit().
     */
    class soulcloud_client
    {
    public:
        /**
         * @brief Access the singleton instance.
         * @return Reference to the process-wide client.
         */
        static soulcloud_client &instance()
        {
            static soulcloud_client s_instance;
            return s_instance;
        }

        soulcloud_client(const soulcloud_client &) = delete;
        soulcloud_client &operator=(const soulcloud_client &) = delete;

        /**
         * @brief Initialise the client core.
         *
         * Copies *cfg (the caller may reuse the storage), creates the
         * MQTT bridge, the periodic stat timer, the on9log MQTT sink and
         * the OTA executor. Idempotent until deinit().
         *
         * @param[in] cfg Configuration (copied).
         * @return
         *  - ESP_OK
         *  - ESP_ERR_INVALID_STATE if already initialised
         *  - ESP_ERR_INVALID_ARG   if cfg is NULL or uid/broker_uri empty
         *  - ESP_ERR_NO_MEM        if the internal allocation failed
         *  - ESP_FAIL              if a sub-component failed to init
         */
        esp_err_t init(const config *cfg);

        /**
         * @brief Start the client: connects to the broker (auto-reconnect
         *        enabled) and starts the periodic stat timer.
         *
         * @return ESP_OK, or ESP_ERR_INVALID_STATE if not initialised.
         */
        esp_err_t start();

        /**
         * @brief Stop the client: disconnects and stops the stat timer.
         * @return ESP_OK, or ESP_ERR_INVALID_STATE if not initialised.
         */
        esp_err_t stop();

        /**
         * @brief Tear down the client (opposite of init()).
         * @return ESP_OK, or ESP_ERR_INVALID_STATE if not initialised.
         */
        esp_err_t deinit();

        /**
         * @brief Whether the MQTT session is currently established.
         * @return true when connected.
         */
        bool is_connected() const { return connected_; }

        /**
         * @brief Set the connection state callback.
         *
         * @param[in] cb  Callback (invoked from the MQTT event loop), or NULL.
         * @param[in] ctx Opaque user context passed to the callback.
         */
        void set_connection_cb(connection_cb_t cb, void *ctx)
        {
            conn_cb_ = cb;
            conn_ctx_ = ctx;
        }

        /**
         * @brief Register a command name (up to 16). Duplicate names are
         *        replaced.
         *
         * @param[in] name    Command name; the registry keeps the POINTER,
         *                    so it must be static or outlive the registry.
         * @param[in] handler Handler to invoke for this command.
         * @param[in] ctx     Opaque user context passed to the handler on
         *                    every dispatch (may be NULL).
         * @return
         *  - ESP_OK
         *  - ESP_ERR_INVALID_ARG if name/handler is NULL or name empty
         *  - ESP_ERR_NO_MEM      if the registry is full
         */
        esp_err_t register_command(const char *name, command_handler_t handler, void *ctx = nullptr);

        /**
         * @brief Report stat immediately (also sent on every connect).
         *
         * Encodes and publishes a QoS 1 stat message on the `stat` topic.
         * @return ESP_OK, or ESP_FAIL if the publish could not be queued.
         */
        esp_err_t report_stat();

        /**
         * @brief Nudge an immediate reconnect attempt.
         *
         * Call when the network interface (re)connects so the client does
         * not wait for the esp-mqtt reconnect timer.
         */
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
