idf_component_register(
    SRCS
        "src/sc_client.cpp"
        "src/sc_codec.cpp"
        "src/sc_credentials.cpp"
        "src/sc_json.cpp"
        "src/sc_log_uplink.cpp"
        "src/sc_mqtt.cpp"
        "src/sc_ota.cpp"
        "src/sc_rpc.cpp"
        "src/sc_shell.cpp"
        "src/sc_status.cpp"

    INCLUDE_DIRS
        "include"

    PRIV_INCLUDE_DIRS
        "src"
        "external/jsmn"

    REQUIRES
        "esp_event"

    PRIV_REQUIRES
        "on9log"
        "mqtt"
        "nvs_flash"
        "esp_http_client"
        "esp_https_ota"
        "app_update"
        "esp_app_format"
        "esp_partition"
        "esp_ringbuf"
        "esp_timer"
        "esp_netif"
        "lwip"
        "mbedtls"
        "log"
)
