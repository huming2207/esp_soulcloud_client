#include "sc_shell.hpp"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>

#include <esp_app_desc.h>
#include <esp_chip_info.h>
#include <esp_heap_caps.h>
#include <esp_mac.h>
#include <esp_ota_ops.h>
#include <esp_partition.h>
#include <esp_system.h>
#include <esp_timer.h>
#include <lwip/inet.h>
#include <lwip/netdb.h>

sc_shell_out::sc_shell_out(char *_buf, size_t _cap) : buf(_buf), cap(_cap)
{
    if (cap > 0) {
        buf[0] = '\0';
    }
}

void sc_shell_out::write(const char *str, size_t len)
{
    if (cap == 0 || full) {
        return;
    }
    size_t room = cap - 1 - pos;
    if (len > room) {
        len = room;
        full = true;
    }
    memcpy(buf + pos, str, len);
    pos += len;
    buf[pos] = '\0';
}

void sc_shell_out::printf(const char *fmt, ...)
{
    if (cap == 0 || full) {
        return;
    }
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(buf + pos, cap - pos, fmt, args);
    va_end(args);
    if (len < 0) {
        buf[pos] = '\0';
        return;
    }
    if (static_cast<size_t>(len) >= cap - pos) {
        pos = cap - 1;
        full = true;
    } else {
        pos += len;
    }
}

uint8_t sc_shell::count_words(const char *name)
{
    uint8_t words = 1;
    for (const char *c = name; *c != '\0'; c++) {
        words += *c == ' ' ? 1 : 0;
    }
    return words;
}

bool sc_shell::name_matches(const char *name, char **argv, int argc)
{
    int word = 0;
    const char *start = name;
    while (true) {
        const char *end = strchr(start, ' ');
        size_t len = end != nullptr ? static_cast<size_t>(end - start) : strlen(start);
        if (word >= argc || strlen(argv[word]) != len || strncmp(argv[word], start, len) != 0) {
            return false;
        }
        word += 1;
        if (end == nullptr) {
            return true;
        }
        start = end + 1;
    }
}

esp_err_t sc_shell::add(const char *name, const char *help, const sc_shell_handler &handler)
{
    if (name == nullptr || name[0] == '\0' || !handler) {
        return ESP_ERR_INVALID_ARG;
    }

    for (auto &cmd : cmds) {
        if (cmd.name == nullptr || strcmp(cmd.name, name) == 0) {
            cmd.name = name;
            cmd.help = help;
            cmd.handler = handler;
            cmd.words = count_words(name);
            return ESP_OK;
        }
    }
    return ESP_ERR_NO_MEM;
}

int sc_shell::run(char *line, sc_shell_out &out)
{
    char *argv[MAX_ARGS] = {};
    int argc = 0;
    char *save = nullptr;
    for (char *word = strtok_r(line, " \t\r\n", &save); word != nullptr && argc < MAX_ARGS; word = strtok_r(nullptr, " \t\r\n", &save)) {
        argv[argc++] = word;
    }
    if (argc == 0) {
        return 0;
    }

    const entry *best = nullptr;
    for (const auto &cmd : cmds) {
        if (cmd.name != nullptr && (best == nullptr || cmd.words > best->words) && name_matches(cmd.name, argv, argc)) {
            best = &cmd;
        }
    }
    if (best == nullptr) {
        out.printf("Unknown command \"%s\"; try \"help\"\n", argv[0]);
        return 127;
    }
    return best->handler(argc - best->words, argv + best->words, out);
}

static const char *reset_reason_name(esp_reset_reason_t reason)
{
    static const constexpr char *NAMES[] = {
        "unknown",    "power_on", "external", "software", "panic", "int_wdt", "task_wdt",     "other_wdt",
        "deep_sleep", "brownout", "sdio",     "usb",      "jtag",  "efuse",   "power_glitch", "cpu_lockup",
    };
    auto idx = static_cast<size_t>(reason);
    return idx < sizeof(NAMES) / sizeof(NAMES[0]) ? NAMES[idx] : "other";
}

static void print_heap(sc_shell_out &out, const char *label, uint32_t caps)
{
    out.printf(
        "%s: free %u, min free %u, largest block %u\n", label, heap_caps_get_free_size(caps), heap_caps_get_minimum_free_size(caps),
        heap_caps_get_largest_free_block(caps)
    );
}

void sc_shell::add_builtins()
{
    add("help", "List commands", [this](int, char **, sc_shell_out &out) {
        for (const auto &cmd : cmds) {
            if (cmd.name != nullptr) {
                out.printf("%-24s %s\n", cmd.name, cmd.help != nullptr ? cmd.help : "");
            }
        }
        return 0;
    });

    add("status uptime", "Time since boot", [](int, char **, sc_shell_out &out) {
        out.printf("uptime= %lld ms\n", esp_timer_get_time() / 1000);
        return 0;
    });

    add("get app_desc", "Firmware description", [](int, char **, sc_shell_out &out) {
        const esp_app_desc_t *desc = esp_app_get_description();
        out.printf(
            "Project name: %s, App version: %s, Compile time: %s %s, ESP-IDF: %s\n", desc->project_name, desc->version, desc->date, desc->time,
            desc->idf_ver
        );
        return 0;
    });

    add("get elf_sha256", "SHA-256 of the running firmware's ELF", [](int, char **, sc_shell_out &out) {
        const uint8_t *sha = esp_app_get_description()->app_elf_sha256;
        for (int idx = 0; idx < 32; idx++) {
            out.printf("%02x", sha[idx]);
        }
        out.write("\n", 1);
        return 0;
    });

    add("get unique_id", "Base MAC address", [](int, char **, sc_shell_out &out) {
        uint8_t mac[6] = {};
        esp_efuse_mac_get_default(mac);
        out.printf(MACSTR "\n", MAC2STR(mac));
        return 0;
    });

    add("get chip", "Chip model, revision and cores", [](int, char **, sc_shell_out &out) {
        esp_chip_info_t info = {};
        esp_chip_info(&info);
        out.printf("%s rev %u.%u, %u cores\n", CONFIG_IDF_TARGET, info.revision / 100, info.revision % 100, info.cores);
        return 0;
    });

    add("get free_heap", "Heap usage [internal|external]", [](int argc, char **argv, sc_shell_out &out) {
        bool internal = argc == 0 || strcmp(argv[0], "internal") == 0;
        bool external = argc == 0 || strcmp(argv[0], "external") == 0;
        if (!internal && !external) {
            out.printf("Usage: get free_heap [internal|external]\n");
            return 1;
        }
        if (internal) {
            print_heap(out, "internal", MALLOC_CAP_INTERNAL);
        }
        if (external) {
            if (heap_caps_get_total_size(MALLOC_CAP_SPIRAM) == 0) {
                out.printf("external: no PSRAM\n");
            } else {
                print_heap(out, "external", MALLOC_CAP_SPIRAM);
            }
        }
        return 0;
    });

    add("get partitions", "App partitions and OTA state", [](int, char **, sc_shell_out &out) {
        const esp_partition_t *running = esp_ota_get_running_partition();
        const esp_partition_t *boot = esp_ota_get_boot_partition();
        esp_partition_iterator_t iter = esp_partition_find(ESP_PARTITION_TYPE_APP, ESP_PARTITION_SUBTYPE_ANY, nullptr);
        for (; iter != nullptr; iter = esp_partition_next(iter)) {
            const esp_partition_t *part = esp_partition_get(iter);
            esp_app_desc_t desc = {};
            bool has_app = esp_ota_get_partition_description(part, &desc) == ESP_OK;
            out.printf(
                "%-8s 0x%06lx %4lu KiB %-8s%s%s\n", part->label, static_cast<unsigned long>(part->address),
                static_cast<unsigned long>(part->size / 1024), has_app ? desc.version : "-", part == running ? " running" : "",
                part == boot ? " boot" : ""
            );
        }
        esp_partition_iterator_release(iter);
        return 0;
    });

    add("get reset_reason", "Why the chip last restarted", [](int, char **, sc_shell_out &out) {
        out.printf("%s\n", reset_reason_name(esp_reset_reason()));
        return 0;
    });

    add("get time", "Wall-clock time (UTC)", [](int, char **, sc_shell_out &out) {
        time_t now = time(nullptr);
        struct tm utc = {};
        gmtime_r(&now, &utc);
        char text[32] = {};
        strftime(text, sizeof(text), "%Y-%m-%dT%H:%M:%SZ", &utc);
        out.printf("%s\n", text);
        return 0;
    });

    add("dns_resolve", "Resolve a host name", [](int argc, char **argv, sc_shell_out &out) {
        if (argc < 1) {
            out.printf("Usage: dns_resolve <host>\n");
            return 1;
        }
        addrinfo hints = {};
        hints.ai_family = AF_UNSPEC;
        addrinfo *result = nullptr;
        int err = getaddrinfo(argv[0], nullptr, &hints, &result);
        if (err != 0 || result == nullptr) {
            out.printf("getaddrinfo error: %d\n", err);
            return 1;
        }
        for (addrinfo *item = result; item != nullptr; item = item->ai_next) {
            char text[48] = {};
            if (item->ai_family == AF_INET) {
                inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in *>(item->ai_addr)->sin_addr, text, sizeof(text));
            } else if (item->ai_family == AF_INET6) {
                inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6 *>(item->ai_addr)->sin6_addr, text, sizeof(text));
            }
            out.printf("Host: %s, IP: %s\n", argv[0], text);
        }
        freeaddrinfo(result);
        return 0;
    });
}
