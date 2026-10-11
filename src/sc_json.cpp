#include "sc_json.hpp"

#include <cerrno>
#include <cinttypes>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#define JSMN_STATIC
#define JSMN_STRICT
#include "jsmn.h"

static_assert(sizeof(sc_json_token) == sizeof(jsmntok_t), "sc_json_token must mirror jsmntok_t");
static_assert(offsetof(sc_json_token, start) == offsetof(jsmntok_t, start), "sc_json_token must mirror jsmntok_t");
static_assert(offsetof(sc_json_token, size) == offsetof(jsmntok_t, size), "sc_json_token must mirror jsmntok_t");

sc_json_writer::sc_json_writer(char *_buf, size_t _cap) : buf(_buf), cap(_cap)
{
    reset();
}

void sc_json_writer::reset()
{
    pos = 0;
    depth = 0;
    has_items = 0;
    overflow = buf == nullptr || cap < 1;
    if (!overflow) {
        buf[0] = '\0';
    }
}

void sc_json_writer::put(char c)
{
    if (overflow || pos + 1 >= cap) {
        overflow = true;
        return;
    }
    buf[pos++] = c;
    buf[pos] = '\0';
}

void sc_json_writer::put(const char *str, size_t len)
{
    if (overflow || pos + len >= cap) {
        overflow = true;
        return;
    }
    memcpy(buf + pos, str, len);
    pos += len;
    buf[pos] = '\0';
}

void sc_json_writer::put_escaped(const char *str, size_t len)
{
    static const constexpr char HEX[] = "0123456789abcdef";
    put('"');
    for (size_t idx = 0; idx < len && !overflow; idx++) {
        auto c = static_cast<uint8_t>(str[idx]);
        switch (c) {
        case '"':
            put("\\\"", 2);
            break;
        case '\\':
            put("\\\\", 2);
            break;
        case '\n':
            put("\\n", 2);
            break;
        case '\r':
            put("\\r", 2);
            break;
        case '\t':
            put("\\t", 2);
            break;
        default:
            if (c < 0x20) {
                const char esc[] = {'\\', 'u', '0', '0', HEX[c >> 4], HEX[c & 0xf]};
                put(esc, sizeof(esc));
            } else {
                put(static_cast<char>(c));
            }
            break;
        }
    }
    put('"');
}

void sc_json_writer::begin_value(const char *key)
{
    if (depth > 0) {
        if (has_items & (1UL << depth)) {
            put(',');
        }
        has_items |= (1UL << depth);
    }
    if (key != nullptr) {
        put_escaped(key, strlen(key));
        put(':');
    }
}

sc_json_writer &sc_json_writer::begin_object(const char *key)
{
    begin_value(key);
    put('{');
    if (depth >= MAX_DEPTH) {
        overflow = true;
        return *this;
    }
    depth += 1;
    has_items &= ~(1UL << depth);
    return *this;
}

sc_json_writer &sc_json_writer::end_object()
{
    put('}');
    if (depth > 0) {
        depth -= 1;
    }
    return *this;
}

sc_json_writer &sc_json_writer::begin_array(const char *key)
{
    begin_value(key);
    put('[');
    if (depth >= MAX_DEPTH) {
        overflow = true;
        return *this;
    }
    depth += 1;
    has_items &= ~(1UL << depth);
    return *this;
}

sc_json_writer &sc_json_writer::end_array()
{
    put(']');
    if (depth > 0) {
        depth -= 1;
    }
    return *this;
}

sc_json_writer &sc_json_writer::add_str(const char *key, const char *value)
{
    if (value == nullptr) {
        return add_null(key);
    }
    return add_str(key, value, strlen(value));
}

sc_json_writer &sc_json_writer::add_str(const char *key, const char *value, size_t len)
{
    begin_value(key);
    put_escaped(value, len);
    return *this;
}

sc_json_writer &sc_json_writer::add_fmt(const char *key, const char *fmt, ...)
{
    char text[128] = {};
    va_list args;
    va_start(args, fmt);
    int text_len = vsnprintf(text, sizeof(text), fmt, args);
    va_end(args);
    if (text_len < 0) {
        overflow = true;
        return *this;
    }
    return add_str(key, text, static_cast<size_t>(text_len) < sizeof(text) ? text_len : sizeof(text) - 1);
}

sc_json_writer &sc_json_writer::add_int(const char *key, int64_t value)
{
    char text[24] = {};
    int text_len = snprintf(text, sizeof(text), "%" PRId64, value);
    begin_value(key);
    put(text, text_len);
    return *this;
}

sc_json_writer &sc_json_writer::add_uint(const char *key, uint64_t value)
{
    char text[24] = {};
    int text_len = snprintf(text, sizeof(text), "%" PRIu64, value);
    begin_value(key);
    put(text, text_len);
    return *this;
}

sc_json_writer &sc_json_writer::add_bool(const char *key, bool value)
{
    begin_value(key);
    if (value) {
        put("true", 4);
    } else {
        put("false", 5);
    }
    return *this;
}

sc_json_writer &sc_json_writer::add_null(const char *key)
{
    begin_value(key);
    put("null", 4);
    return *this;
}

sc_json_writer &sc_json_writer::add_hex(const char *key, const uint8_t *bytes, size_t len)
{
    static const constexpr char HEX[] = "0123456789abcdef";
    begin_value(key);
    put('"');
    for (size_t idx = 0; idx < len && !overflow; idx++) {
        put(HEX[bytes[idx] >> 4]);
        put(HEX[bytes[idx] & 0xf]);
    }
    put('"');
    return *this;
}

sc_json_reader::sc_json_reader(sc_json_token *_tokens, size_t _max_tokens) : tokens(_tokens), max_tokens(_max_tokens) {}

esp_err_t sc_json_reader::parse(const char *_json, size_t _len)
{
    json = _json;
    len = _len;
    count = 0;

    jsmn_parser parser = {};
    jsmn_init(&parser);
    int ret = jsmn_parse(&parser, json, len, reinterpret_cast<jsmntok_t *>(tokens), max_tokens);
    if (ret == JSMN_ERROR_NOMEM) {
        return ESP_ERR_NO_MEM;
    }
    if (ret < 1 || tokens[0].type != JSMN_OBJECT) {
        return ESP_ERR_INVALID_ARG;
    }

    count = ret;
    return ESP_OK;
}

int sc_json_reader::skip(int tok) const
{
    if (tok < 0 || tok >= count) {
        return -1;
    }

    // Walk forward until every child of the subtree has been consumed.
    int pending = 1;
    int idx = tok;
    while (pending > 0 && idx < count) {
        pending += tokens[idx].size - 1;
        idx += 1;
    }
    return idx < count ? idx : -1;
}

bool sc_json_reader::key_equals(int tok, const char *key) const
{
    size_t key_len = strlen(key);
    const auto &token = tokens[tok];
    return token.type == JSMN_STRING && static_cast<size_t>(token.end - token.start) == key_len && memcmp(json + token.start, key, key_len) == 0;
}

int sc_json_reader::find(int object, const char *key) const
{
    if (!is_object(object)) {
        return -1;
    }

    int member = object + 1;
    for (int idx = 0; idx < tokens[object].size && member >= 0 && member + 1 < count; idx++) {
        // jsmn accepts a key without a value ({"a":}); such a key has no child and counts as missing.
        if (tokens[member].size != 1) {
            return -1;
        }
        if (key_equals(member, key)) {
            return member + 1;
        }
        member = skip(member + 1);
    }
    return -1;
}

bool sc_json_reader::is_object(int tok) const
{
    return tok >= 0 && tok < count && tokens[tok].type == JSMN_OBJECT;
}

int sc_json_reader::child_count(int tok) const
{
    return tok >= 0 && tok < count ? tokens[tok].size : 0;
}

const char *sc_json_reader::raw(int tok, size_t *text_len) const
{
    if (tok < 0 || tok >= count) {
        return nullptr;
    }
    *text_len = tokens[tok].end - tokens[tok].start;
    return json + tokens[tok].start;
}

static int hex_value(char c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

bool sc_json_reader::str_at(int tok, char *out, size_t cap) const
{
    if (tok < 0 || tok >= count || tokens[tok].type != JSMN_STRING || cap < 1) {
        return false;
    }

    const char *in = json + tokens[tok].start;
    const char *in_end = json + tokens[tok].end;
    size_t out_len = 0;
    auto emit = [&](char c) {
        if (out_len + 1 >= cap) {
            return false;
        }
        out[out_len++] = c;
        return true;
    };

    while (in < in_end) {
        char c = *in++;
        if (c != '\\') {
            if (!emit(c)) {
                return false;
            }
            continue;
        }
        if (in >= in_end) {
            return false;
        }

        char esc = *in++;
        switch (esc) {
        case 'b':
            c = '\b';
            break;
        case 'f':
            c = '\f';
            break;
        case 'n':
            c = '\n';
            break;
        case 'r':
            c = '\r';
            break;
        case 't':
            c = '\t';
            break;
        case 'u': {
            if (in_end - in < 4) {
                return false;
            }
            uint32_t code = 0;
            for (int idx = 0; idx < 4; idx++) {
                int digit = hex_value(in[idx]);
                if (digit < 0) {
                    return false;
                }
                code = (code << 4) | digit;
            }
            in += 4;
            // Surrogate pairs are not combined: they become '?'
            if (code >= 0xd800 && code <= 0xdfff) {
                code = '?';
            }
            bool fits = true;
            if (code < 0x80) {
                fits = emit(static_cast<char>(code));
            } else if (code < 0x800) {
                fits = emit(static_cast<char>(0xc0 | (code >> 6))) && emit(static_cast<char>(0x80 | (code & 0x3f)));
            } else {
                fits = emit(static_cast<char>(0xe0 | (code >> 12))) && emit(static_cast<char>(0x80 | ((code >> 6) & 0x3f))) &&
                       emit(static_cast<char>(0x80 | (code & 0x3f)));
            }
            if (!fits) {
                return false;
            }
            continue;
        }
        default: // '"', '\\' and '/'
            c = esc;
            break;
        }
        if (!emit(c)) {
            return false;
        }
    }

    out[out_len] = '\0';
    return true;
}

bool sc_json_reader::get_str(int object, const char *key, char *out, size_t cap) const
{
    return str_at(find(object, key), out, cap);
}

bool sc_json_reader::int_at(int tok, int64_t &out) const
{
    size_t text_len = 0;
    const char *text = raw(tok, &text_len);
    if (text == nullptr || tokens[tok].type != JSMN_PRIMITIVE || text_len == 0 || text_len > 20) {
        return false;
    }

    char number[24] = {};
    memcpy(number, text, text_len);
    char *end = nullptr;
    errno = 0;
    long long value = strtoll(number, &end, 10);
    // Only whole integers: "1.5", "1e3" or out-of-range values are rejected, not rounded.
    if (end == number || *end != '\0' || errno == ERANGE) {
        return false;
    }
    out = value;
    return true;
}

bool sc_json_reader::get_int(int object, const char *key, int64_t &out) const
{
    return int_at(find(object, key), out);
}

bool sc_json_reader::get_bool(int object, const char *key, bool &out) const
{
    size_t text_len = 0;
    int tok = find(object, key);
    const char *text = raw(tok, &text_len);
    if (text == nullptr || tokens[tok].type != JSMN_PRIMITIVE) {
        return false;
    }
    if (text_len == 4 && memcmp(text, "true", 4) == 0) {
        out = true;
        return true;
    }
    if (text_len == 5 && memcmp(text, "false", 5) == 0) {
        out = false;
        return true;
    }
    return false;
}
