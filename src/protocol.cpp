/*
 * Soulcloud v1 wire protocol implementation on top of MPack.
 *
 * Wire contract (frozen by soulcloud.js):
 *   - cmd/exec:   map {id: bin(16), seq: uint64, cmd: str, args: nil|array}
 *   - cmd/result: map {id: bin(16), seq: uint64, code: int32,
 *                      payload: nil|array}
 *   - stat:       map {sn: bin, fw: bin, up: uint64, rst: str}
 *   - ota notice: map {release_id, job_id, bin_sha256, bin_size,
 *                      download: {url, token, expires_at}, version?}
 *   - ota/result: map {release_id, job_id, state, code, message?}
 *
 * Unknown map keys are skipped (forward compatibility); duplicate keys,
 * wrong types, missing required fields and trailing bytes are rejected.
 *
 * NOTE on ota/result message: the backend schema is z.string().optional(),
 * which rejects null — the field must be omitted entirely when absent.
 */

#include "protocol.hpp"

#include <cstdio>
#include <cstring>

using namespace soulcloud;

// ------------------------------------------------------------------ //
// msgpack_reader
// ------------------------------------------------------------------ //

soulcloud::msgpack_reader::msgpack_reader(const uint8_t *data, size_t len)
{
    mpack_reader_init_data(&r, (const char *)data, len);
}

soulcloud::msgpack_reader::~msgpack_reader()
{
    mpack_reader_destroy(&r);
}

mpack_type_t soulcloud::msgpack_reader::peek_type()
{
    mpack_tag_t tag = mpack_peek_tag(&r);
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
    return mpack_tag_type(&tag);
}

uint32_t soulcloud::msgpack_reader::expect_map()
{
    const uint32_t n = mpack_expect_map(&r);
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
    return n;
}

uint32_t soulcloud::msgpack_reader::expect_array()
{
    const uint32_t n = mpack_expect_array(&r);
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
    return n;
}

uint64_t soulcloud::msgpack_reader::expect_u64()
{
    const uint64_t v = mpack_expect_u64(&r);
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
    return v;
}

int64_t soulcloud::msgpack_reader::expect_int()
{
    const int64_t v = mpack_expect_int(&r);
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
    return v;
}

int32_t soulcloud::msgpack_reader::expect_int32()
{
    const int64_t v = mpack_expect_int(&r);
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return 0;
    }
    if (v < INT32_MIN || v > INT32_MAX) {
        fail(ERR_TYPE);
        return 0;
    }
    return (int32_t)v;
}

bool soulcloud::msgpack_reader::expect_bool()
{
    const bool v = mpack_expect_bool(&r);
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
    return v;
}

void soulcloud::msgpack_reader::expect_nil()
{
    mpack_expect_nil(&r);
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
}

double soulcloud::msgpack_reader::expect_float()
{
    mpack_tag_t tag = mpack_peek_tag(&r);
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return 0.0;
    }
    double v = 0.0;
    switch (mpack_tag_type(&tag)) {
    case mpack_type_float:
        v = mpack_expect_float(&r);
        break;
    case mpack_type_double:
        v = mpack_expect_double(&r);
        break;
    default:
        fail(ERR_TYPE);
        return 0.0;
    }
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
    return v;
}

int32_t soulcloud::msgpack_reader::read_key(char *buf, size_t cap, size_t *out_len)
{
    mpack_tag_t tag = mpack_read_tag(&r);
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return ret;
    }
    if (mpack_tag_type(&tag) != mpack_type_str) {
        fail(ERR_TYPE);
        return ret;
    }
    const size_t n = mpack_tag_str_length(&tag);
    if (n >= cap) {
        fail(ERR_OVERFLOW);
        return ret;
    }
    mpack_read_bytes(&r, buf, n);
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return ret;
    }
    buf[n] = '\0';
    *out_len = n;
    return ret;
}

int32_t soulcloud::msgpack_reader::read_str(const char **ptr, uint32_t *len, uint32_t max_len)
{
    mpack_tag_t tag = mpack_read_tag(&r);
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return ret;
    }
    if (mpack_tag_type(&tag) != mpack_type_str) {
        fail(ERR_TYPE);
        return ret;
    }
    const size_t n = mpack_tag_str_length(&tag);
    if (n > max_len) {
        fail(ERR_OVERFLOW);
        return ret;
    }
    *ptr = mpack_read_bytes_inplace(&r, n);
    *len = (uint32_t)n;
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return ret;
    }
    return ret;
}

int32_t soulcloud::msgpack_reader::read_bin(const uint8_t **ptr, uint32_t *len, uint32_t max_len)
{
    mpack_tag_t tag = mpack_read_tag(&r);
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return ret;
    }
    if (mpack_tag_type(&tag) != mpack_type_bin) {
        fail(ERR_TYPE);
        return ret;
    }
    const size_t n = mpack_tag_bin_length(&tag);
    if (n > max_len) {
        fail(ERR_OVERFLOW);
        return ret;
    }
    *ptr = (const uint8_t *)mpack_read_bytes_inplace(&r, n);
    *len = (uint32_t)n;
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return ret;
    }
    return ret;
}

void soulcloud::msgpack_reader::skip_value()
{
    mpack_discard(&r);
    if (mpack_reader_error(&r) != mpack_ok)
        fail(ERR_BAD_MSG);
}

int32_t soulcloud::msgpack_reader::finish()
{
    if (ret != ERR_OK)
        return ret;
    if (mpack_reader_error(&r) != mpack_ok) {
        fail(ERR_BAD_MSG);
        return ret;
    }
    if (mpack_reader_remaining(&r, NULL) != 0) {
        fail(ERR_BAD_MSG);
    }
    return ret;
}

// ------------------------------------------------------------------ //
// msgpack_writer
// ------------------------------------------------------------------ //

soulcloud::msgpack_writer::msgpack_writer(uint8_t *buf, size_t cap)
{
    mpack_writer_init(&w, (char *)buf, cap);
}

soulcloud::msgpack_writer::~msgpack_writer()
{
    if (!destroyed) {
        mpack_writer_destroy(&w);
        destroyed = true;
    }
}

void soulcloud::msgpack_writer::start_map(uint32_t count)
{
    if (ret != ERR_OK)
        return;
    mpack_start_map(&w, count);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::finish_map()
{
    if (ret != ERR_OK)
        return;
    mpack_finish_map(&w);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::start_array(uint32_t count)
{
    if (ret != ERR_OK)
        return;
    mpack_start_array(&w, count);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::finish_array()
{
    if (ret != ERR_OK)
        return;
    mpack_finish_array(&w);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::write_str(const char *s)
{
    if (ret != ERR_OK)
        return;
    mpack_write_cstr(&w, s);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::write_str(const char *s, size_t len)
{
    if (ret != ERR_OK)
        return;
    mpack_write_str(&w, s, len);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::write_bin(const void *data, size_t len)
{
    if (ret != ERR_OK)
        return;
    mpack_write_bin(&w, (const char *)data, len);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::write_uint(uint64_t v)
{
    if (ret != ERR_OK)
        return;
    mpack_write_uint(&w, v);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::write_int(int64_t v)
{
    if (ret != ERR_OK)
        return;
    mpack_write_int(&w, v);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::write_nil()
{
    if (ret != ERR_OK)
        return;
    mpack_write_nil(&w);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::write_bool(bool v)
{
    if (ret != ERR_OK)
        return;
    mpack_write_bool(&w, v);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

void soulcloud::msgpack_writer::write_double(double v)
{
    if (ret != ERR_OK)
        return;
    mpack_write_double(&w, v);
    if (mpack_writer_error(&w) != mpack_ok)
        fail(ERR_OVERFLOW);
}

size_t soulcloud::msgpack_writer::bytes_written()
{
    return mpack_writer_buffer_used(&w);
}

int32_t soulcloud::msgpack_writer::finish()
{
    if (destroyed)
        return ret;
    if (ret == ERR_OK && mpack_writer_error(&w) != mpack_ok) {
        fail(ERR_OVERFLOW);
    }
    mpack_writer_destroy(&w);
    destroyed = true;
    return ret;
}

// ------------------------------------------------------------------ //
// topics
// ------------------------------------------------------------------ //

void soulcloud::topic_cmd_exec(char *out, size_t cap, const char *device_uid)
{
    snprintf(out, cap, "soulcloud/v1/devices/%s/cmd/exec", device_uid);
}

void soulcloud::topic_cmd_result(char *out, size_t cap, const char *device_uid)
{
    snprintf(out, cap, "soulcloud/v1/devices/%s/cmd/result", device_uid);
}

void soulcloud::topic_ota(char *out, size_t cap, const char *device_uid)
{
    snprintf(out, cap, "soulcloud/v1/devices/%s/ota", device_uid);
}

void soulcloud::topic_ota_result(char *out, size_t cap, const char *device_uid)
{
    snprintf(out, cap, "soulcloud/v1/devices/%s/ota/result", device_uid);
}

void soulcloud::topic_log(char *out, size_t cap, const char *device_uid)
{
    snprintf(out, cap, "soulcloud/v1/devices/%s/log", device_uid);
}

void soulcloud::topic_stat(char *out, size_t cap, const char *device_uid)
{
    snprintf(out, cap, "soulcloud/v1/devices/%s/stat", device_uid);
}

// ------------------------------------------------------------------ //
// shared decode helpers
// ------------------------------------------------------------------ //

/** Parses one argument value; strings/bins point into the payload. */
static int32_t read_arg_value(msgpack_reader &r, cmd_arg_value *out)
{
    switch (r.peek_type()) {
    case mpack_type_nil:
        r.expect_nil();
        out->type = cmd_arg_value::TYPE_NIL;
        break;
    case mpack_type_bool:
        out->b = r.expect_bool();
        out->type = cmd_arg_value::TYPE_BOOL;
        break;
    case mpack_type_int:
        out->i = r.expect_int();
        out->type = cmd_arg_value::TYPE_INT;
        break;
    case mpack_type_uint: {
        const uint64_t v = r.expect_u64();
        if (v <= (uint64_t)INT64_MAX) {
            out->i = (int64_t)v;
            out->type = cmd_arg_value::TYPE_INT;
        } else {
            out->u = v;
            out->type = cmd_arg_value::TYPE_UINT;
        }
        break;
    }
    case mpack_type_float:
    case mpack_type_double:
        out->f = r.expect_float();
        out->type = cmd_arg_value::TYPE_FLOAT;
        break;
    case mpack_type_str:
        out->type = cmd_arg_value::TYPE_STR;
        return r.read_str(&out->str.ptr, &out->str.len, UINT32_MAX);
    case mpack_type_bin:
        out->type = cmd_arg_value::TYPE_BIN;
        return r.read_bin(&out->bin.ptr, &out->bin.len, UINT32_MAX);
    default:
        r.skip_value();
        return ERR_TYPE;
    }
    return r.ok() ? ERR_OK : r.err();
}

/** Writes one argument value. */
static void write_arg_value(msgpack_writer &w, const cmd_arg_value *v)
{
    switch (v->type) {
    case cmd_arg_value::TYPE_NIL:
        w.write_nil();
        break;
    case cmd_arg_value::TYPE_BOOL:
        w.write_bool(v->b);
        break;
    case cmd_arg_value::TYPE_INT:
        w.write_int(v->i);
        break;
    case cmd_arg_value::TYPE_UINT:
        w.write_uint(v->u);
        break;
    case cmd_arg_value::TYPE_FLOAT:
        w.write_double(v->f);
        break;
    case cmd_arg_value::TYPE_STR:
        w.write_str(v->str.ptr, v->str.len);
        break;
    case cmd_arg_value::TYPE_BIN:
        w.write_bin(v->bin.ptr, v->bin.len);
        break;
    }
}

/** Writes the args array (or nil when empty). */
static void write_args(msgpack_writer &w, const cmd_arg *args, uint32_t count)
{
    if (args == NULL || count == 0) {
        w.write_nil();
        return;
    }
    w.start_array(count);
    for (uint32_t i = 0; i < count; ++i) {
        w.start_map(1);
        w.write_str(args[i].key, args[i].key_len);
        write_arg_value(w, &args[i].value);
        w.finish_map();
    }
    w.finish_array();
}

// ------------------------------------------------------------------ //
// command execution
// ------------------------------------------------------------------ //

int32_t soulcloud::decode_command_id(const uint8_t *payload, size_t len, uint8_t *id_out, uint64_t *seq_out)
{
    msgpack_reader r(payload, len);
    const uint32_t map_count = r.expect_map();
    if (!r.ok()) {
        return r.err();
    }
    bool have_id = false;
    for (uint32_t i = 0; i < map_count; ++i) {
        char key[24];
        size_t key_len = 0;
        if (r.read_key(key, sizeof(key), &key_len) != ERR_OK) {
            return r.err();
        }
        if (key_len == 2 && memcmp(key, "id", 2) == 0) {
            const uint8_t *bin = nullptr;
            uint32_t bin_len = 0;
            if (r.read_bin(&bin, &bin_len, 16) != ERR_OK) {
                return r.err();
            }
            if (bin_len != 16u) {
                return ERR_FIELD_LEN;
            }
            memcpy(id_out, bin, 16);
            have_id = true;
        } else if (key_len == 3 && memcmp(key, "seq", 3) == 0) {
            // best-effort: a malformed seq keeps the error result
            // answerable (the backend matches on id first, seq second);
            // leave *seq_out at 0 when it is absent or wrong-typed.
            // Peek before consuming: mpack reader errors are sticky, so a
            // wrong-typed seq must be skipped (not read) or it would
            // poison the scan before the id is found.
            if (r.peek_type() == mpack_type_uint) {
                *seq_out = r.expect_u64();
            } else {
                r.skip_value();
            }
        } else {
            r.skip_value();
        }
    }
    return have_id ? ERR_OK : ERR_MISSING_FIELD;
}

int32_t soulcloud::decode_command_exec(const uint8_t *payload, size_t len, command_exec *out)
{
    msgpack_reader r(payload, len);
    const uint32_t map_count = r.expect_map();
    if (!r.ok())
        return r.err();

    uint32_t seen = 0; // bit0=id bit1=seq bit2=cmd bit3=args
    command_exec tmp = {};

    for (uint32_t i = 0; i < map_count; ++i) {
        char key[24];
        size_t key_len = 0;
        if (r.read_key(key, sizeof(key), &key_len) != ERR_OK)
            return r.err();

        if (key_len == 2 && memcmp(key, "id", 2) == 0) {
            if (seen & 1u)
                return ERR_DUP_KEY;
            seen |= 1u;
            uint32_t id_len = 0;
            if (r.read_bin(&tmp.id, &id_len, 16) != ERR_OK)
                return r.err();
            if (id_len != 16u)
                return ERR_FIELD_LEN;
        } else if (key_len == 3 && memcmp(key, "seq", 3) == 0) {
            if (seen & 2u)
                return ERR_DUP_KEY;
            seen |= 2u;
            tmp.seq = r.expect_u64();
        } else if (key_len == 3 && memcmp(key, "cmd", 3) == 0) {
            if (seen & 4u)
                return ERR_DUP_KEY;
            seen |= 4u;
            if (r.read_str(&tmp.cmd, &tmp.cmd_len, 255) != ERR_OK)
                return r.err();
        } else if (key_len == 4 && memcmp(key, "args", 4) == 0) {
            if (seen & 8u)
                return ERR_DUP_KEY;
            seen |= 8u;
            if (r.peek_type() == mpack_type_nil) {
                r.expect_nil();
            } else if (r.peek_type() == mpack_type_array) {
                const uint32_t n = r.expect_array();
                if (n > 8u)
                    return ERR_OVERFLOW;
                for (uint32_t j = 0; j < n; ++j) {
                    if (r.expect_map() != 1u)
                        return ERR_TYPE;
                    size_t arg_key_len = 0;
                    if (r.read_key(tmp.key_storage[j], sizeof(tmp.key_storage[j]), &arg_key_len) != ERR_OK) {
                        return r.err();
                    }
                    tmp.args[j].key = tmp.key_storage[j];
                    tmp.args[j].key_len = (uint32_t)arg_key_len;
                    const int32_t value_rc = read_arg_value(r, &tmp.args[j].value);
                    if (value_rc != ERR_OK)
                        return value_rc;
                }
                tmp.arg_count = n;
            } else {
                return ERR_TYPE;
            }
        } else {
            // unknown field: skip its value
            r.skip_value();
        }

        if (!r.ok())
            return r.err();
    }

    if ((seen & 7u) != 7u)
        return ERR_MISSING_FIELD; // id + seq + cmd
    if (r.finish() != ERR_OK)
        return r.err();

    *out = tmp;
    return ERR_OK;
}

// ------------------------------------------------------------------ //
// command result
// ------------------------------------------------------------------ //

int32_t soulcloud::encode_command_result(uint8_t *buf, size_t cap, size_t *out_len, const command_result *res)
{
    msgpack_writer w(buf, cap);

    w.start_map(4);
    w.write_str("id");
    w.write_bin(res->id, 16);
    w.write_str("seq");
    w.write_uint(res->seq);
    w.write_str("code");
    w.write_int(res->code);
    w.write_str("payload");
    write_args(w, res->args, res->arg_count);
    w.finish_map();

    if (!w.ok())
        return w.err();
    *out_len = w.bytes_written();
    return w.finish();
}

// ------------------------------------------------------------------ //
// device status
// ------------------------------------------------------------------ //

int32_t soulcloud::encode_stat(uint8_t *buf, size_t cap, size_t *out_len, const device_stat *stat)
{
    msgpack_writer w(buf, cap);

    w.start_map(4);
    w.write_str("sn");
    w.write_bin(stat->sn, stat->sn_len);
    w.write_str("fw");
    w.write_bin(stat->fw, stat->fw_len);
    w.write_str("up");
    w.write_uint(stat->up);
    w.write_str("rst");
    w.write_str(stat->rst);
    w.finish_map();

    if (!w.ok())
        return w.err();
    *out_len = w.bytes_written();
    return w.finish();
}

// ------------------------------------------------------------------ //
// OTA notice
// ------------------------------------------------------------------ //

int32_t soulcloud::decode_ota_notice(const uint8_t *payload, size_t len, ota_notice *out)
{
    msgpack_reader r(payload, len);
    const uint32_t map_count = r.expect_map();
    if (!r.ok())
        return r.err();

    uint32_t seen = 0; // bit0=release_id bit1=job_id bit2=bin_sha256
                       // bit3=bin_size bit4=download bit5=version
    ota_notice tmp = {};
    tmp.has_version = false;

    for (uint32_t i = 0; i < map_count; ++i) {
        char key[24];
        size_t key_len = 0;
        if (r.read_key(key, sizeof(key), &key_len) != ERR_OK)
            return r.err();

        if (key_len == 10 && memcmp(key, "release_id", 10) == 0) {
            if (seen & 1u)
                return ERR_DUP_KEY;
            seen |= 1u;
            uint32_t vlen = 0;
            const char *v = NULL;
            if (r.read_str(&v, &vlen, (uint32_t)sizeof(tmp.release_id) - 1) != ERR_OK)
                return r.err();
            memcpy(tmp.release_id, v, vlen);
            tmp.release_id[vlen] = '\0';
        } else if (key_len == 6 && memcmp(key, "job_id", 6) == 0) {
            if (seen & 2u)
                return ERR_DUP_KEY;
            seen |= 2u;
            uint32_t vlen = 0;
            const char *v = NULL;
            if (r.read_str(&v, &vlen, (uint32_t)sizeof(tmp.job_id) - 1) != ERR_OK)
                return r.err();
            memcpy(tmp.job_id, v, vlen);
            tmp.job_id[vlen] = '\0';
        } else if (key_len == 10 && memcmp(key, "bin_sha256", 10) == 0) {
            if (seen & 4u)
                return ERR_DUP_KEY;
            seen |= 4u;
            uint32_t vlen = 0;
            const char *v = NULL;
            if (r.read_str(&v, &vlen, (uint32_t)sizeof(tmp.bin_sha256) - 1) != ERR_OK)
                return r.err();
            memcpy(tmp.bin_sha256, v, vlen);
            tmp.bin_sha256[vlen] = '\0';
        } else if (key_len == 8 && memcmp(key, "bin_size", 8) == 0) {
            if (seen & 8u)
                return ERR_DUP_KEY;
            seen |= 8u;
            const uint64_t v = r.expect_u64();
            if (v > UINT32_MAX)
                return ERR_OVERFLOW;
            tmp.bin_size = (uint32_t)v;
        } else if (key_len == 8 && memcmp(key, "download", 8) == 0) {
            if (seen & 16u)
                return ERR_DUP_KEY;
            seen |= 16u;
            uint32_t dl_seen = 0; // bit0=url bit1=token bit2=expires_at
            const uint32_t dl_count = r.expect_map();
            if (!r.ok())
                return r.err();
            for (uint32_t j = 0; j < dl_count; ++j) {
                char dl_key[24];
                size_t dl_key_len = 0;
                if (r.read_key(dl_key, sizeof(dl_key), &dl_key_len) != ERR_OK)
                    return r.err();
                uint32_t vlen = 0;
                const char *v = NULL;
                if (dl_key_len == 3 && memcmp(dl_key, "url", 3) == 0) {
                    if (dl_seen & 1u)
                        return ERR_DUP_KEY;
                    dl_seen |= 1u;
                    if (r.read_str(&v, &vlen, (uint32_t)sizeof(tmp.download_url) - 1) != ERR_OK)
                        return r.err();
                    memcpy(tmp.download_url, v, vlen);
                    tmp.download_url[vlen] = '\0';
                } else if (dl_key_len == 5 && memcmp(dl_key, "token", 5) == 0) {
                    if (dl_seen & 2u)
                        return ERR_DUP_KEY;
                    dl_seen |= 2u;
                    if (r.read_str(&v, &vlen, (uint32_t)sizeof(tmp.download_token) - 1) != ERR_OK)
                        return r.err();
                    memcpy(tmp.download_token, v, vlen);
                    tmp.download_token[vlen] = '\0';
                } else if (dl_key_len == 10 && memcmp(dl_key, "expires_at", 10) == 0) {
                    if (dl_seen & 4u)
                        return ERR_DUP_KEY;
                    dl_seen |= 4u;
                    if (r.read_str(&v, &vlen, (uint32_t)sizeof(tmp.download_expires_at) - 1) != ERR_OK)
                        return r.err();
                    memcpy(tmp.download_expires_at, v, vlen);
                    tmp.download_expires_at[vlen] = '\0';
                } else {
                    r.skip_value(); // unknown download field
                }
                if (!r.ok())
                    return r.err();
            }
            if ((dl_seen & 3u) != 3u)
                return ERR_MISSING_FIELD; // url + token
        } else if (key_len == 7 && memcmp(key, "version", 7) == 0) {
            if (seen & 32u)
                return ERR_DUP_KEY;
            seen |= 32u;
            uint32_t vlen = 0;
            const char *v = NULL;
            if (r.read_str(&v, &vlen, (uint32_t)sizeof(tmp.version) - 1) != ERR_OK)
                return r.err();
            memcpy(tmp.version, v, vlen);
            tmp.version[vlen] = '\0';
            tmp.has_version = true;
        } else {
            r.skip_value(); // unknown top-level field
        }

        if (!r.ok())
            return r.err();
    }

    if ((seen & 31u) != 31u)
        return ERR_MISSING_FIELD; // all but version
    if (tmp.release_id[0] == '\0' || tmp.job_id[0] == '\0' || tmp.bin_size == 0 || tmp.download_url[0] == '\0' ||
        tmp.download_token[0] == '\0' || strlen(tmp.bin_sha256) != 64u) {
        return ERR_FIELD_LEN;
    }
    for (size_t i = 0; i < 64u; ++i) {
        const char c = tmp.bin_sha256[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
            return ERR_TYPE;
        }
    }
    if (r.finish() != ERR_OK)
        return r.err();

    *out = tmp;
    return ERR_OK;
}

// ------------------------------------------------------------------ //
// OTA result
// ------------------------------------------------------------------ //

int32_t soulcloud::encode_ota_result(uint8_t *buf, size_t cap, size_t *out_len, const ota_result *res)
{
    msgpack_writer w(buf, cap);

    const bool has_message = (res->message != NULL);
    w.start_map(has_message ? 5u : 4u);
    w.write_str("release_id");
    w.write_str(res->release_id);
    w.write_str("job_id");
    w.write_str(res->job_id);
    w.write_str("state");
    w.write_str(res->state);
    w.write_str("code");
    w.write_int(res->code);
    if (has_message) {
        w.write_str("message");
        w.write_str(res->message);
    }
    w.finish_map();

    if (!w.ok())
        return w.err();
    *out_len = w.bytes_written();
    return w.finish();
}
// ------------------------------------------------------------------ //
// log uplink container
// ------------------------------------------------------------------ //

int32_t soulcloud::encode_log_container(uint8_t *buf, size_t cap, size_t *out_len, const uint8_t *const *pkts, const size_t *lens,
                                        uint32_t n)
{
    if (buf == nullptr || out_len == nullptr || pkts == nullptr || lens == nullptr) {
        return ERR_BAD_MSG;
    }
    if (n == 0 || n > LOG_CONTAINER_MAX_ELEMS) {
        return ERR_OVERFLOW;
    }
    if (cap < 1) {
        return ERR_OVERFLOW;
    }
    // Element sanity: empty or non-on9log elements are rejected by the
    // platform, so refuse to send them (one bad element used to poison
    // the whole container; better to drop it device-side).
    for (uint32_t i = 0; i < n; ++i) {
        if (lens[i] == 0 || pkts[i] == nullptr || pkts[i][0] != 0x9a) {
            return ERR_BAD_MSG;
        }
    }

    buf[0] = 0x01; // container type byte (outside the msgpack payload)
    msgpack_writer w(buf + 1, cap - 1);
    w.start_array(n);
    for (uint32_t i = 0; i < n; ++i) {
        w.write_bin(pkts[i], lens[i]);
    }
    w.finish_array();
    if (w.err() != ERR_OK) {
        return w.err(); // ERR_OVERFLOW (cap too small)
    }
    *out_len = 1 + w.bytes_written();
    return ERR_OK;
}
