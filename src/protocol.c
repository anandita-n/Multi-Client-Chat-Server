#include "protocol.h"

#include <ctype.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ---- writer ---- */

void wr_init(wr_t *w, uint8_t *buf, size_t cap)
{
    w->buf = buf;
    w->cap = cap;
    w->len = 0;
    w->overflow = 0;
}

void wr_bytes(wr_t *w, const void *p, size_t n)
{
    if (w->overflow || n > w->cap - w->len) {
        w->overflow = 1;
        return;
    }
    memcpy(w->buf + w->len, p, n);
    w->len += n;
}

void wr_u8(wr_t *w, uint8_t v) { wr_bytes(w, &v, 1); }

void wr_u16(wr_t *w, uint16_t v)
{
    uint8_t b[2] = { v >> 8, v };
    wr_bytes(w, b, 2);
}

void wr_u32(wr_t *w, uint32_t v)
{
    uint8_t b[4] = { v >> 24, v >> 16, v >> 8, v };
    wr_bytes(w, b, 4);
}

void wr_u64(wr_t *w, uint64_t v)
{
    wr_u32(w, (uint32_t)(v >> 32));
    wr_u32(w, (uint32_t)v);
}

void wr_str(wr_t *w, const char *s)
{
    size_t n = strlen(s);
    if (n > 255) {
        w->overflow = 1;
        return;
    }
    wr_u8(w, (uint8_t)n);
    wr_bytes(w, s, n);
}

/* ---- reader ---- */

void rd_init(rd_t *r, const uint8_t *buf, size_t len)
{
    r->buf = buf;
    r->len = len;
    r->pos = 0;
    r->err = 0;
}

static const uint8_t *rd_take(rd_t *r, size_t n)
{
    if (r->err || n > r->len - r->pos) {
        r->err = 1;
        return NULL;
    }
    const uint8_t *p = r->buf + r->pos;
    r->pos += n;
    return p;
}

uint8_t rd_u8(rd_t *r)
{
    const uint8_t *p = rd_take(r, 1);
    return p ? p[0] : 0;
}

uint16_t rd_u16(rd_t *r)
{
    const uint8_t *p = rd_take(r, 2);
    return p ? (uint16_t)(p[0] << 8 | p[1]) : 0;
}

uint32_t rd_u32(rd_t *r)
{
    const uint8_t *p = rd_take(r, 4);
    return p ? (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
               (uint32_t)p[2] << 8 | p[3]
             : 0;
}

uint64_t rd_u64(rd_t *r)
{
    uint64_t hi = rd_u32(r);
    return hi << 32 | rd_u32(r);
}

int rd_str(rd_t *r, char *out, size_t outsz)
{
    size_t n = rd_u8(r);
    const uint8_t *p = rd_take(r, n);
    if (!p || n >= outsz) {
        r->err = 1;
        if (outsz)
            out[0] = '\0';
        return -1;
    }
    memcpy(out, p, n);
    out[n] = '\0';
    return 0;
}

const uint8_t *rd_rest(rd_t *r, size_t *n)
{
    *n = r->err ? 0 : r->len - r->pos;
    const uint8_t *p = r->buf + r->pos;
    r->pos = r->len;
    return p;
}

/* ---- framing ---- */

size_t frame_encode(uint8_t *out, size_t cap, uint8_t type,
                    const uint8_t *payload, size_t len)
{
    size_t body = 1 + len;
    if (body > MAX_FRAME || FRAME_HDR + body > cap)
        return 0;
    out[0] = body >> 24;
    out[1] = body >> 16;
    out[2] = body >> 8;
    out[3] = body;
    out[4] = type;
    if (len)
        memcpy(out + 5, payload, len);
    return FRAME_HDR + body;
}

int frame_parse(const uint8_t *data, size_t avail, uint8_t *type,
                const uint8_t **payload, uint32_t *plen, size_t *used)
{
    if (avail < FRAME_HDR)
        return 0;
    uint32_t len = (uint32_t)data[0] << 24 | (uint32_t)data[1] << 16 |
                   (uint32_t)data[2] << 8 | data[3];
    /* Validate before waiting for the body, so a bogus length can't make
     * us buffer gigabytes. */
    if (len < 1 || len > MAX_FRAME)
        return -1;
    if (avail - FRAME_HDR < len)
        return 0;
    *type = data[FRAME_HDR];
    *payload = data + FRAME_HDR + 1;
    *plen = len - 1;
    *used = FRAME_HDR + len;
    return 1;
}

/* ---- buffer ---- */

int buf_reserve(buf_t *b, size_t n)
{
    if (b->cap - b->end >= n)
        return 0;
    /* Reclaim consumed space at the front before growing. */
    if (b->start > 0) {
        size_t live = buf_len(b);
        memmove(b->data, b->data + b->start, live);
        b->start = 0;
        b->end = live;
        if (b->cap - b->end >= n)
            return 0;
    }
    size_t cap = b->cap ? b->cap : 4096;
    while (cap - b->end < n)
        cap *= 2;
    uint8_t *p = realloc(b->data, cap);
    if (!p)
        return -1;
    b->data = p;
    b->cap = cap;
    return 0;
}

int buf_append(buf_t *b, const void *p, size_t n)
{
    if (buf_reserve(b, n) < 0)
        return -1;
    memcpy(b->data + b->end, p, n);
    b->end += n;
    return 0;
}

void buf_consume(buf_t *b, size_t n)
{
    b->start += n;
    if (b->start >= b->end)
        b->start = b->end = 0;
}

void buf_free(buf_t *b)
{
    free(b->data);
    b->data = NULL;
    b->start = b->end = b->cap = 0;
}

/* ---- misc ---- */

uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

int set_nonblocking(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    return fl < 0 ? -1 : fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

int valid_name(const char *s)
{
    size_t n = strlen(s);
    if (n < 1 || n > NAME_MAX_LEN)
        return 0;
    for (size_t i = 0; i < n; i++)
        if (!isalnum((unsigned char)s[i]) && s[i] != '_' && s[i] != '-')
            return 0;
    return 1;
}

const char *msg_type_name(uint8_t type)
{
    static const char *names[] = {
        "?", "HELLO", "WELCOME", "JOIN", "LEAVE", "ROOM", "PRIVATE",
        "PRESENCE", "LIST", "ERROR", "PING", "PONG", "BYE",
    };
    return type < sizeof names / sizeof names[0] ? names[type] : "?";
}
