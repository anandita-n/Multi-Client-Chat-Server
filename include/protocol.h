/*
 * protocol.h - wire protocol shared by servers, clients and tools.
 *
 * TCP framing (TCP is a byte stream, so messages need explicit boundaries):
 *
 *     +----------------+----------+---------------------------+
 *     | length (u32 BE)| type (u8)| payload (length - 1 bytes) |
 *     +----------------+----------+---------------------------+
 *
 *   `length` counts the type byte plus the payload, so a valid frame has
 *   1 <= length <= MAX_FRAME. Anything else is a protocol error and the
 *   connection is dropped.
 *
 * UDP datagrams (one message per datagram, so no length prefix is needed):
 *
 *     +----------+--------------+---------+
 *     | type (u8)| seq (u32 BE) | payload |
 *     +----------+--------------+---------+
 *
 *   `seq` is per-direction and per-peer: each client numbers what it sends,
 *   and the server numbers what it sends to each client separately, so the
 *   receiver can detect gaps (loss) and late arrivals (reordering).
 *
 * Payload encoding: integers are big-endian; a "str" is a u8 length followed
 * by that many bytes (no NUL); "body" is the remaining bytes of the payload.
 *
 * Client -> server payloads
 *   HELLO    str nick
 *   JOIN     str room
 *   LEAVE    str room
 *   ROOM     str room, u64 msg_id, u64 ts_ns, body
 *   PRIVATE  str to,   u64 msg_id, u64 ts_ns, body
 *   LIST     str room
 *   PING     u64 ts_ns
 *   BYE      (empty)
 *
 * Server -> client payloads
 *   WELCOME  str nick
 *   ROOM     str room, str from, u64 msg_id, u64 ts_ns, body
 *   PRIVATE  str from, u64 msg_id, u64 ts_ns, body
 *   PRESENCE u8 event, str room, str nick
 *   LIST     str room, u16 count, count x str nick
 *   ERROR    str text
 *   PONG     u64 ts_ns
 */
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stddef.h>
#include <stdint.h>

#define FRAME_HDR     4          /* u32 length prefix (TCP) */
#define UDP_HDR       5          /* u8 type + u32 seq (UDP) */
#define MAX_FRAME     65536      /* max type + payload bytes in one frame */
#define MAX_BODY      4096       /* max chat message body */
#define NAME_MAX_LEN  31         /* nicknames and room names */
#define UDP_MAX_DGRAM (UDP_HDR + MAX_FRAME)

enum msg_type {
    MSG_HELLO = 1,
    MSG_WELCOME,
    MSG_JOIN,
    MSG_LEAVE,
    MSG_ROOM,
    MSG_PRIVATE,
    MSG_PRESENCE,
    MSG_LIST,
    MSG_ERROR,
    MSG_PING,
    MSG_PONG,
    MSG_BYE,
};

enum presence_event {
    PRESENCE_JOINED = 1,
    PRESENCE_LEFT,
    PRESENCE_OFFLINE,
};

/* ---- payload writer: appends into a caller-owned buffer ---- */

typedef struct {
    uint8_t *buf;
    size_t cap, len;
    int overflow;               /* set if any write did not fit */
} wr_t;

void wr_init(wr_t *w, uint8_t *buf, size_t cap);
void wr_u8(wr_t *w, uint8_t v);
void wr_u16(wr_t *w, uint16_t v);
void wr_u32(wr_t *w, uint32_t v);
void wr_u64(wr_t *w, uint64_t v);
void wr_str(wr_t *w, const char *s);
void wr_bytes(wr_t *w, const void *p, size_t n);

/* ---- payload reader: bounds-checked, sets err instead of overrunning ---- */

typedef struct {
    const uint8_t *buf;
    size_t len, pos;
    int err;
} rd_t;

void rd_init(rd_t *r, const uint8_t *buf, size_t len);
uint8_t rd_u8(rd_t *r);
uint16_t rd_u16(rd_t *r);
uint32_t rd_u32(rd_t *r);
uint64_t rd_u64(rd_t *r);
/* Copies a str into out (NUL-terminated). Fails if it does not fit. */
int rd_str(rd_t *r, char *out, size_t outsz);
/* Returns the unread remainder of the payload. */
const uint8_t *rd_rest(rd_t *r, size_t *n);

/* ---- TCP framing ---- */

/*
 * Writes a complete frame (length prefix + type + payload) into out.
 * Returns the total frame size, or 0 if it does not fit / is too large.
 */
size_t frame_encode(uint8_t *out, size_t cap, uint8_t type,
                    const uint8_t *payload, size_t len);

/*
 * Looks for one complete frame at the start of data[0..avail).
 * Returns  1  frame found: *type, *payload, *plen filled, *used = bytes to consume
 *          0  incomplete: need more bytes (partial read)
 *         -1  invalid length prefix (protocol error)
 * Call repeatedly to drain several frames that arrived in one read.
 */
int frame_parse(const uint8_t *data, size_t avail, uint8_t *type,
                const uint8_t **payload, uint32_t *plen, size_t *used);

/* ---- growable byte buffer used for per-connection read/write queues ---- */

typedef struct {
    uint8_t *data;
    size_t start, end, cap;     /* live bytes are data[start..end) */
} buf_t;

static inline size_t buf_len(const buf_t *b) { return b->end - b->start; }
static inline uint8_t *buf_head(const buf_t *b) { return b->data + b->start; }

/* Ensures at least n free bytes after end. Returns 0 on success. */
int buf_reserve(buf_t *b, size_t n);
int buf_append(buf_t *b, const void *p, size_t n);
void buf_consume(buf_t *b, size_t n);
void buf_free(buf_t *b);

/* ---- misc helpers ---- */

uint64_t now_ns(void);                 /* CLOCK_MONOTONIC in nanoseconds */
int set_nonblocking(int fd);
int valid_name(const char *s);         /* 1..NAME_MAX_LEN of [A-Za-z0-9_-] */
const char *msg_type_name(uint8_t type);

#endif
