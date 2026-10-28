/*
 * integration_test.c - checks the framing logic and the TCP server's chat
 * behaviour end to end.
 *
 * usage: integration_test HOST PORT   (expects server_tcp to be running)
 *
 * The framing cases are the important ones: TCP may split one message across
 * many reads or merge many messages into one read, and the server must
 * produce exactly the same result either way.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "protocol.h"

static const char *host;
static int port;
static int failures, checks;

#define CHECK(cond, ...)                                                    \
    do {                                                                    \
        checks++;                                                           \
        if (!(cond)) {                                                      \
            failures++;                                                     \
            fprintf(stderr, "  FAIL %s:%d: ", __FILE__, __LINE__);          \
            fprintf(stderr, __VA_ARGS__);                                   \
            fputc('\n', stderr);                                            \
        }                                                                   \
    } while (0)

typedef struct {
    int fd;
    buf_t in;
} tconn;

typedef struct {
    uint8_t type;
    uint8_t payload[MAX_FRAME];
    uint32_t len;
} frame_t;

static tconn tconnect(void)
{
    tconn c = { .fd = socket(AF_INET, SOCK_STREAM, 0) };
    struct sockaddr_in sa = { .sin_family = AF_INET, .sin_port = htons(port) };
    inet_pton(AF_INET, host, &sa.sin_addr);
    if (c.fd < 0 || connect(c.fd, (struct sockaddr *)&sa, sizeof sa) < 0) {
        perror("connect");
        exit(2);
    }
    int one = 1;
    setsockopt(c.fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return c;
}

static void tclose(tconn *c)
{
    close(c->fd);
    buf_free(&c->in);
}

static void send_all(int fd, const void *p, size_t n)
{
    const uint8_t *b = p;
    while (n > 0) {
        ssize_t w = send(fd, b, n, MSG_NOSIGNAL);
        if (w <= 0) {
            perror("send");
            exit(2);
        }
        b += w;
        n -= w;
    }
}

static size_t build(uint8_t *out, uint8_t type, const wr_t *w)
{
    return frame_encode(out, FRAME_HDR + MAX_FRAME, type, w ? w->buf : NULL,
                        w ? w->len : 0);
}

static void send_frame(tconn *c, uint8_t type, const wr_t *w)
{
    static uint8_t f[FRAME_HDR + MAX_FRAME];
    send_all(c->fd, f, build(f, type, w));
}

/* Reads the next frame. Returns 1 on success, 0 on timeout, -1 on EOF. */
static int next_frame(tconn *c, frame_t *out, int timeout_ms)
{
    for (;;) {
        uint8_t type;
        const uint8_t *payload;
        uint32_t plen;
        size_t used;
        int r = frame_parse(buf_head(&c->in), buf_len(&c->in), &type,
                            &payload, &plen, &used);
        if (r < 0)
            return -1;
        if (r == 1) {
            out->type = type;
            out->len = plen;
            memcpy(out->payload, payload, plen);
            buf_consume(&c->in, used);
            return 1;
        }
        struct pollfd pfd = { .fd = c->fd, .events = POLLIN };
        if (poll(&pfd, 1, timeout_ms) <= 0)
            return 0;
        buf_reserve(&c->in, 65536);
        ssize_t n = recv(c->fd, c->in.data + c->in.end, 65536, 0);
        if (n <= 0)
            return -1;
        c->in.end += n;
    }
}

/* Reads frames until one of `type` arrives, skipping others. */
static int wait_for(tconn *c, uint8_t type, frame_t *out)
{
    for (;;) {
        int r = next_frame(c, out, 2000);
        if (r != 1)
            return r;
        if (out->type == type)
            return 1;
    }
}

static uint8_t wbuf[MAX_FRAME];

static wr_t w_str(const char *s)
{
    wr_t w;
    wr_init(&w, wbuf, sizeof wbuf);
    wr_str(&w, s);
    return w;
}

static void hello(tconn *c, const char *nick)
{
    wr_t w = w_str(nick);
    send_frame(c, MSG_HELLO, &w);
    frame_t f;
    CHECK(wait_for(c, MSG_WELCOME, &f) == 1, "%s: no WELCOME", nick);
}

static void join(tconn *c, const char *room)
{
    wr_t w = w_str(room);
    send_frame(c, MSG_JOIN, &w);
    frame_t f;
    CHECK(wait_for(c, MSG_LIST, &f) == 1, "join %s: no LIST ack", room);
}

static void test_parser_unit(void)
{
    fprintf(stderr, "[unit] frame_parse edge cases\n");
    uint8_t buf[64], p[3] = { 'a', 'b', 'c' };
    size_t n1 = frame_encode(buf, sizeof buf, MSG_PING, p, 3);
    size_t n2 = frame_encode(buf + n1, sizeof buf - n1, MSG_PONG, p, 2);
    uint8_t type;
    const uint8_t *pl;
    uint32_t plen;
    size_t used;

    for (size_t k = 0; k < n1; k++)
        CHECK(frame_parse(buf, k, &type, &pl, &plen, &used) == 0,
              "prefix of %zu bytes should be incomplete", k);
    CHECK(frame_parse(buf, n1 + n2, &type, &pl, &plen, &used) == 1 &&
              type == MSG_PING && plen == 3 && used == n1,
          "first of two merged frames");
    CHECK(frame_parse(buf + used, n2, &type, &pl, &plen, &used) == 1 &&
              type == MSG_PONG && plen == 2,
          "second of two merged frames");

    uint8_t zero[4] = { 0, 0, 0, 0 }, huge[4] = { 0x7f, 0xff, 0xff, 0xff };
    CHECK(frame_parse(zero, 4, &type, &pl, &plen, &used) == -1, "length 0 rejected");
    CHECK(frame_parse(huge, 4, &type, &pl, &plen, &used) == -1, "huge length rejected");
}

static void test_byte_by_byte(void)
{
    fprintf(stderr, "[tcp] frame delivered one byte per write (partial reads)\n");
    tconn a = tconnect();
    uint8_t f[64];
    wr_t w = w_str("trickle");
    size_t n = build(f, MSG_HELLO, &w);
    for (size_t i = 0; i < n; i++) {
        send_all(a.fd, f + i, 1);
        usleep(3000);           /* with TCP_NODELAY each byte is its own segment */
    }
    frame_t r;
    CHECK(wait_for(&a, MSG_WELCOME, &r) == 1, "no WELCOME after trickled HELLO");
    tclose(&a);
}

static void test_merged_and_chat(void)
{
    fprintf(stderr, "[tcp] 50 messages merged into one write, rooms, private, presence\n");
    tconn alice = tconnect(), bob = tconnect();
    hello(&alice, "alice");
    hello(&bob, "bob");
    join(&bob, "lobby");

    /* JOIN + 50 ROOM messages in a single send(). */
    static uint8_t big[64 * 1024];
    size_t off = 0;
    wr_t w = w_str("lobby");
    off += build(big + off, MSG_JOIN, &w);
    for (uint64_t id = 1; id <= 50; id++) {
        wr_init(&w, wbuf, sizeof wbuf);
        wr_str(&w, "lobby");
        wr_u64(&w, id);
        wr_u64(&w, now_ns());
        char body[32];
        int bl = snprintf(body, sizeof body, "msg-%lu", (unsigned long)id);
        wr_bytes(&w, body, bl);
        off += build(big + off, MSG_ROOM, &w);
    }
    send_all(alice.fd, big, off);

    frame_t f;
    CHECK(wait_for(&alice, MSG_LIST, &f) == 1, "alice: no LIST for lobby");
    CHECK(wait_for(&bob, MSG_PRESENCE, &f) == 1, "bob: no presence for alice");
    if (f.type == MSG_PRESENCE) {
        rd_t rd;
        rd_init(&rd, f.payload, f.len);
        char room[32], nick[32];
        uint8_t ev = rd_u8(&rd);
        rd_str(&rd, room, sizeof room);
        rd_str(&rd, nick, sizeof nick);
        CHECK(ev == PRESENCE_JOINED && !strcmp(nick, "alice"), "bob: wrong presence");
    }
    int got = 0, in_order = 1;
    for (uint64_t id = 1; id <= 50; id++) {
        if (wait_for(&bob, MSG_ROOM, &f) != 1)
            break;
        rd_t rd;
        rd_init(&rd, f.payload, f.len);
        char room[32], from[32], expect[32];
        rd_str(&rd, room, sizeof room);
        rd_str(&rd, from, sizeof from);
        uint64_t mid = rd_u64(&rd);
        rd_u64(&rd);
        size_t bl;
        const uint8_t *body = rd_rest(&rd, &bl);
        int el = snprintf(expect, sizeof expect, "msg-%lu", (unsigned long)id);
        if (mid != id || strcmp(from, "alice") || bl != (size_t)el ||
            memcmp(body, expect, el))
            in_order = 0;
        got++;
    }
    CHECK(got == 50, "bob received %d/50 room messages", got);
    CHECK(in_order, "room messages corrupted or out of order");

    /* Private message, and one to a user who doesn't exist. */
    wr_init(&w, wbuf, sizeof wbuf);
    wr_str(&w, "alice");
    wr_u64(&w, 7);
    wr_u64(&w, 0);
    wr_bytes(&w, "psst", 4);
    send_frame(&bob, MSG_PRIVATE, &w);
    CHECK(wait_for(&alice, MSG_PRIVATE, &f) == 1, "alice: no private message");
    wr_init(&w, wbuf, sizeof wbuf);
    wr_str(&w, "nobody");
    wr_u64(&w, 8);
    wr_u64(&w, 0);
    send_frame(&bob, MSG_PRIVATE, &w);
    CHECK(wait_for(&bob, MSG_ERROR, &f) == 1, "bob: no error for unknown user");

    /* Duplicate nickname is refused. */
    tconn imp = tconnect();
    w = w_str("alice");
    send_frame(&imp, MSG_HELLO, &w);
    CHECK(wait_for(&imp, MSG_ERROR, &f) == 1, "duplicate nick accepted");
    tclose(&imp);

    /* Presence: leave, rejoin, then disconnect. */
    w = w_str("lobby");
    send_frame(&alice, MSG_LEAVE, &w);
    CHECK(wait_for(&bob, MSG_PRESENCE, &f) == 1 && f.payload[0] == PRESENCE_LEFT,
          "bob: no LEFT presence");
    join(&alice, "lobby");
    CHECK(wait_for(&bob, MSG_PRESENCE, &f) == 1 && f.payload[0] == PRESENCE_JOINED,
          "bob: no JOINED presence after rejoin");
    tclose(&alice);
    CHECK(wait_for(&bob, MSG_PRESENCE, &f) == 1 && f.payload[0] == PRESENCE_OFFLINE,
          "bob: no OFFLINE presence after disconnect");

    /* The nickname is released when its owner disconnects. */
    tconn again = tconnect();
    hello(&again, "alice");
    tclose(&again);
    tclose(&bob);
}

static void test_requires_hello(void)
{
    fprintf(stderr, "[tcp] commands before HELLO are refused\n");
    tconn c = tconnect();
    wr_t w = w_str("lobby");
    send_frame(&c, MSG_JOIN, &w);
    frame_t f;
    CHECK(wait_for(&c, MSG_ERROR, &f) == 1, "JOIN before HELLO accepted");
    tclose(&c);
}

static void test_ping(void)
{
    fprintf(stderr, "[tcp] PING echoes timestamp\n");
    tconn c = tconnect();
    wr_t w;
    wr_init(&w, wbuf, sizeof wbuf);
    wr_u64(&w, 0x1122334455667788ull);
    send_frame(&c, MSG_PING, &w);
    frame_t f;
    int ok = wait_for(&c, MSG_PONG, &f) == 1;
    rd_t rd;
    rd_init(&rd, f.payload, f.len);
    CHECK(ok && rd_u64(&rd) == 0x1122334455667788ull, "bad PONG");
    tclose(&c);
}

static void test_oversized_frame(void)
{
    fprintf(stderr, "[tcp] oversized length prefix closes the connection\n");
    tconn c = tconnect();
    uint8_t hdr[4] = { 0x7f, 0xff, 0xff, 0xff };
    send_all(c.fd, hdr, 4);
    frame_t f;
    int r;
    while ((r = next_frame(&c, &f, 2000)) == 1)
        ;                       /* the ERROR frame, then EOF */
    CHECK(r == -1, "server did not close connection");
    tclose(&c);
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "usage: %s HOST PORT\n", argv[0]);
        return 2;
    }
    host = argv[1];
    port = atoi(argv[2]);

    test_parser_unit();
    test_byte_by_byte();
    test_merged_and_chat();
    test_requires_hello();
    test_ping();
    test_oversized_frame();

    fprintf(stderr, "%s: %d/%d checks passed\n", failures ? "FAILED" : "OK",
            checks - failures, checks);
    return failures ? 1 : 0;
}
