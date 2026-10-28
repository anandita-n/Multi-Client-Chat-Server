/*
 * client.c - interactive terminal client for the TCP chat server.
 *
 * usage: client [-h host] [-p port] NICK
 *
 *   /join ROOM        join a room (it becomes the current room)
 *   /leave [ROOM]     leave a room
 *   /msg NICK TEXT    private message
 *   /list [ROOM]      who is in a room
 *   /ping             measure round-trip time
 *   /quit             disconnect
 *   anything else     sent to the current room
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "protocol.h"

static int fd;
static char current_room[NAME_MAX_LEN + 1];
static uint64_t next_id = 1;

static void send_frame(uint8_t type, const wr_t *w)
{
    static uint8_t f[FRAME_HDR + MAX_FRAME];
    size_t n = frame_encode(f, sizeof f, type, w->buf, w->len);
    for (size_t off = 0; off < n;) {
        ssize_t s = send(fd, f + off, n - off, MSG_NOSIGNAL);
        if (s <= 0) {
            perror("send");
            exit(1);
        }
        off += s;
    }
}

static void send_text(uint8_t type, const char *target, const char *text)
{
    static uint8_t p[MAX_FRAME];
    wr_t w;
    wr_init(&w, p, sizeof p);
    wr_str(&w, target);
    wr_u64(&w, next_id++);
    wr_u64(&w, now_ns());
    size_t n = strlen(text);
    wr_bytes(&w, text, n > MAX_BODY ? MAX_BODY : n);
    send_frame(type, &w);
}

static void send_name(uint8_t type, const char *name)
{
    uint8_t p[64];
    wr_t w;
    wr_init(&w, p, sizeof p);
    wr_str(&w, name);
    send_frame(type, &w);
}

static void handle_line(char *line)
{
    line[strcspn(line, "\r\n")] = '\0';
    if (line[0] == '\0')
        return;
    if (line[0] != '/') {
        if (!current_room[0])
            printf("! join a room first (/join ROOM)\n");
        else
            send_text(MSG_ROOM, current_room, line);
        return;
    }
    char *cmd = strtok(line, " ");
    char *arg = strtok(NULL, " ");
    char *rest = arg ? strtok(NULL, "") : NULL;
    if (!strcmp(cmd, "/join") && arg) {
        send_name(MSG_JOIN, arg);
        snprintf(current_room, sizeof current_room, "%s", arg);
    } else if (!strcmp(cmd, "/leave")) {
        const char *room = arg ? arg : current_room;
        if (room[0])
            send_name(MSG_LEAVE, room);
        if (!strcmp(room, current_room))
            current_room[0] = '\0';
    } else if (!strcmp(cmd, "/msg") && arg && rest) {
        send_text(MSG_PRIVATE, arg, rest);
    } else if (!strcmp(cmd, "/list")) {
        send_name(MSG_LIST, arg ? arg : current_room);
    } else if (!strcmp(cmd, "/ping")) {
        uint8_t p[8];
        wr_t w;
        wr_init(&w, p, sizeof p);
        wr_u64(&w, now_ns());
        send_frame(MSG_PING, &w);
    } else if (!strcmp(cmd, "/quit")) {
        wr_t w;
        wr_init(&w, NULL, 0);
        send_frame(MSG_BYE, &w);
        exit(0);
    } else {
        printf("! commands: /join R, /leave [R], /msg N TEXT, /list [R], /ping, /quit\n");
    }
}

static void print_frame(uint8_t type, const uint8_t *payload, uint32_t len)
{
    rd_t rd;
    rd_init(&rd, payload, len);
    char a[64], b[64];
    size_t blen;
    const uint8_t *body;
    switch (type) {
    case MSG_WELCOME:
        rd_str(&rd, a, sizeof a);
        printf("* connected as %s\n", a);
        break;
    case MSG_ROOM:
        rd_str(&rd, a, sizeof a);
        rd_str(&rd, b, sizeof b);
        rd_u64(&rd);
        rd_u64(&rd);
        body = rd_rest(&rd, &blen);
        printf("[%s] %s: %.*s\n", a, b, (int)blen, (const char *)body);
        break;
    case MSG_PRIVATE:
        rd_str(&rd, a, sizeof a);
        rd_u64(&rd);
        rd_u64(&rd);
        body = rd_rest(&rd, &blen);
        printf("(private) %s: %.*s\n", a, (int)blen, (const char *)body);
        break;
    case MSG_PRESENCE: {
        uint8_t ev = rd_u8(&rd);
        rd_str(&rd, a, sizeof a);
        rd_str(&rd, b, sizeof b);
        const char *what = ev == PRESENCE_JOINED ? "joined"
                         : ev == PRESENCE_LEFT   ? "left"
                                                 : "went offline from";
        printf("* %s %s %s\n", b, what, a);
        break;
    }
    case MSG_LIST: {
        rd_str(&rd, a, sizeof a);
        unsigned n = rd_u16(&rd);
        printf("* %s (%u):", a, n);
        for (unsigned i = 0; i < n && !rd.err; i++) {
            rd_str(&rd, b, sizeof b);
            printf(" %s", b);
        }
        printf("\n");
        break;
    }
    case MSG_ERROR:
        rd_str(&rd, a, sizeof a);
        printf("! error: %s\n", a);
        break;
    case MSG_PONG:
        printf("* pong: %.3f ms\n", (now_ns() - rd_u64(&rd)) / 1e6);
        break;
    default:
        printf("? unexpected %s\n", msg_type_name(type));
    }
    fflush(stdout);
}

int main(int argc, char **argv)
{
    const char *host = "127.0.0.1", *port = "9000";
    int opt;
    while ((opt = getopt(argc, argv, "h:p:")) != -1) {
        if (opt == 'h')
            host = optarg;
        else if (opt == 'p')
            port = optarg;
    }
    if (optind >= argc) {
        fprintf(stderr, "usage: %s [-h host] [-p port] NICK\n", argv[0]);
        return 2;
    }

    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM }, *ai;
    if (getaddrinfo(host, port, &hints, &ai) != 0) {
        fprintf(stderr, "cannot resolve %s\n", host);
        return 1;
    }
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(fd, ai->ai_addr, ai->ai_addrlen) < 0) {
        perror("connect");
        return 1;
    }
    freeaddrinfo(ai);
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    send_name(MSG_HELLO, argv[optind]);

    buf_t in = { 0 };
    struct pollfd pfds[2] = { { .fd = 0, .events = POLLIN }, { .fd = fd, .events = POLLIN } };
    char line[MAX_BODY + 64];
    for (;;) {
        if (poll(pfds, 2, -1) < 0)
            continue;
        if (pfds[0].revents & (POLLIN | POLLHUP)) {
            if (!fgets(line, sizeof line, stdin))
                break;
            handle_line(line);
        }
        if (pfds[1].revents & (POLLIN | POLLHUP)) {
            buf_reserve(&in, 65536);
            ssize_t n = recv(fd, in.data + in.end, 65536, 0);
            if (n <= 0) {
                printf("* server closed the connection\n");
                break;
            }
            in.end += n;
            uint8_t type;
            const uint8_t *payload;
            uint32_t plen;
            size_t used;
            int r;
            while ((r = frame_parse(buf_head(&in), buf_len(&in), &type,
                                    &payload, &plen, &used)) == 1) {
                print_frame(type, payload, plen);
                buf_consume(&in, used);
            }
            if (r < 0) {
                printf("! protocol error from server\n");
                break;
            }
        }
    }
    close(fd);
    return 0;
}
