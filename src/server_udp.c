/*
 * server_udp.c - single-threaded UDP variant of the chat server.
 *
 * Same chat core (rooms, private messages, presence) as server_tcp, but the
 * transport is one UDP socket, which changes three things:
 *
 *   1. No framing: each datagram is exactly one message, so there is no
 *      partial-read / merged-message problem to solve.
 *   2. No connections: a client is identified by its source "ip:port".
 *      HELLO from a new address creates it; since there is no FIN/RST,
 *      clients are dropped after CLIENT_TIMEOUT without traffic (PING
 *      works as a heartbeat), or immediately on BYE.
 *   3. No delivery guarantee: every datagram carries a sequence number
 *      (see protocol.h). The server numbers its outgoing datagrams per
 *      recipient, so a client can count gaps (loss) and late arrivals
 *      (reordering); the server does the same for what it receives.
 *      Lost messages are NOT retransmitted - the point of this variant is
 *      to measure what raw UDP does compared with TCP.
 *
 * epoll watches the socket and a 1-second timerfd that drives timeouts.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include "chat.h"
#include "hashmap.h"
#include "protocol.h"

#define CLIENT_TIMEOUT_NS (10ull * 1000000000ull)

typedef struct {
    client_t c;                 /* must be first: chat core hands us client_t* */
    struct sockaddr_in addr;
    char key[32];               /* "ip:port" */
    uint32_t seq_out;           /* last sequence number sent to this client */
    uint32_t seq_in_max;        /* highest sequence number received from it */
    uint64_t last_seen;
    size_t idx;                 /* position in `all` */
} uconn_t;

static int sock = -1;
static chat_t chat;
static hmap_t by_addr;          /* "ip:port" -> uconn_t* */
static uconn_t **all;           /* dense list for timeout sweeps */
static size_t nall, all_cap;
static volatile sig_atomic_t stop;

static struct {
    uint64_t dgrams_in, dgrams_out, in_gaps, in_late, send_drops, timeouts, peak;
} stats;

static void on_signal(int sig) { (void)sig; stop = 1; }

static void addr_key(const struct sockaddr_in *a, char *out, size_t n)
{
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &a->sin_addr, ip, sizeof ip);
    snprintf(out, n, "%s:%u", ip, ntohs(a->sin_port));
}

static void send_dgram(const struct sockaddr_in *to, uint8_t type, uint32_t seq,
                       const uint8_t *payload, size_t len)
{
    static uint8_t d[UDP_MAX_DGRAM];
    if (UDP_HDR + len > sizeof d)
        return;
    d[0] = type;
    d[1] = seq >> 24;
    d[2] = seq >> 16;
    d[3] = seq >> 8;
    d[4] = seq;
    if (len)
        memcpy(d + UDP_HDR, payload, len);
    if (sendto(sock, d, UDP_HDR + len, 0, (const struct sockaddr *)to,
               sizeof *to) < 0) {
        /* Socket buffer full: for UDP, dropping is the honest behaviour. */
        stats.send_drops++;
        return;
    }
    stats.dgrams_out++;
}

/* chat core -> wire */
static void udp_send(chat_t *ch, client_t *to, uint8_t type,
                     const uint8_t *payload, size_t len)
{
    (void)ch;
    uconn_t *u = (uconn_t *)to;
    send_dgram(&u->addr, type, ++u->seq_out, payload, len);
}

static uconn_t *conn_new(const struct sockaddr_in *from, const char *key)
{
    uconn_t *u = calloc(1, sizeof *u);
    if (!u)
        return NULL;
    u->addr = *from;
    snprintf(u->key, sizeof u->key, "%s", key);
    if (nall == all_cap) {
        size_t cap = all_cap ? all_cap * 2 : 256;
        uconn_t **p = realloc(all, cap * sizeof *p);
        if (!p) {
            free(u);
            return NULL;
        }
        all = p;
        all_cap = cap;
    }
    if (hm_put(&by_addr, key, u) < 0) {
        free(u);
        return NULL;
    }
    u->idx = nall;
    all[nall++] = u;
    if (nall > stats.peak)
        stats.peak = nall;
    return u;
}

static void conn_drop(uconn_t *u)
{
    chat_disconnect(&chat, &u->c);      /* OFFLINE presence to room-mates */
    hm_del(&by_addr, u->key);
    all[u->idx] = all[--nall];
    all[u->idx]->idx = u->idx;
    free(u);
}

static void handle_dgram(const uint8_t *d, size_t n, const struct sockaddr_in *from)
{
    stats.dgrams_in++;
    if (n < UDP_HDR)
        return;
    uint8_t type = d[0];
    uint32_t seq = (uint32_t)d[1] << 24 | (uint32_t)d[2] << 16 |
                   (uint32_t)d[3] << 8 | d[4];
    const uint8_t *payload = d + UDP_HDR;
    size_t plen = n - UDP_HDR;

    char key[32];
    addr_key(from, key, sizeof key);
    uconn_t *u = hm_get(&by_addr, key);
    if (!u) {
        if (type != MSG_HELLO) {
            uint8_t p[64];
            wr_t w;
            wr_init(&w, p, sizeof p);
            wr_str(&w, "send HELLO first");
            send_dgram(from, MSG_ERROR, 0, w.buf, w.len);
            return;
        }
        if (!(u = conn_new(from, key)))
            return;
    }

    /* Sequence tracking for the client -> server direction. */
    if (seq > u->seq_in_max + 1)
        stats.in_gaps += seq - u->seq_in_max - 1;
    if (seq <= u->seq_in_max)
        stats.in_late++;
    else
        u->seq_in_max = seq;
    u->last_seen = now_ns();

    if (type == MSG_BYE) {
        conn_drop(u);
        return;
    }
    chat_handle(&chat, &u->c, type, payload, plen);
    if (!u->c.registered)
        conn_drop(u);           /* HELLO was rejected; don't keep the address */
}

static void sweep_timeouts(void)
{
    uint64_t now = now_ns();
    /* Iterate backwards: conn_drop moves the last entry into the hole. */
    for (size_t i = nall; i-- > 0;) {
        if (now - all[i]->last_seen > CLIENT_TIMEOUT_NS) {
            stats.timeouts++;
            conn_drop(all[i]);
        }
    }
}

int main(int argc, char **argv)
{
    const char *host = "0.0.0.0";
    int port = 9001, opt;
    while ((opt = getopt(argc, argv, "b:p:h")) != -1) {
        switch (opt) {
        case 'b': host = optarg; break;
        case 'p': port = atoi(optarg); break;
        default:
            fprintf(stderr, "usage: %s [-b bind_addr] [-p port]\n", argv[0]);
            return opt == 'h' ? 0 : 2;
        }
    }

    struct sigaction sa = { .sa_handler = on_signal };
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (chat_init(&chat, udp_send, 1) < 0 || hm_init(&by_addr) < 0)
        return 1;
    sock = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
    if (sock < 0 || inet_pton(AF_INET, host, &addr.sin_addr) != 1 ||
        bind(sock, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("socket/bind");
        return 1;
    }
    int sz = 4 << 20;           /* capped by net.core.{r,w}mem_max */
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &sz, sizeof sz);
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sz, sizeof sz);

    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    struct itimerspec its = { .it_interval = { 1, 0 }, .it_value = { 1, 0 } };
    timerfd_settime(tfd, 0, &its, NULL);

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    struct epoll_event ev = { .events = EPOLLIN, .data.fd = sock };
    epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev);
    ev.data.fd = tfd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, tfd, &ev);
    fprintf(stderr, "udp chat server listening on %s:%d\n", host, port);

    static uint8_t d[UDP_MAX_DGRAM];
    struct epoll_event events[2];
    while (!stop) {
        int n = epoll_wait(epfd, events, 2, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == tfd) {
                uint64_t expirations;
                if (read(tfd, &expirations, sizeof expirations) > 0)
                    sweep_timeouts();
                continue;
            }
            /* Bounded batch per wakeup so the timer is never starved. */
            for (int k = 0; k < 1024; k++) {
                struct sockaddr_in from;
                socklen_t fl = sizeof from;
                ssize_t r = recvfrom(sock, d, sizeof d, 0,
                                     (struct sockaddr *)&from, &fl);
                if (r < 0)
                    break;      /* EAGAIN: drained */
                handle_dgram(d, (size_t)r, &from);
            }
        }
    }

    fprintf(stderr,
            "\nshutdown: peak_clients=%lu dgrams_in=%lu dgrams_out=%lu "
            "uplink_gaps=%lu uplink_late=%lu send_drops=%lu timeouts=%lu\n",
            stats.peak, stats.dgrams_in, stats.dgrams_out, stats.in_gaps,
            stats.in_late, stats.send_drops, stats.timeouts);
    while (nall)
        conn_drop(all[nall - 1]);
    close(sock);
    close(tfd);
    close(epfd);
    chat_free(&chat);
    hm_free(&by_addr);
    free(all);
    return 0;
}
