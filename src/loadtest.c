/*
 * loadtest.c - simulates many concurrent chat users against server_tcp or
 * server_udp and measures latency, throughput, loss and reordering.
 *
 * usage: loadtest [-P tcp|udp] [-h host] [-p port] [-c clients] [-r rooms]
 *                 [-R msgs_per_sec] [-d seconds] [-s body_bytes]
 *                 [-w drain_seconds] [-o results.csv] [-l label]
 *                 [-S command_to_run_after_setup]
 *
 * Client i joins room "<tag>_r<i % rooms>" as nick "<tag>_u<i>", where tag
 * is unique per run. During the run, messages are sent at a fixed total rate
 * from randomly chosen clients; each one is delivered by the server to the
 * other members of the sender's room.
 *
 * Because every simulated client lives in this one process, sender and
 * receiver share CLOCK_MONOTONIC, so one-way latency is simply
 * receive_time - send_timestamp carried in the message.
 *
 *   expected   = sum over sent messages of (room size - 1)
 *   delivery % = unique deliveries / expected
 *   per-sender out of order = a message from sender S that arrives after a
 *                later message from S (msg_ids per sender are increasing)
 *   UDP only: the server numbers every datagram it sends to each recipient,
 *   so the receiver sees the server->client stream directly: a datagram
 *   with a lower seq than one already received is "late" (reordered), and
 *   seq numbers never seen are missing (lost).
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <math.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "protocol.h"

typedef struct {
    int fd, id, room;
    buf_t in, out;              /* TCP stream buffers */
    int want_out;
    int joined;
    uint64_t next_msg_id;
    uint32_t seq_out, seq_in_max;   /* UDP sequence numbers */
    uint64_t *last_from;        /* highest msg_id seen per room-mate */
} lc_t;

static struct {
    int udp;
    const char *host, *label, *csv, *after_setup;
    int port, clients, rooms, body;
    double rate, duration, drain;
} opt = { 0, "127.0.0.1", "run", NULL, NULL, 9000, 200, 10, 64, 2000, 10, 3 };

/* Per-run prefix for nick and room names ("t<pid>"), so clients left over
 * from an earlier run that the server hasn't timed out yet can't collide. */
static char tag[16];

static lc_t *cl;
static int *room_size;
static int epfd;
static struct sockaddr_in server;
static int measuring;

static struct {
    uint64_t sent, expected, received, reordered, dups, udp_rx, udp_gaps,
             udp_late, local_drops, errors, joined;
    uint64_t *lat;              /* per-delivery latency, ns */
    size_t nlat, cap;
    uint64_t last_rx_ns;
} st;

static void die(const char *what)
{
    perror(what);
    exit(1);
}

/* ---- sending ---- */

static void update_events(lc_t *c, int want_out)
{
    if (c->want_out == want_out)
        return;
    struct epoll_event ev = { .events = EPOLLIN | (want_out ? EPOLLOUT : 0),
                              .data.u32 = (uint32_t)c->id };
    epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev);
    c->want_out = want_out;
}

static void tcp_flush(lc_t *c)
{
    while (buf_len(&c->out) > 0) {
        ssize_t n = send(c->fd, buf_head(&c->out), buf_len(&c->out), MSG_NOSIGNAL);
        if (n > 0)
            buf_consume(&c->out, n);
        else if (n < 0 && errno == EINTR)
            continue;
        else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            break;
        else
            die("tcp send");
    }
    update_events(c, buf_len(&c->out) > 0);
}

/* Returns 0 if the message left this process, -1 if dropped locally. */
static int send_msg(lc_t *c, uint8_t type, const wr_t *w)
{
    if (!opt.udp) {
        uint8_t f[FRAME_HDR + MAX_BODY + 128];
        size_t n = frame_encode(f, sizeof f, type, w->buf, w->len);
        if (buf_append(&c->out, f, n) < 0)
            die("buf_append");
        if (!c->want_out)
            tcp_flush(c);
        return 0;
    }
    uint8_t d[UDP_HDR + MAX_BODY + 128];
    uint32_t seq = ++c->seq_out;
    d[0] = type;
    d[1] = seq >> 24;
    d[2] = seq >> 16;
    d[3] = seq >> 8;
    d[4] = seq;
    if (w->len)
        memcpy(d + UDP_HDR, w->buf, w->len);
    if (send(c->fd, d, UDP_HDR + w->len, 0) < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS ||
            errno == ECONNREFUSED)
            return -1;
        die("udp send");
    }
    return 0;
}

static void send_hello_join(lc_t *c)
{
    uint8_t p[64];
    char name[32];
    wr_t w;
    wr_init(&w, p, sizeof p);
    snprintf(name, sizeof name, "%s_u%d", tag, c->id);
    wr_str(&w, name);
    send_msg(c, MSG_HELLO, &w);
    wr_init(&w, p, sizeof p);
    snprintf(name, sizeof name, "%s_r%d", tag, c->room);
    wr_str(&w, name);
    send_msg(c, MSG_JOIN, &w);
}

static void send_ping(lc_t *c)
{
    uint8_t p[8];
    wr_t w;
    wr_init(&w, p, sizeof p);
    wr_u64(&w, now_ns());
    send_msg(c, MSG_PING, &w);
}

static void send_chat(lc_t *c)
{
    static uint8_t body[MAX_BODY];
    if (!body[0])
        memset(body, 'x', sizeof body);
    uint8_t p[MAX_BODY + 64];
    char room[32];
    snprintf(room, sizeof room, "%s_r%d", tag, c->room);
    wr_t w;
    wr_init(&w, p, sizeof p);
    wr_str(&w, room);
    wr_u64(&w, ++c->next_msg_id);
    wr_u64(&w, now_ns());
    wr_bytes(&w, body, opt.body);
    if (send_msg(c, MSG_ROOM, &w) < 0) {
        st.local_drops++;
        return;
    }
    st.sent++;
    st.expected += room_size[c->room] - 1;
}

/* ---- receiving ---- */

static void record_latency(uint64_t ns)
{
    if (st.nlat == st.cap) {
        st.cap = st.cap ? st.cap * 2 : 1 << 16;
        st.lat = realloc(st.lat, st.cap * sizeof *st.lat);
        if (!st.lat)
            die("realloc");
    }
    st.lat[st.nlat++] = ns;
}

static void on_message(lc_t *c, uint8_t type, const uint8_t *payload, size_t len)
{
    rd_t rd;
    rd_init(&rd, payload, len);
    if (type == MSG_LIST) {
        if (!c->joined) {
            c->joined = 1;
            st.joined++;
        }
        return;
    }
    if (type == MSG_ERROR) {
        char text[128];
        rd_str(&rd, text, sizeof text);
        if (st.errors++ < 5)
            fprintf(stderr, "u%d: server error: %s\n", c->id, text);
        return;
    }
    if (type != MSG_ROOM || !measuring)
        return;

    char room[32], from[32];
    rd_str(&rd, room, sizeof room);
    rd_str(&rd, from, sizeof from);
    uint64_t id = rd_u64(&rd), ts = rd_u64(&rd);
    if (rd.err)
        return;
    uint64_t now = now_ns();
    const char *u = strrchr(from, 'u');     /* nick is "<tag>_u<id>" */
    int sender = u ? atoi(u + 1) : -1;
    if (sender < 0 || sender >= opt.clients)
        return;
    uint64_t *last = &c->last_from[sender / opt.rooms];
    if (id == *last) {
        st.dups++;
        return;
    }
    if (id < *last)
        st.reordered++;         /* an older message overtaken by a newer one */
    else
        *last = id;
    st.received++;
    st.last_rx_ns = now;
    record_latency(now - ts);
}

static void handle_tcp_read(lc_t *c)
{
    buf_reserve(&c->in, 65536);
    ssize_t n = recv(c->fd, c->in.data + c->in.end, 65536, 0);
    if (n == 0) {
        fprintf(stderr, "u%d: server closed connection\n", c->id);
        exit(1);
    }
    if (n < 0) {
        if (errno == EAGAIN || errno == EINTR)
            return;
        die("tcp recv");
    }
    c->in.end += n;
    uint8_t type;
    const uint8_t *payload;
    uint32_t plen;
    size_t used;
    int r;
    while ((r = frame_parse(buf_head(&c->in), buf_len(&c->in), &type,
                            &payload, &plen, &used)) == 1) {
        on_message(c, type, payload, plen);
        buf_consume(&c->in, used);
    }
    if (r < 0) {
        fprintf(stderr, "u%d: bad frame from server\n", c->id);
        exit(1);
    }
}

static void handle_udp_read(lc_t *c)
{
    static uint8_t d[UDP_MAX_DGRAM];
    for (;;) {
        ssize_t n = recv(c->fd, d, sizeof d, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ||
                errno == ECONNREFUSED)
                return;
            die("udp recv");
        }
        if (n < UDP_HDR)
            continue;
        uint32_t seq = (uint32_t)d[1] << 24 | (uint32_t)d[2] << 16 |
                       (uint32_t)d[3] << 8 | d[4];
        if (measuring) {
            st.udp_rx++;
            if (seq > c->seq_in_max + 1)
                st.udp_gaps += seq - c->seq_in_max - 1;
            if (seq <= c->seq_in_max)
                st.udp_late++;
        }
        if (seq > c->seq_in_max)
            c->seq_in_max = seq;
        on_message(c, d[0], d + UDP_HDR, n - UDP_HDR);
    }
}

static void poll_events(int timeout_ms)
{
    struct epoll_event evs[512];
    int n = epoll_wait(epfd, evs, 512, timeout_ms);
    for (int i = 0; i < n; i++) {
        lc_t *c = &cl[evs[i].data.u32];
        if (evs[i].events & EPOLLOUT)
            tcp_flush(c);
        if (evs[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP)) {
            if (opt.udp)
                handle_udp_read(c);
            else
                handle_tcp_read(c);
        }
    }
}

/* ---- setup ---- */

static void connect_client(lc_t *c)
{
    c->fd = socket(AF_INET, opt.udp ? SOCK_DGRAM : SOCK_STREAM, 0);
    if (c->fd < 0)
        die("socket");
    if (opt.udp) {
        int sz = 1 << 20;       /* capped by net.core.rmem_max */
        setsockopt(c->fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof sz);
    }
    /* Blocking connect keeps setup simple; for UDP it just fixes the peer. */
    if (connect(c->fd, (struct sockaddr *)&server, sizeof server) < 0)
        die("connect");
    if (!opt.udp) {
        int one = 1;
        setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    }
    set_nonblocking(c->fd);
    struct epoll_event ev = { .events = EPOLLIN, .data.u32 = (uint32_t)c->id };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, c->fd, &ev) < 0)
        die("epoll_ctl");
}

static void raise_fd_limit(void)
{
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

/* ---- results ---- */

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

static double pct(double p)
{
    if (st.nlat == 0)
        return NAN;
    size_t i = (size_t)ceil(p / 100.0 * st.nlat);
    if (i > 0)
        i--;
    return st.lat[i] / 1e3;     /* microseconds */
}

static void report(double run_s)
{
    qsort(st.lat, st.nlat, sizeof *st.lat, cmp_u64);
    double sum = 0;
    for (size_t i = 0; i < st.nlat; i++)
        sum += st.lat[i];
    double mean = st.nlat ? sum / st.nlat / 1e3 : NAN;
    double delivery = st.expected ? 100.0 * st.received / st.expected : NAN;
    double reorder = st.received ? 100.0 * st.reordered / st.received : 0;
    uint64_t lost = st.expected > st.received ? st.expected - st.received : 0;
    /* Server->client stream as seen through the per-recipient sequence
     * numbers: a late datagram first shows up as a gap and is then filled. */
    double udp_reorder = st.udp_rx ? 100.0 * st.udp_late / st.udp_rx : 0;
    uint64_t udp_seq_lost = st.udp_gaps > st.udp_late ? st.udp_gaps - st.udp_late : 0;

    printf("\n=== %s: %s, %d clients, %d rooms, target %.0f msg/s, %.0fs ===\n",
           opt.label, opt.udp ? "UDP" : "TCP", opt.clients, opt.rooms,
           opt.rate, opt.duration);
    printf("sent            %lu msgs (%.0f msg/s achieved)\n", st.sent, st.sent / run_s);
    printf("deliveries      %lu / %lu expected (%.3f%%), lost %lu\n",
           st.received, st.expected, delivery, lost);
    printf("throughput      %.0f deliveries/s\n", st.received / run_s);
    printf("per-sender      %lu out of order (%.3f%%), %lu duplicates\n",
           st.reordered, reorder, st.dups);
    if (opt.udp)
        printf("udp seq         %lu datagrams, %lu late (%.3f%% reordered), "
               "%lu missing, %lu local send drops\n",
               st.udp_rx, st.udp_late, udp_reorder, udp_seq_lost, st.local_drops);
    printf("latency (us)    p50 %.1f  p95 %.1f  p99 %.1f  max %.1f  mean %.1f\n",
           pct(50), pct(95), pct(99), st.nlat ? st.lat[st.nlat - 1] / 1e3 : NAN, mean);
    if (st.errors)
        printf("server errors   %lu\n", st.errors);

    if (!opt.csv)
        return;
    struct stat sb;
    int fresh = stat(opt.csv, &sb) != 0 || sb.st_size == 0;
    FILE *f = fopen(opt.csv, "a");
    if (!f)
        die(opt.csv);
    if (fresh)
        fprintf(f, "label,proto,clients,rooms,target_rate,duration_s,body_bytes,"
                   "sent,expected,received,delivery_pct,lost,sender_reordered,"
                   "sender_reorder_pct,dups,lat_p50_us,lat_p95_us,lat_p99_us,"
                   "lat_max_us,lat_mean_us,send_rate,deliver_rate,udp_rx,udp_late,"
                   "udp_reorder_pct,udp_seq_lost,local_drops,errors\n");
    fprintf(f, "%s,%s,%d,%d,%.0f,%.1f,%d,%lu,%lu,%lu,%.4f,%lu,%lu,%.4f,%lu,"
               "%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%lu,%lu,%.4f,%lu,%lu,%lu\n",
            opt.label, opt.udp ? "udp" : "tcp", opt.clients, opt.rooms, opt.rate,
            opt.duration, opt.body, st.sent, st.expected, st.received, delivery,
            lost, st.reordered, reorder, st.dups, pct(50), pct(95), pct(99),
            st.nlat ? st.lat[st.nlat - 1] / 1e3 : NAN, mean, st.sent / run_s,
            st.received / run_s, st.udp_rx, st.udp_late, udp_reorder,
            udp_seq_lost, st.local_drops, st.errors);
    fclose(f);
}

int main(int argc, char **argv)
{
    int o;
    while ((o = getopt(argc, argv, "P:h:p:c:r:R:d:s:w:o:l:S:")) != -1) {
        switch (o) {
        case 'P': opt.udp = !strcmp(optarg, "udp"); break;
        case 'h': opt.host = optarg; break;
        case 'p': opt.port = atoi(optarg); break;
        case 'c': opt.clients = atoi(optarg); break;
        case 'r': opt.rooms = atoi(optarg); break;
        case 'R': opt.rate = atof(optarg); break;
        case 'd': opt.duration = atof(optarg); break;
        case 's': opt.body = atoi(optarg); break;
        case 'w': opt.drain = atof(optarg); break;
        case 'o': opt.csv = optarg; break;
        case 'l': opt.label = optarg; break;
        case 'S': opt.after_setup = optarg; break;
        default:
            fprintf(stderr, "see the comment at the top of loadtest.c for usage\n");
            return 2;
        }
    }
    if (opt.clients < 2 || opt.rooms < 1 || opt.rooms > opt.clients ||
        opt.body < 0 || opt.body > MAX_BODY || opt.rate <= 0) {
        fprintf(stderr, "invalid options\n");
        return 2;
    }
    signal(SIGPIPE, SIG_IGN);
    raise_fd_limit();
    snprintf(tag, sizeof tag, "t%d", (int)getpid());
    server.sin_family = AF_INET;
    server.sin_port = htons(opt.port);
    if (inet_pton(AF_INET, opt.host, &server.sin_addr) != 1) {
        fprintf(stderr, "bad host %s\n", opt.host);
        return 2;
    }
    if ((epfd = epoll_create1(0)) < 0)
        die("epoll_create1");

    cl = calloc(opt.clients, sizeof *cl);
    room_size = calloc(opt.rooms, sizeof *room_size);
    int per_room = (opt.clients + opt.rooms - 1) / opt.rooms;
    for (int i = 0; i < opt.clients; i++) {
        cl[i].id = i;
        cl[i].room = i % opt.rooms;
        cl[i].last_from = calloc(per_room, sizeof(uint64_t));
        room_size[cl[i].room]++;
    }

    /* Phase 1: connect, register and join. UDP retransmits HELLO+JOIN until
     * the LIST acknowledgement arrives, since either may be lost. */
    uint64_t t_setup = now_ns(), last_retry = t_setup;
    for (int i = 0; i < opt.clients; i++) {
        connect_client(&cl[i]);
        send_hello_join(&cl[i]);
        if (i % 64 == 63)
            poll_events(0);     /* don't let replies pile up during connect */
    }
    while (st.joined < (uint64_t)opt.clients) {
        poll_events(20);
        uint64_t now = now_ns();
        if (now - t_setup > 20000000000ull) {
            fprintf(stderr, "setup timed out: %lu/%d joined\n", st.joined, opt.clients);
            return 1;
        }
        if (opt.udp && now - last_retry > 300000000ull) {
            for (int i = 0; i < opt.clients; i++)
                if (!cl[i].joined)
                    send_hello_join(&cl[i]);
            last_retry = now;
        }
    }
    fprintf(stderr, "setup: %d clients joined %d rooms in %.0f ms\n", opt.clients,
            opt.rooms, (now_ns() - t_setup) / 1e6);

    /* Let join presence notifications settle before measuring. */
    for (uint64_t t = now_ns(); now_ns() - t < 300000000ull;)
        poll_events(10);

    /* Hook for the experiment script, e.g. to switch on tc netem only once
     * everyone is connected, so we measure chat traffic under loss rather
     * than 1-second SYN retransmission timeouts during connect(). */
    if (opt.after_setup && system(opt.after_setup) != 0) {
        fprintf(stderr, "after-setup command failed: %s\n", opt.after_setup);
        return 1;
    }

    /* Phase 2: send at a fixed total rate from randomly chosen clients, so
     * arrivals per room are irregular (Poisson-like) as in real chat rather
     * than perfectly evenly spaced. Fixed seed: runs are repeatable. */
    uint64_t rng = 0x9e3779b97f4a7c15ull;
    measuring = 1;
    uint64_t t0 = now_ns(), dur = (uint64_t)(opt.duration * 1e9), last_ping = t0;
    for (;;) {
        uint64_t now = now_ns(), el = now - t0;
        if (el >= dur)
            break;
        uint64_t target = (uint64_t)(opt.rate * el / 1e9);
        while (st.sent + st.local_drops < target) {
            rng ^= rng << 13;   /* xorshift64 */
            rng ^= rng >> 7;
            rng ^= rng << 17;
            send_chat(&cl[rng % opt.clients]);
        }
        if (opt.udp && now - last_ping > 1000000000ull) {
            for (int i = 0; i < opt.clients; i++)
                send_ping(&cl[i]);      /* keeps server-side presence alive */
            last_ping = now;
        }
        poll_events(1);
    }
    double run_s = (now_ns() - t0) / 1e9;

    /* Phase 3: wait for in-flight messages (TCP retransmissions can take a
     * while under loss), up to the drain timeout. */
    uint64_t td = now_ns(), drain = (uint64_t)(opt.drain * 1e9);
    while (st.received < st.expected && now_ns() - td < drain)
        poll_events(10);

    report(run_s);

    for (int i = 0; i < opt.clients; i++) {
        if (opt.udp) {
            wr_t w;
            wr_init(&w, NULL, 0);
            send_msg(&cl[i], MSG_BYE, &w);
        }
        close(cl[i].fd);
    }
    return 0;
}
