/*
 * server_tcp.c - single-threaded, event-driven TCP chat server.
 *
 * One epoll loop multiplexes the listening socket and every client socket.
 * All sockets are non-blocking, so no call can stall the loop:
 *
 *   - accept:  drain accept4() until EAGAIN (several clients may be queued)
 *   - read:    one recv() per readiness event into the client's input buffer,
 *              then parse as many complete frames as the buffer holds
 *              (handles partial reads and several frames merged in one read)
 *   - write:   outgoing frames are appended to the client's output queue and
 *              the client is marked dirty; after the whole event batch has
 *              been processed each dirty client is flushed with ONE send().
 *              A busy room fan-out thus costs one syscall per recipient per
 *              loop iteration instead of one per message (write coalescing).
 *              Whatever the kernel doesn't take stays queued and EPOLLOUT is
 *              enabled until the queue drains (backpressure). A client whose
 *              queue exceeds MAX_OUTBUF is disconnected so one slow reader
 *              can't exhaust server memory.
 *
 * epoll is used level-triggered: a socket with unread data keeps reporting
 * readiness, so reading a bounded chunk per event is safe and keeps one busy
 * client from starving the others.
 *
 * Closing is deferred to the end of each event batch, because a close can be
 * triggered while broadcasting to a room (e.g. a member's queue overflows),
 * and freeing a client in the middle of iterating that room would be unsafe.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "chat.h"
#include "protocol.h"

#define MAX_EVENTS  1024
#define READ_CHUNK  65536
#define MAX_OUTBUF  (4 * 1024 * 1024)

typedef struct {
    client_t c;                 /* must be first: chat core hands us client_t* */
    int fd;
    buf_t in, out;
    int want_out;               /* EPOLLOUT currently registered */
    int dirty;                  /* queued for flush at end of batch */
    int closing;                /* queued for close at end of batch */
    char addr[48];
} conn_t;

static int epfd = -1;
static conn_t **conns;          /* indexed by fd */
static size_t nconns_cap;
typedef struct {
    conn_t **items;
    size_t n, cap;
} conn_list;

static conn_list close_list, dirty_list;
static chat_t chat;
static volatile sig_atomic_t stop;

static struct {
    uint64_t accepted, active, peak, bytes_in, bytes_out, slow_kicks, bad_frames;
} stats;

static void on_signal(int sig) { (void)sig; stop = 1; }

static void update_events(conn_t *c, int want_out)
{
    if (c->want_out == want_out)
        return;
    struct epoll_event ev = {
        .events = EPOLLIN | EPOLLRDHUP | (want_out ? EPOLLOUT : 0),
        .data.fd = c->fd,
    };
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev) == 0)
        c->want_out = want_out;
}

static void list_push(conn_list *l, conn_t *c)
{
    if (l->n == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 64;
        l->items = realloc(l->items, l->cap * sizeof *l->items);
        if (!l->items) {
            perror("realloc");
            exit(1);
        }
    }
    l->items[l->n++] = c;
}

static void schedule_close(conn_t *c)
{
    if (c->closing)
        return;
    c->closing = 1;
    list_push(&close_list, c);
}

/* Sends as much of the queued output as the socket accepts. */
static void flush_out(conn_t *c)
{
    while (buf_len(&c->out) > 0) {
        ssize_t n = send(c->fd, buf_head(&c->out), buf_len(&c->out), MSG_NOSIGNAL);
        if (n > 0) {
            stats.bytes_out += n;
            buf_consume(&c->out, n);
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            schedule_close(c);
            return;
        }
    }
    update_events(c, buf_len(&c->out) > 0);
}

/* chat core -> wire: frame the message and queue/send it. */
static void tcp_send(chat_t *ch, client_t *to, uint8_t type,
                     const uint8_t *payload, size_t len)
{
    (void)ch;
    static uint8_t frame[FRAME_HDR + MAX_FRAME];
    conn_t *c = (conn_t *)to;
    if (c->closing)
        return;
    size_t n = frame_encode(frame, sizeof frame, type, payload, len);
    if (n == 0)
        return;
    if (buf_len(&c->out) + n > MAX_OUTBUF) {
        fprintf(stderr, "kick %s (%s): output queue full, client too slow\n",
                c->c.nick[0] ? c->c.nick : "?", c->addr);
        stats.slow_kicks++;
        schedule_close(c);
        return;
    }
    if (buf_append(&c->out, frame, n) < 0) {
        schedule_close(c);
        return;
    }
    /* If EPOLLOUT is armed the socket is full and the event will flush it;
     * otherwise flush once at the end of this event batch. */
    if (!c->want_out && !c->dirty) {
        c->dirty = 1;
        list_push(&dirty_list, c);
    }
}

static void process_flushes(void)
{
    for (size_t i = 0; i < dirty_list.n; i++) {
        conn_t *c = dirty_list.items[i];
        c->dirty = 0;
        if (!c->closing)
            flush_out(c);       /* may schedule a close on send error */
    }
    dirty_list.n = 0;
}

static void handle_accept(int lfd)
{
    for (;;) {
        struct sockaddr_in sa;
        socklen_t sl = sizeof sa;
        int fd = accept4(lfd, (struct sockaddr *)&sa, &sl,
                         SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                perror("accept4");   /* e.g. EMFILE: retry on next event */
            return;
        }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

        if ((size_t)fd >= nconns_cap) {
            size_t cap = nconns_cap ? nconns_cap : 1024;
            while (cap <= (size_t)fd)
                cap *= 2;
            conn_t **p = realloc(conns, cap * sizeof *p);
            if (!p) {
                close(fd);
                continue;
            }
            memset(p + nconns_cap, 0, (cap - nconns_cap) * sizeof *p);
            conns = p;
            nconns_cap = cap;
        }
        conn_t *c = calloc(1, sizeof *c);
        if (!c) {
            close(fd);
            continue;
        }
        c->fd = fd;
        snprintf(c->addr, sizeof c->addr, "%s:%u",
                 inet_ntoa(sa.sin_addr), ntohs(sa.sin_port));
        struct epoll_event ev = { .events = EPOLLIN | EPOLLRDHUP, .data.fd = fd };
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("epoll_ctl");
            close(fd);
            free(c);
            continue;
        }
        conns[fd] = c;
        stats.accepted++;
        if (++stats.active > stats.peak)
            stats.peak = stats.active;
    }
}

static void handle_read(conn_t *c)
{
    if (buf_reserve(&c->in, READ_CHUNK) < 0) {
        schedule_close(c);
        return;
    }
    ssize_t n = recv(c->fd, c->in.data + c->in.end, READ_CHUNK, 0);
    if (n == 0) {
        schedule_close(c);          /* orderly shutdown by peer */
        return;
    }
    if (n < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            schedule_close(c);
        return;
    }
    c->in.end += n;
    stats.bytes_in += n;

    /* Drain every complete frame; a trailing partial frame stays buffered
     * until more bytes arrive. */
    for (;;) {
        uint8_t type;
        const uint8_t *payload;
        uint32_t plen;
        size_t used;
        int r = frame_parse(buf_head(&c->in), buf_len(&c->in),
                            &type, &payload, &plen, &used);
        if (r == 0)
            break;
        if (r < 0) {
            stats.bad_frames++;
            chat_error(&chat, &c->c, "bad frame length");
            flush_out(c);
            schedule_close(c);
            return;
        }
        if (type == MSG_BYE) {
            schedule_close(c);
            return;
        }
        chat_handle(&chat, &c->c, type, payload, plen);
        buf_consume(&c->in, used);
        if (c->closing)
            return;
    }
}

static void process_closes(void)
{
    /* chat_disconnect broadcasts presence, which can schedule more closes,
     * so the list may grow while we walk it. */
    for (size_t i = 0; i < close_list.n; i++) {
        conn_t *c = close_list.items[i];
        chat_disconnect(&chat, &c->c);
        if (c->dirty) {
            /* Rare: queued for flush and then kicked in the same batch. */
            for (size_t j = 0; j < dirty_list.n; j++)
                if (dirty_list.items[j] == c)
                    dirty_list.items[j] = dirty_list.items[--dirty_list.n];
        }
        epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
        close(c->fd);
        conns[c->fd] = NULL;
        buf_free(&c->in);
        buf_free(&c->out);
        free(c);
        stats.active--;
    }
    close_list.n = 0;
}

static int listen_on(const char *host, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in sa = { .sin_family = AF_INET, .sin_port = htons(port) };
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) {
        fprintf(stderr, "bad bind address: %s\n", host);
        return -1;
    }
    if (bind(fd, (struct sockaddr *)&sa, sizeof sa) < 0 ||
        listen(fd, SOMAXCONN) < 0) {
        perror("bind/listen");
        return -1;
    }
    return fd;
}

int main(int argc, char **argv)
{
    const char *host = "0.0.0.0";
    int port = 9000, opt;
    while ((opt = getopt(argc, argv, "b:p:h")) != -1) {
        switch (opt) {
        case 'b': host = optarg; break;
        case 'p': port = atoi(optarg); break;
        default:
            fprintf(stderr, "usage: %s [-b bind_addr] [-p port]\n", argv[0]);
            return opt == 'h' ? 0 : 2;
        }
    }

    struct sigaction sa = { .sa_handler = on_signal };   /* no SA_RESTART */
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    if (chat_init(&chat, tcp_send, 0) < 0)
        return 1;
    int lfd = listen_on(host, port);
    if (lfd < 0)
        return 1;
    epfd = epoll_create1(EPOLL_CLOEXEC);
    struct epoll_event lev = { .events = EPOLLIN, .data.fd = lfd };
    if (epfd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &lev) < 0) {
        perror("epoll");
        return 1;
    }
    fprintf(stderr, "tcp chat server listening on %s:%d\n", host, port);

    struct epoll_event events[MAX_EVENTS];
    while (!stop) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;
            if (fd == lfd) {
                handle_accept(lfd);
                continue;
            }
            conn_t *c = (size_t)fd < nconns_cap ? conns[fd] : NULL;
            if (!c || c->closing)
                continue;
            if (ev & (EPOLLERR | EPOLLHUP)) {
                schedule_close(c);
                continue;
            }
            if (ev & EPOLLOUT)
                flush_out(c);
            /* EPOLLRDHUP: peer closed its side; reading returns the
             * remaining data and then 0, which closes us. */
            if (!c->closing && (ev & (EPOLLIN | EPOLLRDHUP)))
                handle_read(c);
        }
        /* Closing broadcasts presence (more flushes) and flushing can fail
         * (more closes), so repeat until both lists are empty. */
        while (dirty_list.n || close_list.n) {
            process_flushes();
            process_closes();
        }
    }

    fprintf(stderr,
            "\nshutdown: accepted=%lu peak_clients=%lu msgs_in=%lu msgs_out=%lu "
            "bytes_in=%lu bytes_out=%lu slow_kicks=%lu bad_frames=%lu\n",
            stats.accepted, stats.peak, chat.msgs_in, chat.msgs_out,
            stats.bytes_in, stats.bytes_out, stats.slow_kicks, stats.bad_frames);
    for (size_t fd = 0; fd < nconns_cap; fd++)
        if (conns[fd])
            schedule_close(conns[fd]);
    process_closes();
    close(lfd);
    close(epfd);
    chat_free(&chat);
    free(conns);
    free(close_list.items);
    free(dirty_list.items);
    return 0;
}
