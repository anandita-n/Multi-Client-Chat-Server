#include "chat.h"

#include <stdlib.h>
#include <string.h>

/* Large enough for the biggest message the core builds (a LIST of a full
 * room is capped by MAX_FRAME too). Single-threaded, so one is enough. */
static uint8_t out_buf[MAX_FRAME];

int chat_init(chat_t *chat, chat_send_fn send, int join_idempotent)
{
    memset(chat, 0, sizeof *chat);
    chat->send = send;
    chat->join_idempotent = join_idempotent;
    if (hm_init(&chat->nicks) < 0 || hm_init(&chat->rooms) < 0)
        return -1;
    return 0;
}

void chat_free(chat_t *chat)
{
    hm_free(&chat->nicks);
    hm_free(&chat->rooms);
}

static void send_msg(chat_t *chat, client_t *to, uint8_t type, const wr_t *w)
{
    chat->msgs_out++;
    chat->send(chat, to, type, w->buf, w->len);
}

void chat_error(chat_t *chat, client_t *c, const char *text)
{
    wr_t w;
    wr_init(&w, out_buf, sizeof out_buf);
    wr_str(&w, text);
    send_msg(chat, c, MSG_ERROR, &w);
}

/* ---- rooms ---- */

static int client_in_room(const client_t *c, const room_t *r)
{
    for (int i = 0; i < c->nrooms; i++)
        if (c->rooms[i] == r)
            return 1;
    return 0;
}

static room_t *room_get_or_create(chat_t *chat, const char *name)
{
    room_t *r = hm_get(&chat->rooms, name);
    if (r)
        return r;
    r = calloc(1, sizeof *r);
    if (!r)
        return NULL;
    strcpy(r->name, name);
    if (hm_put(&chat->rooms, name, r) < 0) {
        free(r);
        return NULL;
    }
    return r;
}

static int room_add(room_t *r, client_t *c)
{
    if (r->nmembers == r->cap) {
        int cap = r->cap ? r->cap * 2 : 8;
        client_t **m = realloc(r->members, cap * sizeof *m);
        if (!m)
            return -1;
        r->members = m;
        r->cap = cap;
    }
    r->members[r->nmembers++] = c;
    c->rooms[c->nrooms++] = r;
    return 0;
}

static void room_remove(chat_t *chat, room_t *r, client_t *c)
{
    /* Order inside a room doesn't matter, so swap-remove in O(1) after the
     * O(n) search. */
    for (int i = 0; i < r->nmembers; i++) {
        if (r->members[i] == c) {
            r->members[i] = r->members[--r->nmembers];
            break;
        }
    }
    for (int i = 0; i < c->nrooms; i++) {
        if (c->rooms[i] == r) {
            c->rooms[i] = c->rooms[--c->nrooms];
            break;
        }
    }
    if (r->nmembers == 0) {
        hm_del(&chat->rooms, r->name);
        free(r->members);
        free(r);
    }
}

/* Sends one prepared message to every room member except `skip`. */
static void room_broadcast(chat_t *chat, room_t *r, client_t *skip,
                           uint8_t type, const wr_t *w)
{
    for (int i = 0; i < r->nmembers; i++)
        if (r->members[i] != skip)
            send_msg(chat, r->members[i], type, w);
}

static void broadcast_presence(chat_t *chat, room_t *r, client_t *c,
                               uint8_t event)
{
    wr_t w;
    wr_init(&w, out_buf, sizeof out_buf);
    wr_u8(&w, event);
    wr_str(&w, r->name);
    wr_str(&w, c->nick);
    room_broadcast(chat, r, c, MSG_PRESENCE, &w);
}

static void send_list(chat_t *chat, client_t *c, room_t *r)
{
    wr_t w;
    wr_init(&w, out_buf, sizeof out_buf);
    wr_str(&w, r->name);
    /* 32 bytes per nick at most, so cap the count to what fits. */
    int count = r->nmembers;
    int fit = (int)((sizeof out_buf - 64) / (NAME_MAX_LEN + 1));
    if (count > fit)
        count = fit;
    wr_u16(&w, (uint16_t)count);
    for (int i = 0; i < count; i++)
        wr_str(&w, r->members[i]->nick);
    send_msg(chat, c, MSG_LIST, &w);
}

/* ---- handlers ---- */

static void on_hello(chat_t *chat, client_t *c, rd_t *rd)
{
    char nick[NAME_MAX_LEN + 1];
    if (rd_str(rd, nick, sizeof nick) < 0 || !valid_name(nick)) {
        chat_error(chat, c, "invalid nickname");
        return;
    }
    if (c->registered) {
        /* A repeated HELLO with the same name is a retransmit; answer it. */
        if (strcmp(c->nick, nick) != 0) {
            chat_error(chat, c, "already registered");
            return;
        }
    } else {
        if (hm_get(&chat->nicks, nick)) {
            chat_error(chat, c, "nickname taken");
            return;
        }
        if (hm_put(&chat->nicks, nick, c) < 0) {
            chat_error(chat, c, "server out of memory");
            return;
        }
        strcpy(c->nick, nick);
        c->registered = 1;
    }
    wr_t w;
    wr_init(&w, out_buf, sizeof out_buf);
    wr_str(&w, c->nick);
    send_msg(chat, c, MSG_WELCOME, &w);
}

static void on_join(chat_t *chat, client_t *c, rd_t *rd)
{
    char name[NAME_MAX_LEN + 1];
    if (rd_str(rd, name, sizeof name) < 0 || !valid_name(name)) {
        chat_error(chat, c, "invalid room name");
        return;
    }
    room_t *r = hm_get(&chat->rooms, name);
    if (r && client_in_room(c, r)) {
        if (chat->join_idempotent)
            send_list(chat, c, r);
        else
            chat_error(chat, c, "already in room");
        return;
    }
    if (c->nrooms == MAX_ROOMS_PER_CLIENT) {
        chat_error(chat, c, "too many rooms");
        return;
    }
    r = room_get_or_create(chat, name);
    if (!r || room_add(r, c) < 0) {
        chat_error(chat, c, "server out of memory");
        return;
    }
    broadcast_presence(chat, r, c, PRESENCE_JOINED);
    send_list(chat, c, r);      /* doubles as the join acknowledgement */
}

static void on_leave(chat_t *chat, client_t *c, rd_t *rd)
{
    char name[NAME_MAX_LEN + 1];
    room_t *r = NULL;
    if (rd_str(rd, name, sizeof name) == 0)
        r = hm_get(&chat->rooms, name);
    if (!r || !client_in_room(c, r)) {
        chat_error(chat, c, "not in room");
        return;
    }
    broadcast_presence(chat, r, c, PRESENCE_LEFT);
    room_remove(chat, r, c);
}

static void on_room_msg(chat_t *chat, client_t *c, rd_t *rd)
{
    char name[NAME_MAX_LEN + 1];
    rd_str(rd, name, sizeof name);
    uint64_t id = rd_u64(rd), ts = rd_u64(rd);
    size_t blen;
    const uint8_t *body = rd_rest(rd, &blen);
    if (rd->err || blen > MAX_BODY) {
        chat_error(chat, c, "malformed room message");
        return;
    }
    room_t *r = hm_get(&chat->rooms, name);
    if (!r || !client_in_room(c, r)) {
        chat_error(chat, c, "not in room");
        return;
    }
    /* Encode once, fan out to every member. */
    wr_t w;
    wr_init(&w, out_buf, sizeof out_buf);
    wr_str(&w, r->name);
    wr_str(&w, c->nick);
    wr_u64(&w, id);
    wr_u64(&w, ts);
    wr_bytes(&w, body, blen);
    room_broadcast(chat, r, c, MSG_ROOM, &w);
}

static void on_private(chat_t *chat, client_t *c, rd_t *rd)
{
    char to[NAME_MAX_LEN + 1];
    rd_str(rd, to, sizeof to);
    uint64_t id = rd_u64(rd), ts = rd_u64(rd);
    size_t blen;
    const uint8_t *body = rd_rest(rd, &blen);
    if (rd->err || blen > MAX_BODY) {
        chat_error(chat, c, "malformed private message");
        return;
    }
    client_t *dst = hm_get(&chat->nicks, to);
    if (!dst) {
        chat_error(chat, c, "no such user");
        return;
    }
    wr_t w;
    wr_init(&w, out_buf, sizeof out_buf);
    wr_str(&w, c->nick);
    wr_u64(&w, id);
    wr_u64(&w, ts);
    wr_bytes(&w, body, blen);
    send_msg(chat, dst, MSG_PRIVATE, &w);
}

static void on_list(chat_t *chat, client_t *c, rd_t *rd)
{
    char name[NAME_MAX_LEN + 1];
    room_t *r = NULL;
    if (rd_str(rd, name, sizeof name) == 0)
        r = hm_get(&chat->rooms, name);
    if (!r) {
        chat_error(chat, c, "no such room");
        return;
    }
    send_list(chat, c, r);
}

static void on_ping(chat_t *chat, client_t *c, rd_t *rd)
{
    uint64_t ts = rd_u64(rd);
    wr_t w;
    wr_init(&w, out_buf, sizeof out_buf);
    wr_u64(&w, ts);
    send_msg(chat, c, MSG_PONG, &w);
}

void chat_handle(chat_t *chat, client_t *c, uint8_t type,
                 const uint8_t *payload, size_t len)
{
    rd_t rd;
    rd_init(&rd, payload, len);
    chat->msgs_in++;

    if (type == MSG_PING) {
        on_ping(chat, c, &rd);
        return;
    }
    if (type == MSG_HELLO) {
        on_hello(chat, c, &rd);
        return;
    }
    if (!c->registered) {
        chat_error(chat, c, "send HELLO first");
        return;
    }
    switch (type) {
    case MSG_JOIN:    on_join(chat, c, &rd); break;
    case MSG_LEAVE:   on_leave(chat, c, &rd); break;
    case MSG_ROOM:    on_room_msg(chat, c, &rd); break;
    case MSG_PRIVATE: on_private(chat, c, &rd); break;
    case MSG_LIST:    on_list(chat, c, &rd); break;
    default:          chat_error(chat, c, "unknown message type"); break;
    }
}

void chat_disconnect(chat_t *chat, client_t *c)
{
    while (c->nrooms > 0) {
        room_t *r = c->rooms[c->nrooms - 1];
        broadcast_presence(chat, r, c, PRESENCE_OFFLINE);
        room_remove(chat, r, c);
    }
    if (c->registered && hm_get(&chat->nicks, c->nick) == c)
        hm_del(&chat->nicks, c->nick);
    c->registered = 0;
}
