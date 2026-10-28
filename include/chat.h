/*
 * chat.h - transport-independent chat logic: nicknames, rooms, private
 * messages and presence. The TCP and UDP servers both drive this core; each
 * supplies a send callback that knows how to put a message on its wire.
 *
 * Transport connection structs embed client_t as their FIRST member, so the
 * send callback can cast a client_t* back to its own connection type.
 */
#ifndef CHAT_H
#define CHAT_H

#include <stddef.h>
#include <stdint.h>

#include "hashmap.h"
#include "protocol.h"

#define MAX_ROOMS_PER_CLIENT 16

typedef struct room room_t;

typedef struct client {
    char nick[NAME_MAX_LEN + 1];
    int registered;                         /* HELLO accepted */
    room_t *rooms[MAX_ROOMS_PER_CLIENT];
    int nrooms;
} client_t;

struct room {
    char name[NAME_MAX_LEN + 1];
    client_t **members;
    int nmembers, cap;
};

typedef struct chat chat_t;
typedef void (*chat_send_fn)(chat_t *chat, client_t *to, uint8_t type,
                             const uint8_t *payload, size_t len);

struct chat {
    hmap_t nicks;               /* nick -> client_t* */
    hmap_t rooms;               /* name -> room_t*   */
    chat_send_fn send;
    /* UDP clients retransmit JOIN when the ack is lost, so a repeated JOIN
     * must be answered (re-send LIST) instead of rejected. */
    int join_idempotent;
    uint64_t msgs_in, msgs_out;
};

int chat_init(chat_t *chat, chat_send_fn send, int join_idempotent);
void chat_free(chat_t *chat);

/* Handles one decoded message from a client. */
void chat_handle(chat_t *chat, client_t *c, uint8_t type,
                 const uint8_t *payload, size_t len);

/* Removes the client from every room (notifying members) and frees its
 * nickname. The transport frees the connection afterwards. */
void chat_disconnect(chat_t *chat, client_t *c);

/* Sends an ERROR message to one client. */
void chat_error(chat_t *chat, client_t *c, const char *text);

#endif
