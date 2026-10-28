/*
 * hashmap.h - small string-keyed hash map (separate chaining, FNV-1a).
 * Used for nickname -> client, room name -> room and, in the UDP server,
 * "ip:port" -> client. Keys are copied; values are borrowed pointers.
 */
#ifndef HASHMAP_H
#define HASHMAP_H

#include <stddef.h>

typedef struct hm_entry {
    char *key;
    void *val;
    struct hm_entry *next;
} hm_entry;

typedef struct {
    hm_entry **buckets;
    size_t nbuckets, count;
} hmap_t;

int hm_init(hmap_t *h);
void *hm_get(const hmap_t *h, const char *key);
int hm_put(hmap_t *h, const char *key, void *val);  /* insert or replace */
void *hm_del(hmap_t *h, const char *key);           /* returns old value */
void hm_free(hmap_t *h);

#endif
