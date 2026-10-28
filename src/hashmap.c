#include "hashmap.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static uint64_t fnv1a(const char *s)
{
    uint64_t h = 1469598103934665603ull;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211ull;
    }
    return h;
}

int hm_init(hmap_t *h)
{
    h->nbuckets = 64;
    h->count = 0;
    h->buckets = calloc(h->nbuckets, sizeof *h->buckets);
    return h->buckets ? 0 : -1;
}

static void hm_grow(hmap_t *h)
{
    size_t nb = h->nbuckets * 2;
    hm_entry **b = calloc(nb, sizeof *b);
    if (!b)
        return;                 /* keep working with longer chains */
    for (size_t i = 0; i < h->nbuckets; i++) {
        hm_entry *e = h->buckets[i];
        while (e) {
            hm_entry *next = e->next;
            size_t j = fnv1a(e->key) & (nb - 1);
            e->next = b[j];
            b[j] = e;
            e = next;
        }
    }
    free(h->buckets);
    h->buckets = b;
    h->nbuckets = nb;
}

void *hm_get(const hmap_t *h, const char *key)
{
    for (hm_entry *e = h->buckets[fnv1a(key) & (h->nbuckets - 1)]; e; e = e->next)
        if (strcmp(e->key, key) == 0)
            return e->val;
    return NULL;
}

int hm_put(hmap_t *h, const char *key, void *val)
{
    size_t i = fnv1a(key) & (h->nbuckets - 1);
    for (hm_entry *e = h->buckets[i]; e; e = e->next) {
        if (strcmp(e->key, key) == 0) {
            e->val = val;
            return 0;
        }
    }
    hm_entry *e = malloc(sizeof *e);
    if (!e || !(e->key = strdup(key))) {
        free(e);
        return -1;
    }
    e->val = val;
    e->next = h->buckets[i];
    h->buckets[i] = e;
    if (++h->count > h->nbuckets * 3 / 4)
        hm_grow(h);
    return 0;
}

void *hm_del(hmap_t *h, const char *key)
{
    hm_entry **pp = &h->buckets[fnv1a(key) & (h->nbuckets - 1)];
    for (; *pp; pp = &(*pp)->next) {
        hm_entry *e = *pp;
        if (strcmp(e->key, key) == 0) {
            void *val = e->val;
            *pp = e->next;
            free(e->key);
            free(e);
            h->count--;
            return val;
        }
    }
    return NULL;
}

void hm_free(hmap_t *h)
{
    for (size_t i = 0; i < h->nbuckets; i++) {
        hm_entry *e = h->buckets[i];
        while (e) {
            hm_entry *next = e->next;
            free(e->key);
            free(e);
            e = next;
        }
    }
    free(h->buckets);
    h->buckets = NULL;
    h->nbuckets = h->count = 0;
}
