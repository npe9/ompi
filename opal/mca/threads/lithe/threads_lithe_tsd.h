/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H
#define OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H

#include "opal_config.h"
#include "opal/constants.h"
#include <lithe/lithe.h>
#include <parlib/uthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

BEGIN_C_DECLS

/* CRITICAL: TSD Implementation for Lithe using hash table
 * 
 * This implementation stores TSD data per-Lithe-context (uthread), not per-pthread.
 * Uses a hash table keyed by context pointer to store per-context TSD lists.
 * 
 * Implementation:
 * - Each TSD key is a simple integer identifier
 * - TSD values are stored in a linked list per context
 * - Hash table maps context pointers to their TSD lists
 */

// TSD key is just an integer (no pthread dependency)
typedef uintptr_t opal_tsd_key_t;

// TSD entry in the per-context linked list
struct opal_lithe_tsd_entry {
    opal_tsd_key_t key;
    void *value;
    void (*destructor)(void *);
    struct opal_lithe_tsd_entry *next;
};

// Hash table entry for context -> TSD list mapping
struct opal_lithe_tsd_context_entry {
    lithe_context_t *ctx;
    struct opal_lithe_tsd_entry *tsd_list;
    struct opal_lithe_tsd_context_entry *next;
};

// Hash table for context -> TSD list (simple chained hash table)
#define OPAL_LITHE_TSD_HASH_SIZE 256
static struct opal_lithe_tsd_context_entry *opal_lithe_tsd_hash_table[OPAL_LITHE_TSD_HASH_SIZE] = {0};

// Global registry of TSD keys (for tracking destructors)
struct opal_lithe_tsd_key_registry {
    opal_tsd_key_t key;
    void (*destructor)(void *);
};
static struct opal_lithe_tsd_key_registry *opal_lithe_tsd_keys = NULL;
static size_t opal_lithe_tsd_keys_size = 0;
static size_t opal_lithe_tsd_keys_capacity = 0;
static opal_tsd_key_t opal_lithe_tsd_next_key = 1;  // Start at 1, 0 is invalid

// Simple hash function for context pointer
static inline size_t opal_lithe_tsd_hash(lithe_context_t *ctx)
{
    return ((uintptr_t)ctx) % OPAL_LITHE_TSD_HASH_SIZE;
}

// Get the TSD list for a specific context (creates if needed)
static inline struct opal_lithe_tsd_entry **opal_lithe_get_tsd_list(lithe_context_t *ctx)
{
    if (ctx == NULL) {
        return NULL;
    }
    
    size_t hash = opal_lithe_tsd_hash(ctx);
    struct opal_lithe_tsd_context_entry *entry = opal_lithe_tsd_hash_table[hash];
    
    // Find existing entry
    while (entry != NULL) {
        if (entry->ctx == ctx) {
            return &entry->tsd_list;
        }
        entry = entry->next;
    }
    
    // Create new entry
    entry = malloc(sizeof(struct opal_lithe_tsd_context_entry));
    if (entry == NULL) {
        return NULL;
    }
    entry->ctx = ctx;
    entry->tsd_list = NULL;
    entry->next = opal_lithe_tsd_hash_table[hash];
    opal_lithe_tsd_hash_table[hash] = entry;
    
    return &entry->tsd_list;
}

// Helper to find a TSD entry in a list
static inline struct opal_lithe_tsd_entry *opal_lithe_find_entry(struct opal_lithe_tsd_entry *list, opal_tsd_key_t key)
{
    struct opal_lithe_tsd_entry *entry = list;
    while (entry != NULL) {
        if (entry->key == key) {
            return entry;
        }
        entry = entry->next;
    }
    return NULL;
}

// Create a new TSD key
static inline int opal_tsd_key_create_inline(opal_tsd_key_t *key, void (*destructor)(void *))
{
    if (key == NULL) {
        return OPAL_ERR_BAD_PARAM;
    }
    
    // Allocate a new key
    opal_tsd_key_t new_key = opal_lithe_tsd_next_key++;
    
    // Store in registry
    if (opal_lithe_tsd_keys_size >= opal_lithe_tsd_keys_capacity) {
        size_t new_capacity = opal_lithe_tsd_keys_capacity == 0 ? 16 : opal_lithe_tsd_keys_capacity * 2;
        struct opal_lithe_tsd_key_registry *new_keys = realloc(opal_lithe_tsd_keys, 
                                                               new_capacity * sizeof(*opal_lithe_tsd_keys));
        if (new_keys == NULL) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        opal_lithe_tsd_keys = new_keys;
        opal_lithe_tsd_keys_capacity = new_capacity;
    }
    opal_lithe_tsd_keys[opal_lithe_tsd_keys_size].key = new_key;
    opal_lithe_tsd_keys[opal_lithe_tsd_keys_size].destructor = destructor;
    opal_lithe_tsd_keys_size++;
    
    *key = new_key;
    return OPAL_SUCCESS;
}

// Real function implementation is in threads_lithe_module.c to avoid multiple definitions
// The inline version above can be used when this header is directly included

// Delete a TSD key (clean up all values across all contexts)
static inline int opal_tsd_key_delete(opal_tsd_key_t key)
{
    if (key == 0) {
        return OPAL_ERR_BAD_PARAM;
    }
    
    // Find and remove from registry
    for (size_t i = 0; i < opal_lithe_tsd_keys_size; i++) {
        if (opal_lithe_tsd_keys[i].key == key) {
            // Shift remaining entries
            for (size_t j = i; j < opal_lithe_tsd_keys_size - 1; j++) {
                opal_lithe_tsd_keys[j] = opal_lithe_tsd_keys[j + 1];
            }
            opal_lithe_tsd_keys_size--;
            break;
        }
    }
    
    // Note: We don't clean up values in existing contexts here
    // The destructor will be called when contexts are destroyed
    return OPAL_SUCCESS;
}

// Set TSD value for current context
static inline int opal_tsd_setspecific(opal_tsd_key_t key, const void *value)
{
    if (key == 0) {
        return OPAL_ERR_BAD_PARAM;
    }
    
    // Get current lithe context
    lithe_context_t *ctx = lithe_context_self();
    if (ctx == NULL) {
        // Not in a lithe context - can't set TSD
        return OPAL_ERR_NOT_AVAILABLE;
    }
    
    // Get the TSD list for this context
    struct opal_lithe_tsd_entry **tsd_list = opal_lithe_get_tsd_list(ctx);
    if (tsd_list == NULL) {
        // Failed to allocate hash table entry
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    
    // Find or create entry
    struct opal_lithe_tsd_entry *entry = opal_lithe_find_entry(*tsd_list, key);
    if (entry == NULL) {
        // Create new entry
        entry = malloc(sizeof(struct opal_lithe_tsd_entry));
        if (entry == NULL) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        entry->key = key;
        entry->next = *tsd_list;
        *tsd_list = entry;
        
        // Find destructor from registry
        entry->destructor = NULL;
        for (size_t i = 0; i < opal_lithe_tsd_keys_size; i++) {
            if (opal_lithe_tsd_keys[i].key == key) {
                entry->destructor = opal_lithe_tsd_keys[i].destructor;
                break;
            }
        }
    }
    
    entry->value = (void *)value;
    return OPAL_SUCCESS;
}

// Get TSD value for current context
static inline int opal_tsd_getspecific_inline(opal_tsd_key_t key, void **value)
{
    if (key == 0 || value == NULL) {
        return OPAL_ERR_BAD_PARAM;
    }
    
    // Get current lithe context
    lithe_context_t *ctx = lithe_context_self();
    if (ctx == NULL) {
        // Not in a lithe context
        *value = NULL;
        return OPAL_ERR_NOT_AVAILABLE;
    }
    
    // Get the TSD list for this context
    struct opal_lithe_tsd_entry **tsd_list = opal_lithe_get_tsd_list(ctx);
    if (tsd_list == NULL) {
        // Failed to allocate hash table entry (shouldn't happen, but handle gracefully)
        *value = NULL;
        return OPAL_SUCCESS;  // Not an error, just no value set
    }
    
    // Find entry
    struct opal_lithe_tsd_entry *entry = opal_lithe_find_entry(*tsd_list, key);
    if (entry != NULL) {
        *value = entry->value;
    } else {
        *value = NULL;
    }
    
    return OPAL_SUCCESS;
}

// Convenience wrappers - these must be defined early so tsd.h can use them
// Note: These are used by tsd.h in static inline functions like opal_tsd_tracked_key_get
static inline int opal_tsd_set(opal_tsd_key_t key, void *value)
{
    return opal_tsd_setspecific(key, value);
}

// Convenience wrapper - must be defined so tsd.h can use it
static inline int opal_tsd_get(opal_tsd_key_t key, void **valuep)
{
    return opal_tsd_getspecific_inline(key, valuep);
}

// Provide the static inline function that tsd.h expects
// This satisfies the forward declaration in tsd.h line 114
// Only define it if we're not compiling tsd.c (which provides the non-static version)
#ifndef OPAL_TSD_C_COMPILING
static inline int opal_tsd_getspecific(opal_tsd_key_t key, void **valuep)
{
    return opal_tsd_getspecific_inline(key, valuep);
}
#endif

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H */
