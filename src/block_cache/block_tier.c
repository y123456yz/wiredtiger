/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_blkcache_tiered_open --
 *     Open a tiered object.
 */
int
__wt_blkcache_tiered_open(
  WT_SESSION_IMPL *session, const char *uri, uint32_t objectid, WT_BLOCK **blockp)
{
    WT_BLOCK *block;
    WT_BUCKET_STORAGE *bstorage;
    WT_CONFIG_ITEM pfx;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_TIERED *tiered;
    const char *cfg[2], *object_name, *object_uri, *object_val;
    bool exist, local_only, readonly;

    *blockp = NULL;

    tiered = (WT_TIERED *)session->dhandle;
    object_uri = object_val = NULL;

    WT_ASSERT(session, objectid <= tiered->current_id);
    WT_ASSERT(session, uri == NULL || WT_PREFIX_MATCH(uri, "tiered:"));
    WT_ASSERT(session, (uri == NULL && objectid != 0) || (uri != NULL && objectid == 0));

    /*
     * First look for the local file. This will be the fastest access and we retain recent objects
     * in the local database for awhile. If we're passed a name to open, then by definition it's a
     * local file.
     *
     * FIXME-WT-7590 we will need some kind of locking while we're looking at the tiered structure.
     * This can be called at any time, because we are opening the objects lazily.
     */
    if (uri != NULL)
        objectid = tiered->current_id;
    if (objectid == tiered->current_id) {
        local_only = true;
        object_uri = tiered->tiers[WT_TIERED_INDEX_LOCAL].name;
        object_name = object_uri;
        WT_PREFIX_SKIP_REQUIRED(session, object_name, "file:");
        readonly = false;
    } else {
        local_only = false;
        WT_ERR(
          __wt_tiered_name(session, &tiered->iface, objectid, WT_TIERED_NAME_OBJECT, &object_uri));
        object_name = object_uri;
        WT_PREFIX_SKIP_REQUIRED(session, object_name, "object:");
        readonly = true;
    }

    /* Get the object's configuration. */
    WT_ERR(__wt_metadata_search(session, object_uri, (char **)&object_val));
    cfg[0] = object_val;
    cfg[1] = NULL;

    /* Check if the object exists. */
    exist = true;
    if (!local_only)
        WT_ERR(__wt_fs_exist(session, object_name, &exist));
    if (exist)
        WT_ERR(
          __wt_block_open(session, object_name, objectid, cfg, false, readonly, false, 0, &block));
    else {
        /* We expect a prefix. */
        WT_ERR(__wt_config_gets(session, cfg, "tiered_storage.bucket_prefix", &pfx));
        WT_ASSERT(session, pfx.len != 0);

        WT_ERR(__wt_scr_alloc(session, 0, &tmp));
        WT_ERR(__wt_buf_fmt(session, tmp, "%.*s%s", (int)pfx.len, pfx.str, object_name));

        bstorage = tiered->bstorage;
        WT_WITH_BUCKET_STORAGE(bstorage, session,
          ret = __wt_block_open(session, tmp->mem, objectid, cfg, false, true, true, 0, &block));
        WT_ERR(ret);
    }

    *blockp = block;

err:
    if (!local_only)
        __wt_free(session, object_uri);
    __wt_free(session, object_val);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_blkcache_get_handle --
 *     Get a cached block handle for an object, creating it if it doesn't exist.
 */
int
__wt_blkcache_get_handle(WT_SESSION_IMPL *session, WT_BM *bm, uint32_t objectid, WT_BLOCK **blockp)
{
    WT_DECL_RET;
    u_int i;

    *blockp = NULL;

    /* We should never be looking for current active file. */
    WT_ASSERT(session, bm->block->objectid != objectid);

    /*
     * Check the block handle array for the object. We don't have to check the name because we can
     * only reference objects in our name space.
     */
    __wt_readlock(session, &bm->handle_array_lock);
    for (i = 0; i < bm->handle_array_next; ++i)
        if (bm->handle_array[i]->objectid == objectid) {
            *blockp = bm->handle_array[i];
            __wt_readunlock(session, &bm->handle_array_lock);
            return (0);
        }

    /* We need to add a new handle the block handle array. Upgrade to a write lock. */
    __wt_readunlock(session, &bm->handle_array_lock);
    __wt_writelock(session, &bm->handle_array_lock);

    /* Check to make sure the object wasn't cached while we locked. */
    for (i = 0; i < bm->handle_array_next; ++i)
        if (bm->handle_array[i]->objectid == objectid) {
            *blockp = bm->handle_array[i];
            __wt_writeunlock(session, &bm->handle_array_lock);
            return (0);
        }

    /* Allocate space to store a new handle (do first for less complicated cleanup). */
    WT_ERR(__wt_realloc_def(
      session, &bm->handle_array_allocated, bm->handle_array_next + 1, &bm->handle_array));

    /* Open the object */
    WT_ERR(__wt_blkcache_tiered_open(session, NULL, objectid, blockp));

    /* Add object to block handle array. */
    bm->handle_array[bm->handle_array_next++] = *blockp;

err:
    __wt_writeunlock(session, &bm->handle_array_lock);
    return (ret);
}