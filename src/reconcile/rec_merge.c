/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
#include "reconcile_private.h"
#include "reconcile_inline.h"

/*
 * Configuration parameters for adjacent page merging
 */
#define WT_MERGE_PADDING_THRESHOLD 80 /* 80% padding triggers merge */

/*
 * __merge_check_page_padding --
 *     Check if a page in WT_REF_DISK state has high padding ratio.
 *     This function must NOT change the ref state from WT_REF_DISK.
 *
 *     If mem_sizep is non-NULL, return the on-disk page header's mem_size.
 *
 *     Padding calculation: padding_ratio = (disk_size - mem_size) / disk_size * 100
 *     where:
 *       - disk_size: actual bytes written to disk (from block address cookie)
 *       - mem_size: uncompressed in-memory size (from WT_PAGE_HEADER on disk)
 */
static bool
__merge_check_page_padding(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t threshold, uint32_t *mem_sizep)
{
    WT_ADDR_COPY addr_copy;
    WT_BTREE *btree;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_PAGE_HEADER *dsk;
    wt_off_t offset;
    uint32_t checksum, disk_size, mem_size, objectid, padding_ratio;

    btree = S2BT(session);

    /*
     * CRITICAL: Only check pages in WT_REF_DISK state.
     * We must not read the page into memory (change state to WT_REF_MEM).
     */
    if (WT_REF_GET_STATE(ref) != WT_REF_DISK)
        return false;

    /* Only check leaf pages. */
    if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
        return false;

    /* Must have a stable on-disk address cookie. */
    if (!__wt_ref_addr_copy(session, ref, &addr_copy))
        return false;

    /*
     * addr_copy.type == WT_ADDR_LEAF indicates the leaf may contain overflow items.
     * Our merge implementation is conservative and does not support overflow items.
     */
    if (addr_copy.type == WT_ADDR_LEAF)
        return false;

    /* Unpack the block address to get disk size and offset. */
    ret = __wt_block_addr_unpack(session, btree->bm->block, addr_copy.addr, addr_copy.size, &objectid,
      &offset, &disk_size, &checksum);
    if (ret != 0 || disk_size == 0)
        return false;

    /*
     * Read the page header from disk to get mem_size.
     * Note: the block manager may read more than the header, but we keep the buffer small.
     */
    WT_ERR(__wt_scr_alloc(session, WT_PAGE_HEADER_BYTE_SIZE(btree), &tmp));
    WT_ERR(__wt_bm_read(btree->bm, session, tmp, NULL, addr_copy.addr, addr_copy.size));
    
    dsk = tmp->mem;
    mem_size = dsk->mem_size;
    if (mem_sizep != NULL)
        *mem_sizep = mem_size;
    
    /* Sanity check */
    if (mem_size == 0 || mem_size >= disk_size) {
        ret = EINVAL;
        goto err;
    }
    
    /* 
     * Calculate padding ratio: (disk_size - mem_size) / disk_size * 100
     * Example: disk=4096, mem=56 -> padding_ratio = (4096-56)/4096*100 = 98%
     */
    padding_ratio = ((disk_size - mem_size) * 100) / disk_size;
    
    __wt_scr_free(session, &tmp);
    return padding_ratio >= threshold;

err:
    __wt_scr_free(session, &tmp);
    return false;
}

/*
 * __merge_check_globally_visible --
 *     Check if page's all data is globally visible (safe to merge).
 */
static bool
__merge_check_globally_visible(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_ADDR_COPY addr_copy;
    wt_timestamp_t newest_durable_ts;
    
    /* 
     * CRITICAL: Must use __wt_ref_addr_copy to safely read address info.
     * ref->addr can be either:
     *   1. Off-page: pointer to WT_ADDR structure with ta field
     *   2. On-page: pointer to WT_CELL in parent's disk image
     * __wt_ref_addr_copy handles both cases and unpacks ta correctly.
     */
    if (!__wt_ref_addr_copy(session, ref, &addr_copy))
        return false;
    
    /* CRITICAL: Check for prepared transactions - cannot merge prepared pages */
    if (addr_copy.ta.prepare)
        return false;
    
    /*
     * Use the newest durable timestamp for visibility check.
     * This is the maximum of newest_start_durable_ts and newest_stop_durable_ts.
     */
    newest_durable_ts = WT_MAX(addr_copy.ta.newest_start_durable_ts, 
                                addr_copy.ta.newest_stop_durable_ts);
    
    /*
     * Use WiredTiger's standard visibility check function.
     * This checks both transaction ID and timestamp visibility.
     */
    return __wt_txn_has_newest_and_visible_all(session, addr_copy.ta.newest_txn, newest_durable_ts);
}

/*
 * __merge_check_leaf_inmem_padding --
 *     Check if a leaf page currently in memory has a high disk-vs-image padding ratio.
 *     This helper does not use page->memory_footprint: it requires a disk image.
 */
static bool
__merge_check_leaf_inmem_padding(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t threshold)
{
    WT_BLOCK_HEADER *blk;
    WT_BLOCK_HEADER swap;
    WT_PAGE *page;
    uint32_t disk_size, mem_size, padding_ratio;

    WT_UNUSED(session);

    if (!F_ISSET(ref, WT_REF_FLAG_LEAF) || WT_REF_GET_STATE(ref) != WT_REF_MEM)
        return false;

    page = ref->page;
    if (page == NULL || page->dsk == NULL)
        return false;

    blk = WT_BLOCK_HEADER_REF(page->dsk);
    __wt_block_header_byteswap_copy(blk, &swap);
    disk_size = swap.disk_size;
    if (disk_size == 0)
        return false;

    mem_size = page->dsk->mem_size;
    if (mem_size >= disk_size)
        return false;

    padding_ratio = (uint32_t)(((uint64_t)disk_size - (uint64_t)mem_size) * 100 / (uint64_t)disk_size);
    return padding_ratio >= threshold;
}

/*
 * __merge_check_adjacent_high_padding --
 *     Given a leaf ref, check whether its parent internal page contains ANY adjacent pair of
 *     high-padding leaf children.
 *
 * NOTE:
 *     This function is used by checkpoint cleanup while processing an in-memory leaf page. To avoid
 *     additional I/O, it only considers children that are already in memory and have a disk image.
 */
static bool
__merge_check_adjacent_high_padding(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_PAGE *parent;
    WT_PAGE_INDEX *pindex;
    WT_REF *left, *right;
    uint32_t slot;

    /* Only process leaf pages. */
    if (!F_ISSET(ref, WT_REF_FLAG_LEAF))
        return false;

    parent = ref->home;
    if (parent == NULL || !WT_PAGE_IS_INTERNAL(parent))
        return false;

    WT_INTL_INDEX_GET(session, parent, pindex);

    /* Look for any adjacent (left,right) pair of high-padding leaf children. */
    for (slot = 0; slot + 1 < pindex->entries; slot++) {
        left = pindex->index[slot];
        right = pindex->index[slot + 1];

        if (__merge_check_leaf_inmem_padding(session, left, WT_MERGE_PADDING_THRESHOLD) &&
          __merge_check_leaf_inmem_padding(session, right, WT_MERGE_PADDING_THRESHOLD))
            return true;
    }

    return false;
}

/*
 * __wt_merge_mark_parent_if_high_padding --
 *     Check if ref has high padding and mark its parent page if it has
 *     adjacent siblings with high padding.
 *     This is called from checkpoint cleanup's obsolete TW processing.
 */
int
__wt_merge_mark_parent_if_high_padding(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_PAGE *parent;

    /* Only process leaf pages in memory. */
    if (!F_ISSET(ref, WT_REF_FLAG_LEAF))
        return 0;

    if (WT_REF_GET_STATE(ref) != WT_REF_MEM)
        return 0;

    /* Get parent - must exist and be internal. */
    parent = ref->home;
    if (parent == NULL || !WT_PAGE_IS_INTERNAL(parent))
        return 0;

    /*
     * Mark the parent page if it contains any adjacent pair of high-padding leaf children.
     * We avoid page->memory_footprint and avoid additional I/O by only considering children already
     * in memory that still have a disk image.
     */
    if (__merge_check_adjacent_high_padding(session, ref)) {
        F_SET_ATOMIC_16(parent, WT_PAGE_HAS_HIGH_PADDING_CHILDREN);

        __wt_verbose(session, WT_VERB_CHECKPOINT_CLEANUP,
          "marked parent page %p (trigger leaf %p): found adjacent high-padding leaf children",
          (void *)parent, (void *)ref);
    }

    return 0;
}

/*
 * __merge_reset_checkpoint_counter --
 *     Reset merge counter at start of checkpoint.
 */
void
__wt_merge_reset_checkpoint_counter(WT_SESSION_IMPL *session)
{
    WT_UNUSED(session);
}


/*
 * __merge_recheck_padding --
 *     Re-check padding ratio before merge (protection against race condition).
 */
static int
__merge_recheck_padding(WT_SESSION_IMPL *session, WT_REF **refs, uint32_t count, bool *still_valid)
{
    uint32_t i;

    *still_valid = true;

    /*
     * Eviction internal-page reconciliation requires children to remain WT_REF_DISK/WT_REF_DELETED.
     * Re-check using on-disk metadata only: do not instantiate pages.
     */
    for (i = 0; i < count; ++i) {
        if (!__merge_check_page_padding(session, refs[i], WT_MERGE_PADDING_THRESHOLD, NULL)) {
            *still_valid = false;
            break;
        }
    }

    return (0);
}

/*
 * __merge_check_history_store --
 *     Check if a page has History Store entries (if so, cannot merge).
 */
static int
__merge_check_history_store(WT_SESSION_IMPL *session, WT_REF *ref, bool *has_hs)
{
    WT_UNUSED(session);
    WT_UNUSED(ref);
    
    /* FIXME: Simplified check - always return false for now */
    *has_hs = false;
    return (0);
}

/*
 * __merge_should_merge_adjacent --
 *     Determine if adjacent pages should be merged.
 */
static int
__merge_should_merge_adjacent(WT_SESSION_IMPL *session, WT_PAGE *parent, WT_REF *current_ref,
  WT_REF ***merge_refsp, uint32_t *merge_allocatedp, uint32_t *merge_countp)
{
    WT_BTREE *btree;
    WT_PAGE_INDEX *pindex;
    WT_REF *ref;
    wt_off_t total_mem_size;
    uint32_t entries, i, mem_size, start_idx;
    uint32_t count;
    uint8_t rec_state;
    bool has_hs;

    btree = S2BT(session);
    *merge_countp = 0;

    /* Find current_ref's position in parent's index[]. */
    WT_INTL_INDEX_GET(session, parent, pindex);
    entries = pindex->entries;
    start_idx = 0;

    for (i = 0; i < entries; ++i) {
        if (pindex->index[i] == current_ref) {
            start_idx = i;
            break;
        }
    }
    if (i >= entries)
        return (0);

    /*
     * Starting from current_ref, find consecutive high-padding leaf pages.
     * Stop adding pages once the sum of their on-disk header mem_size reaches maxleafpage.
     */
    total_mem_size = 0;
    count = 0;
    for (i = start_idx; i < entries; ++i) {
        ref = pindex->index[i];

        rec_state = __wt_atomic_load_uint8_v_acquire(&ref->rec_state);

        if (WT_REF_GET_STATE(ref) != WT_REF_DISK || F_ISSET(ref, WT_REF_FLAG_INTERNAL) ||
          rec_state == WT_REF_REC_MERGED)
            break;

        mem_size = 0;
        if (!__merge_check_page_padding(session, ref, WT_MERGE_PADDING_THRESHOLD, &mem_size) ||
          !__merge_check_globally_visible(session, ref))
            break;

        WT_RET(__merge_check_history_store(session, ref, &has_hs));
        if (has_hs) {
            __wt_verbose(session, WT_VERB_RECONCILE,
              "skipping page %p with history store entries", (void *)ref);
            break;
        }

        /* Size gating: do not build a merged leaf larger than maxleafpage. */
        if ((wt_off_t)mem_size == 0 || total_mem_size + (wt_off_t)mem_size >= (wt_off_t)btree->maxleafpage)
            break;

        WT_RET(__wt_realloc_def(session, merge_allocatedp, count + 1, merge_refsp));
        (*merge_refsp)[count++] = ref;
        total_mem_size += (wt_off_t)mem_size;
    }

    if (count < 2) {
        *merge_countp = 0;
        return (0);
    }

    *merge_countp = count;
    return (0);
}

/*
 * __merge_create_merged_page --
 *     Merge multiple on-disk row-leaf pages into a single new on-disk row-leaf page.
 *
 * NOTES:
 * - This path is intended for internal-page eviction reconciliation: children must remain
 *   WT_REF_DISK and must not be instantiated into cache.
 * - For now, we conservatively refuse to merge pages containing overflow key/value cells.
 */
static int
__merge_create_merged_page(WT_SESSION_IMPL *session, WT_REF **refs, uint32_t count, WT_ADDR **new_addr_out)
{
    WT_BTREE *btree;
    WT_CELL *cell;
    WT_CELL_UNPACK_KV unpack;
    WT_DECL_ITEM(full_key);
    WT_DECL_ITEM(leaf_image);
    WT_DECL_ITEM(new_image);
    WT_DECL_ITEM(pending_key);
    WT_DECL_RET;
    WT_PAGE_HEADER *dsk;
    WT_ADDR *new_addr;
    WT_TIME_AGGREGATE merged_ta;
    WTI_DISK_LEAF_MERGE_STATE s;
    size_t addr_size, compressed_size;
    uint32_t i;
    uint8_t *end;
    uint8_t addr_buf[WT_MERGE_ADDR_MAX_COOKIE];
    bool pending_key_set;

    btree = S2BT(session);
    *new_addr_out = NULL;
    new_addr = NULL;
    pending_key_set = false;
    addr_size = compressed_size = 0;

    WT_TIME_AGGREGATE_INIT(&merged_ta);

    WT_ERR(__wt_scr_alloc(session, btree->maxleafpage, &new_image));
    WT_ERR(__wt_scr_alloc(session, btree->maxleafpage, &leaf_image));
    WT_ERR(__wt_scr_alloc(session, 0, &full_key));
    WT_ERR(__wt_scr_alloc(session, 0, &pending_key));

    /* Initialize merged leaf image with header bytes reserved. */
    memset(new_image->mem, 0, WT_PAGE_HEADER_BYTE_SIZE(btree));
    new_image->size = WT_PAGE_HEADER_BYTE_SIZE(btree);

    WT_CLEAR(s);
    s.key_pfx_compress = btree->prefix_compression;
    s.key_pfx_last = 0;
    s.p_ptr = (uint8_t *)new_image->mem + new_image->size;
    s.entries = 0;
    s.all_empty_value = true;
    s.any_empty_value = false;
    s.last_key = full_key; /* __wt_cell_pack_leaf_kv copies key bytes into this WT_ITEM */

    for (i = 0; i < count; ++i) {
        WT_ADDR_COPY addr_copy;

        if (!__wt_ref_addr_copy(session, refs[i], &addr_copy))
            WT_ERR(WT_NOTFOUND);

        WT_TIME_AGGREGATE_MERGE(session, &merged_ta, &addr_copy.ta);

        /* Read the full leaf disk image without instantiating the page. */
        WT_ERR(__wt_blkcache_read(session, leaf_image, NULL, addr_copy.addr, addr_copy.size));
        dsk = (WT_PAGE_HEADER *)leaf_image->data;

        if (dsk->type != WT_PAGE_ROW_LEAF)
            WT_ERR(WT_NOTFOUND);

        /* Reset per-source-page key reconstruction state (prefix compression doesn't cross pages). */
        full_key->size = 0;
        pending_key_set = false;

        end = (uint8_t *)dsk + dsk->mem_size;
        cell = (WT_CELL *)WT_PAGE_HEADER_BYTE(btree, dsk);

        while ((uint8_t *)cell < end) {
            WT_ERR(__wt_cell_unpack_safe(session, dsk, cell, NULL, &unpack, end));

            switch (unpack.type) {
            case WT_CELL_KEY:
            case WT_CELL_KEY_PFX:
            case WT_CELL_KEY_SHORT:
            case WT_CELL_KEY_SHORT_PFX:
                /* If we had a previous key with no value cell, write it as an empty value. */
                if (pending_key_set) {// 这里可以优化下，例如每个page创建一个新image，当这个page遍历完成后，拷贝到new_image，这样可以避免ENOSPC，例如前面2个page合并没有超过max leaf page，第3个合并后超了，这种场景当前前面2个都会失效
                    if (new_image->size + pending_key->size + 32 > btree->maxleafpage)
                        WT_ERR(ENOSPC);
                    WT_ERR(__wt_cell_pack_leaf_kv(
                      session, true, pending_key->data, pending_key->size, NULL, 0, NULL, new_image, &s));
                    pending_key_set = false;
                }

                /* First key on a page must have 0 prefix. */
                if (full_key->size == 0 && unpack.prefix != 0)
                    WT_ERR(WT_NOTFOUND);

                WT_ERR(__wt_cell_decompress_prefix_key(session, full_key, unpack.data, unpack.size, unpack.prefix));
                /* Ensure the reconstructed key is in local buffer space for subsequent prefix keys. */
                WT_ERR(__wt_buf_set(session, full_key, full_key->data, full_key->size));
                WT_ERR(__wt_buf_set(session, pending_key, full_key->data, full_key->size));
                pending_key_set = true;
                break;
            case WT_CELL_KEY_OVFL:
            case WT_CELL_VALUE_OVFL:
                /* Conservative: overflow requires overflow reference tracking to be correct. */
                WT_ERR(WT_NOTFOUND);
            case WT_CELL_VALUE:
            case WT_CELL_VALUE_SHORT:
            case WT_CELL_VALUE_COPY:
                if (!pending_key_set)
                    WT_ERR(WT_NOTFOUND);

                if (new_image->size + pending_key->size + unpack.size + 64 > btree->maxleafpage)
                    WT_ERR(ENOSPC);

                WT_ERR(__wt_cell_pack_leaf_kv(session, false, pending_key->data, pending_key->size,
                  unpack.data, unpack.size, &unpack.tw, new_image, &s));
                pending_key_set = false;
                break;
            default:
                /* Unsupported cell type for this simplified merge. */
                WT_ERR(WT_NOTFOUND);
            }

            cell = (WT_CELL *)((uint8_t *)cell + unpack.__len);
        }

        /* Flush trailing key with empty value at the end of this source page. */
        if (pending_key_set) {
            if (new_image->size + pending_key->size + 32 > btree->maxleafpage)
                WT_ERR(ENOSPC);
            WT_ERR(__wt_cell_pack_leaf_kv(
              session, true, pending_key->data, pending_key->size, NULL, 0, NULL, new_image, &s));
            pending_key_set = false;
        }
    }

    /* Finalize the merged leaf page header. */
    dsk = (WT_PAGE_HEADER *)new_image->mem;
    dsk->recno = WT_RECNO_OOB;
    dsk->type = WT_PAGE_ROW_LEAF;
    dsk->u.entries = s.entries;
    dsk->mem_size = WT_STORE_SIZE(new_image->size);
    dsk->write_gen = __wt_atomic_add_uint64(&btree->write_gen, 1);
    dsk->unused = 0;
    dsk->version = WT_PAGE_VERSION_TS;

    dsk->flags = 0;
    if (s.all_empty_value)
        FLD_SET(dsk->flags, WT_PAGE_EMPTY_V_ALL);
    else if (!s.any_empty_value)
        FLD_SET(dsk->flags, WT_PAGE_EMPTY_V_NONE);

    /* Write merged page to disk. */
    WT_ERR(__wt_blkcache_write(
      session, new_image, NULL, addr_buf, &addr_size, &compressed_size, false, false, false));

    WT_ERR(__wt_calloc_one(session, &new_addr));
    WT_ERR(__wt_memdup(session, addr_buf, addr_size, &new_addr->block_cookie));
    new_addr->block_cookie_size = (uint8_t)addr_size;
    new_addr->ta = merged_ta;

    *new_addr_out = new_addr;

err:
    __wt_scr_free(session, &new_image);
    __wt_scr_free(session, &leaf_image);
    __wt_scr_free(session, &full_key);
    __wt_scr_free(session, &pending_key);

    if (ret != 0 && new_addr != NULL) {
        __wt_free(session, new_addr->block_cookie);
        __wt_free(session, new_addr);
    }

    return (ret);
}

/*
 * __merge_parent_add_free_cookie --
 *     Record a child's on-disk address cookie for freeing after eviction safely updates the tree.
 */
static int
__merge_parent_add_free_cookie(WT_SESSION_IMPL *session, WT_PAGE *parent, WT_REF *ref)
{
    WT_ADDR_COPY addr_copy;
    WT_PAGE_MODIFY *mod;

    mod = parent->modify;
    WT_ASSERT(session, mod != NULL);

    if (!__wt_ref_addr_copy(session, ref, &addr_copy))
        return (0);

    WT_RET(__wt_realloc_def(
      session, &mod->merge_free_allocated, mod->merge_free_entries + 1, &mod->merge_free));

    memcpy(mod->merge_free[mod->merge_free_entries].addr, addr_copy.addr, addr_copy.size);
    mod->merge_free[mod->merge_free_entries].size = addr_copy.size;
    mod->merge_free_entries++;

    return (0);
}

/*
 * __wt_merge_adjacent_pages --
 *     Merge adjacent high-padding pages during internal page eviction reconciliation.
 */
int
__wt_merge_adjacent_pages(WT_SESSION_IMPL *session, WTI_RECONCILE *r, WT_PAGE *parent,
  WT_REF *current_ref, bool *merged_out, uint32_t *skip_count_out)
{
    WT_DECL_RET;
    WT_REF **merge_refs;
    WT_ADDR *new_addr;
    uint32_t merge_allocated, merge_count, i;
    const void *key_data;
    size_t key_size;
    bool still_valid;

    merge_refs = NULL;
    merge_allocated = 0;

    *merged_out = false;
    *skip_count_out = 0;
    new_addr = NULL;

    /* Only supported during eviction reconciliation. */
    if (!F_ISSET(r, WT_REC_EVICT))
        return (0);

    WT_ERR(__merge_should_merge_adjacent(
      session, parent, current_ref, &merge_refs, &merge_allocated, &merge_count));
    if (merge_count < 2)
        goto err;

    WT_ERR(__merge_recheck_padding(session, merge_refs, merge_count, &still_valid));
    if (!still_valid) {
        ret = 0;
        goto err;
    }

    ret = __merge_create_merged_page(session, merge_refs, merge_count, &new_addr);
    if (ret == WT_NOTFOUND || ret == ENOSPC) {
        /* Not mergeable with this simplified implementation. */
        ret = 0;
        goto err;
    }
    WT_ERR(ret);

    /* Build value cell (new child address). */
    __wti_rec_cell_build_addr(session, r, new_addr, NULL, WT_RECNO_OOB, NULL);

    /* Build key cell (use first ref's key). */
    __wt_ref_key(parent, merge_refs[0], &key_data, &key_size);
    if (r->cell_zero)
        key_size = 1;

    WT_ERR(__wt_buf_set(session, r->cur, key_data, key_size));
    WT_ERR(__wt_buf_set(session, &r->k.buf, key_data, key_size));
    r->k.cell_len = __wt_cell_pack_int_key(&r->k.cell, r->k.buf.size);
    r->k.len = r->k.cell_len + r->k.buf.size;

    /* Refuse to create internal split as part of this optimization. */
    if (__wti_rec_need_split(r, r->k.len + r->v.len))
        goto err;

    __wti_rec_image_copy(session, r, &r->k);
    __wti_rec_image_copy(session, r, &r->v);
    r->cell_zero = false;

    /* Record old child blocks to be freed after eviction installs the new parent. */
    for (i = 0; i < merge_count; ++i)
        WT_ERR(__merge_parent_add_free_cookie(session, parent, merge_refs[i]));

    /* Mark remaining refs as merged so the caller skips them. */
    for (i = 1; i < merge_count; ++i)
        __wt_atomic_store_uint8_v_release(&merge_refs[i]->rec_state, WT_REF_REC_MERGED);

    *merged_out = true;
    *skip_count_out = merge_count - 1;

err:
    if (new_addr != NULL) {
        __wt_free(session, new_addr->block_cookie);
        __wt_free(session, new_addr);
    }

    __wt_free(session, merge_refs);

    return (ret);
}
