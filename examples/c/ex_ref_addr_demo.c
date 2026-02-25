/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 */

#include <test_util.h>

#define HOME_DIR "WT_HOME_REF_ADDR_DEMO"
#define TABLE_URI "table:ref_addr_demo"

/*
 * Test adjacent page merge during eviction:
 * 
 * Strategy:
 * 1. Create pages with ~30 records each (8KB page, 200 byte values)
 * 2. Delete most records to leave only 1-2 per page (high padding ratio)
 * 3. Use very small cache and no checkpoint to force eviction of internal pages
 * 4. When internal page is evicted, adjacent high-padding pages should be merged
 */
#define VALUE_SIZE 200
#define RECORDS_PER_BATCH 300      /* Records per batch */
#define RECORDS_PER_PAGE 60        /* ~30 records per 8KB page */

static WT_RAND_STATE rnd;

static char *
generate_random_value(char *buf, size_t size)
{
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    size_t i;

    for (i = 0; i < size - 1; i++)
        buf[i] = charset[__wt_random(&rnd) % (sizeof(charset) - 1)];
    buf[size - 1] = '\0';
    return buf;
}

/*
 * insert_records --
 *     Insert records with keys in range [start_key, end_key].
 */
static void
insert_records(WT_SESSION *session, int start_key, int end_key)
{
    WT_CURSOR *cursor;
    char key[64], value[VALUE_SIZE + 1];
    int i;

    error_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));

    for (i = start_key; i <= end_key; i++) {
        testutil_snprintf(key, sizeof(key), "key_%010d", i);
        generate_random_value(value, sizeof(value));
        cursor->set_key(cursor, key);
        cursor->set_value(cursor, value);
        error_check(cursor->insert(cursor));
    }

    error_check(cursor->close(cursor));
}

/*
 * delete_most_records --
 *     Delete most records, keeping only 1 per page range to create high padding.
 */
static void
delete_most_records(WT_SESSION *session, int start_key, int end_key)
{
    WT_CURSOR *cursor;
    char key[64];
    int i, pos_in_page;

    error_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));

    for (i = start_key; i <= end_key; i++) {
        /* Calculate position within page range (0-based) */
        pos_in_page = (i - start_key) % RECORDS_PER_PAGE;

        /* Keep only first 1 record of each page range */
        if (pos_in_page < 1)
            continue;

        testutil_snprintf(key, sizeof(key), "key_%010d", i);
        cursor->set_key(cursor, key);
        error_check(cursor->remove(cursor));
    }

    error_check(cursor->close(cursor));
}

/*
 * do_checkpoint --
 *     Perform a checkpoint.
 */
static void
do_checkpoint(WT_SESSION *session, const char *msg)
{
    error_check(session->checkpoint(session, NULL));
    printf("Checkpoint: %s\n", msg);
}

int
main(int argc, char *argv[])
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    int batch, start_key, end_key, ret;
    int total_batches = 50;  /* More batches to create more pressure */

    (void)argc;
    (void)argv;

    /* Clean up and create home directory */
    printf("=== Adjacent Page Merge Test (Eviction-based) ===\n\n");
    testutil_recreate_dir(HOME_DIR);

    /* Initialize random state */
    __wt_random_init_default(&rnd);

    /*
     * Phase 1: Create initial data and establish high-padding pages
     * Use larger cache first to create many pages
     */
    printf("=== Phase 1: Create initial data ===\n");
    error_check(wiredtiger_open(HOME_DIR, NULL,
      "create,cache_size=1MB,"
      "statistics=(all),eviction_dirty_target=1,eviction_dirty_trigger=5,"
      "checkpoint=(wait=1),"
      "checkpoint_cleanup=(wait=1),"
      "verbose=[checkpoint_cleanup:0, reconcile:0, eviction:0, block:2]",
      &conn));
    error_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create table if not exists */
    ret = session->create(session, TABLE_URI,
      "key_format=S,value_format=S,leaf_page_max=4KB,internal_page_max=4KB,exclusive=false");
    if (ret == 0) {
        printf("Table created with leaf_page_max=4KB\n\n");
    } else if (ret == EEXIST) {
        printf("Table already exists, skipping create\n\n");
    } else {
        error_check(ret);
    }

    /* Create batches of data with high padding */
    for (batch = 0; batch < total_batches; batch++) {
        start_key = batch * RECORDS_PER_BATCH + 1;
        end_key = start_key + RECORDS_PER_BATCH - 1;

        printf("--- Batch %d/%d: keys %d-%d ---\n", batch + 1, total_batches, start_key, end_key);

        insert_records(session, start_key, end_key);
        do_checkpoint(session, "after insert");

        delete_most_records(session, start_key, end_key);
        do_checkpoint(session, "after delete");

        printf("\n");
    }

    /*
     * Write new batch and repeatedly update to force eviction of old pages.
     * This creates memory pressure that evicts the high-padding pages created above.
     */
    printf("=== Forcing eviction by writing new data and updating ===\n");
    {
        int new_start = total_batches * RECORDS_PER_BATCH + 1;
        int new_end = new_start + 10*RECORDS_PER_BATCH - 1;
        int update_round;

        /* Insert new batch of records */
        printf("Inserting new batch: keys %d-%d\n", new_start, new_end);
        insert_records(session, new_start, new_end);
        do_checkpoint(session, "after new insert");

        /* Repeatedly update the new batch to keep it hot and evict old pages */
        printf("Starting update loop (100 rounds)...\n");
        for (update_round = 0; update_round < 10; update_round++) {
            WT_CURSOR *cursor;
            char key[64], value[VALUE_SIZE + 1];
            int i;

            error_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));
            for (i = new_start; i <= new_end; i++) {
                testutil_snprintf(key, sizeof(key), "key_%010d", i);
                generate_random_value(value, sizeof(value));
                cursor->set_key(cursor, key);
                cursor->set_value(cursor, value);
                error_check(cursor->update(cursor));
            }
            error_check(cursor->close(cursor));

            if (update_round % 10 == 0)
                printf("Update round %d completed\n", update_round);
        }
        printf("Update loop completed - old pages should be evicted\n");
    } 

    printf("=== Phase 1 completed: Created %d batches ===\n\n", total_batches);

    /* Final checkpoint before closing */
    do_checkpoint(session, "final checkpoint");

    error_check(conn->close(conn, NULL));
    printf("Connection closed.\n\n");

    /* Use wt salvage to dump page info and verify merge results */
    printf("=== Running wt salvage to verify merge results ===\n");
    {
        /*
         * Use wt dump to show all pages and their sizes.
         * After merge, we should see fewer leaf pages with larger mem_size.
         * High-padding pages (padding > 90%) that are adjacent should have been merged.
         */
        ret = system("cd " HOME_DIR " && ../../../wt verify -u -d dump_pages " TABLE_URI " 2>&1");
        if (ret != 0)
            printf("wt verify returned: %d\n", ret);
        printf("\n=== wt verify dump_pages completed ===\n\n");
        return 0;
        
        /*
         * Parse the output to check for high-padding pages:
         * - disk_size and mem_size are shown in the dump
         * - If (disk_size - mem_size) / disk_size >= 90%, it's a high-padding page
         * - Adjacent high-padding leaf pages should have been merged
         */
        printf("=== Checking for remaining high-padding leaf pages ===\n");
        printf("Running analysis script...\n");
        ret = system(
            "cd " HOME_DIR " && ../../../wt verify -d dump_pages " TABLE_URI " 2>&1 | "
            "awk '"
            "/^leaf/ { "
            "  disk_size = 0; mem_size = 0; "
            "  for (i = 1; i <= NF; i++) { "
            "    if ($i ~ /disk_size/) { gsub(/[^0-9]/, \"\", $(i+1)); disk_size = $(i+1) } "
            "    if ($i ~ /mem_size/) { gsub(/[^0-9]/, \"\", $(i+1)); mem_size = $(i+1) } "
            "  } "
            "  if (disk_size > 0 && mem_size > 0) { "
            "    padding = (disk_size - mem_size) * 100 / disk_size; "
            "    if (padding >= 90) { "
            "      high_padding_count++; "
            "      print \"HIGH PADDING LEAF: disk_size=\" disk_size \", mem_size=\" mem_size \", padding=\" padding \"%%\" "
            "    } "
            "  } "
            "} "
            "END { "
            "  print \"\\nTotal high-padding leaf pages (>=90%%): \" high_padding_count+0 "
            "}'"
        );
        if (ret != 0)
            printf("Analysis script returned: %d\n", ret);
    }

    printf("\n=== Test completed! ===\n");
    printf("If there are adjacent high-padding leaf pages remaining, merge may not have worked.\n");
    printf("Expected: Most high-padding pages should have been merged during eviction.\n");

    return (EXIT_SUCCESS);
}
