/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 */

#include <test_util.h>

#define HOME_DIR "WT_HOME_REF_ADDR_DEMO"
#define TABLE_URI "table:ref_addr_demo"

/*
 * To create pages with high padding ratio:
 * - Use small KV (~200 bytes value)
 * - leaf_page_max=8KB (can fit ~30+ small KVs)
 * - Insert many records to fill pages, checkpoint
 * - Delete most records, keeping only 1-2 per page
 * - Checkpoint again -> pages now have 1-2 KVs but 8KB size = lots of padding
 */
#define VALUE_SIZE 200
#define TOTAL_RECORDS 100      /* Insert 300 records to create ~10 pages */
#define RECORDS_PER_PAGE 30    /* Estimate: ~30 records per 8KB page */

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
    printf("Inserted %d records: key_%010d - key_%010d\n", 
           end_key - start_key + 1, start_key, end_key);
}

/*
 * delete_most_records --
 *     Delete most records, keeping only 2 per original page range.
 *     Original pages have ~30 records each.
 *     Keep: 1,2 (page1), 31,32 (page2), 61,62 (page3), 91,92 (page4)
 */
static void
delete_most_records(WT_SESSION *session, int start_key, int end_key)
{
    WT_CURSOR *cursor;
    char key[64];
    int i, pos_in_page, deleted = 0, kept = 0;

    error_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));

    for (i = start_key; i <= end_key; i++) {
        /* Calculate position within page range (0-based) */
        pos_in_page = (i - start_key) % RECORDS_PER_PAGE;

        /* Keep only first 1 record of each page range (not 2) */
        if (pos_in_page < 1) {
            kept++;
            continue;
        }

        testutil_snprintf(key, sizeof(key), "key_%010d", i);
        cursor->set_key(cursor, key);
        error_check(cursor->remove(cursor));
        deleted++;
    }

    error_check(cursor->close(cursor));
    printf("Deleted %d records, kept %d records (1 per page range)\n", deleted, kept);
}

/*
 * do_checkpoint --
 *     Perform a checkpoint.
 */
static void
do_checkpoint(WT_SESSION *session, const char *msg)
{
    error_check(session->checkpoint(session, NULL));
    printf("Checkpoint completed: %s\n", msg);
}

/*
 * verify_dump_pages --
 *     Run wt verify with dump_pages option.
 */
static void
verify_dump_pages(void)
{
    char cmd[1024];
    char cwd[512];
    char wt_path[512];
    char *build_dir;

    if (getcwd(cwd, sizeof(cwd)) == NULL) {
        fprintf(stderr, "Failed to get current directory\n");
        return;
    }

    /* Find the build directory to locate wt binary */
    testutil_snprintf(wt_path, sizeof(wt_path), "%s", cwd);
    build_dir = strstr(wt_path, "/examples/c");
    if (build_dir != NULL) {
        *build_dir = '\0';
    }

    printf("\n=== Running verify -d dump_pages ===\n");
    printf("Current dir: %s\n", cwd);
    printf("WT binary dir: %s\n", wt_path);
    testutil_snprintf(cmd, sizeof(cmd),
      "%s/wt -h %s/%s verify -u -d dump_pages %s 2>&1",
      wt_path, cwd, HOME_DIR, TABLE_URI);

    printf("Command: %s\n", cmd);
    if (system(cmd) != 0)
        fprintf(stderr, "Warning: verify command returned non-zero\n");
}

int
main(int argc, char *argv[])
{
    WT_CONNECTION *conn;
    WT_SESSION *session;

    (void)argc;
    (void)argv;

    /* Clean up and create home directory */
    printf("Cleaning up old data directory: %s\n", HOME_DIR);
    testutil_remove(HOME_DIR);
    testutil_recreate_dir(HOME_DIR);

    /* Initialize random state */
    __wt_random_init_default(&rnd);

    /* Open connection and create table */
    error_check(wiredtiger_open(HOME_DIR, NULL,
      "create,cache_size=100MB,statistics=(all),statistics_log=(wait=0)",
      &conn));
    error_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create table with 8KB page size */
    error_check(session->create(session, TABLE_URI,
      "key_format=S,value_format=S,leaf_page_max=8KB,internal_page_max=8KB"));
    printf("Table created with leaf_page_max=8KB\n");

    /* Step 1: Insert many small records to fill multiple pages */
    printf("\n=== Step 1: Insert records ===\n");
    insert_records(session, 1, TOTAL_RECORDS);

    /* Step 2: Checkpoint to write pages to disk */
    printf("\n=== Step 2: Checkpoint after insert ===\n");
    do_checkpoint(session, "after insert (pages filled with ~30 records each)");

    /* Close and reopen to ensure pages are evicted from cache */
    error_check(conn->close(conn, NULL));
    printf("\nConnection closed after insert.\n");

    error_check(wiredtiger_open(HOME_DIR, NULL,
      "cache_size=100MB,statistics=(all),statistics_log=(wait=0),"
      "verbose=[checkpoint_cleanup:0]",
      &conn));
    error_check(conn->open_session(conn, NULL, NULL, &session));
    printf("Connection reopened.\n");

    /* Delay 10 seconds for debugging/observation */
    printf("Waiting 1 seconds...\n");
    sleep(1);

    /* Step 3: Delete most records, keep only 1-2 per page */
    printf("\n=== Step 3: Delete most records ===\n");
    delete_most_records(session, 1, TOTAL_RECORDS);

    /* Step 4: Checkpoint again - pages should have tombstones, not be rewritten */
    printf("\n=== Step 4: Checkpoint after delete ===\n");
    do_checkpoint(session, "after delete");

    /* Close connection */
    error_check(conn->close(conn, NULL));
    printf("\nConnection closed.\n");

    /* Verify with dump_pages to see the padding */
    verify_dump_pages();

    error_check(wiredtiger_open(HOME_DIR, NULL,
      "cache_size=1MB,statistics=(all),statistics_log=(wait=0),"
      "verbose=[checkpoint_cleanup:0]",
      &conn));
    error_check(conn->open_session(conn, NULL, NULL, &session));
    printf("Connection reopened.\n");
    
    WT_CURSOR *cursor;
    error_check(session->open_cursor(session, TABLE_URI, NULL, NULL, &cursor));

    /* Delay 10 seconds for debugging/observation */
    printf("Waiting 10 seconds...\n");
    sleep(3);
    do_checkpoint(session, "after waiting1");
    do_checkpoint(session, "after waiting2");
    error_check(conn->close(conn, NULL));

    verify_dump_pages();

    printf("\nDemo completed successfully.\n");
    printf("Expected result: Pages may still be ~8KB with only 1-2 live KVs.\n");
    return (EXIT_SUCCESS);
}
