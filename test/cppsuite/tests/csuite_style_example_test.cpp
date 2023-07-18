#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "src/common/thread_manager.h"
#include "src/storage/connection_manager.h"

#include <map>
#include <iostream>

extern "C" {
#include "wiredtiger.h"
#include "test_util.h"
}

using namespace test_harness;

/* Declarations to avoid the error raised by -Werror=missing-prototypes. */
void insert_op(WT_CURSOR *cursor, int key_size, int value_size);

bool do_inserts = false;
int num_unique_inserts = 0;

void
insert_op(WT_CURSOR *cursor, int key_size, int value_size)
{
    logger::log_msg(LOG_INFO, "called insert_op");

    /* Insert random data. */
    std::string key, value;
    while (do_inserts) {
        key = random_generator::instance().generate_random_string(key_size);
        value = random_generator::instance().generate_random_string(value_size);
        cursor->set_key(cursor, key.c_str());

        // Check if the key already exists.
        int ret = cursor->search(cursor);
        testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        if (ret == WT_NOTFOUND)
            ++num_unique_inserts;

        // Insert/update.
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, value.c_str());
        testutil_check(cursor->insert(cursor));

        testutil_check(cursor->reset(cursor));
    }
}

int
main(int argc, char *argv[])
{
    /* Set the program name for error messages. */
    const std::string progname = testutil_set_progname(argv);

    /* Set the tracing level for the logger component. */
    logger::trace_level = LOG_INFO;

    /* Printing some messages. */
    logger::log_msg(LOG_INFO, "Starting " + progname);

    /* Create a connection, set the cache size and specify the home directory. */
    const std::string conn_config = CONNECTION_CREATE + ",cache_size=500MB";
    const std::string home_dir = std::string(DEFAULT_DIR) + '_' + progname;

    /* Create connection. */
    connection_manager::instance().create(conn_config, home_dir);
    WT_CONNECTION *conn = connection_manager::instance().get_connection();

    /* Open different sessions. */
    WT_SESSION *insert_session, *read_session;
    testutil_check(conn->open_session(conn, nullptr, nullptr, &insert_session));
    testutil_check(conn->open_session(conn, nullptr, nullptr, &read_session));

    /* Random seed. */
    __wt_random_init_seed(
      (WT_SESSION_IMPL *)insert_session, &((WT_SESSION_IMPL *)insert_session)->rnd);

    /* Create a collection. */
    const std::string collection_name = "table:my_collection";
    // const std::string collection_cfg = std::string(DEFAULT_FRAMEWORK_SCHEMA) +
    //   "internal_page_max=512,allocation_size=512,leaf_page_max=512";
    const std::string collection_cfg = std::string(DEFAULT_FRAMEWORK_SCHEMA) + "split_pct=100";
    testutil_check(insert_session->create(
      insert_session, collection_name.c_str(), collection_cfg.c_str()));

    /* Open different cursors. */
    WT_CURSOR *insert_cursor, *read_cursor;
    const std::string cursor_insert_config = "";
    // const std::string cursor_insert_config = "debug=(release_evict=true)";
    const std::string cursor_read_config = "";
    testutil_check(insert_session->open_cursor(
      insert_session, collection_name.c_str(), nullptr, cursor_insert_config.c_str(), &insert_cursor));
    testutil_check(read_session->open_cursor(
      read_session, collection_name.c_str(), nullptr, cursor_read_config.c_str(), &read_cursor));

    /* Store cursors. */
    std::vector<WT_CURSOR *> cursors;
    cursors.push_back(insert_cursor);
    cursors.push_back(read_cursor);

    /* Insert some data. */
    std::string key = "a";
    const std::string value = "b";
    insert_cursor->set_key(insert_cursor, key.c_str());
    insert_cursor->set_value(insert_cursor, value.c_str());
    testutil_check(insert_cursor->insert(insert_cursor));

    /* Read some data. */
    read_cursor->set_key(read_cursor, key.c_str());
    testutil_check(read_cursor->search(read_cursor));
    /* If we leave the cursor positioned, this seems to bias the random cursor! */
    testutil_check(read_cursor->reset(read_cursor));

    /* Create a thread manager and spawn some threads that will work. */
    thread_manager t;
    int key_size = 2, value_size = 2;

    do_inserts = true;
    t.add_thread(insert_op, insert_cursor, key_size, value_size);

    /* Sleep for the test duration. */
    int test_duration_s = 5;
    std::this_thread::sleep_for(std::chrono::seconds(test_duration_s));

    /* Stop the threads. */
    do_inserts = false;
    t.join();

    /* Close cursors. */
    for (auto c : cursors)
        testutil_check(c->close(c));

    /* Another message. */
    logger::log_msg(LOG_INFO, "End of test.");

    // Before the sampling, let's do a checkpoint.
    const std::string ckpt_config = "";
    testutil_check(insert_session->checkpoint(insert_session, ""));
    logger::log_msg(LOG_INFO, "Checkpoint done.");

    // And let's evict everything.
    WT_CURSOR *evict_cursor;
    testutil_check(insert_session->open_cursor(
      insert_session, collection_name.c_str(), nullptr, cursor_insert_config.c_str(), &insert_cursor));
    const std::string evict_config("debug=(release_evict)");
    testutil_check(insert_session->open_cursor(
      insert_session, collection_name.c_str(), nullptr, evict_config.c_str(), &evict_cursor));
    int ret;
    
    while((ret = insert_cursor->next(insert_cursor)) != WT_NOTFOUND) {
      char * key_tmp;
      insert_cursor->get_key(insert_cursor, &key_tmp);
      evict_cursor->set_key(evict_cursor, key_tmp);
      testutil_check(evict_cursor->search(evict_cursor));
      testutil_check(evict_cursor->reset(evict_cursor));
    }
    testutil_assert(ret == WT_NOTFOUND);
      testutil_check(insert_cursor->close(insert_cursor));
      testutil_check(evict_cursor->close(evict_cursor));
    logger::log_msg(LOG_INFO, "Eviction done.");
    
    // Use a random cursor and go through what we have.
    WT_CURSOR *random_cursor;

    // Random cursors config.
    const std::string random_config("next_random=true");
    // const std::string random_config("next_random=true,next_random_sample_size=100000");
    testutil_check(insert_session->open_cursor(
      insert_session, collection_name.c_str(), nullptr, random_config.c_str(), &random_cursor));

    // Map of seen elements.
    std::map<std::string, int> seen;

    int sample_size = 10000;
    for (int i = 0; i < sample_size; ++i) {
        const char *my_key;
        testutil_check(random_cursor->next(random_cursor));
        testutil_check(random_cursor->get_key(random_cursor, &my_key));
        testutil_check(random_cursor->reset(random_cursor));
        seen[my_key]++;
    }

    for (const auto &elem : seen) {
        std::cout << elem.first << " " << elem.second << std::endl;
    }
    std::cout << "Number of elements seen: " << seen.size() << " vs num inserts " << num_unique_inserts
              << ". " << (seen.size() * 100 / num_unique_inserts) << "% of the dataset covered."
              << std::endl;

    return (0);
}
