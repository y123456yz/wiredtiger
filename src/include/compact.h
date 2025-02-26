/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

struct __wt_compact_state {
    //如果启用，compact 命令将返回压缩可以从目标集合中回收的空间大小（以字节为单位）的估计值。如果在 dryRun 设置为 true 的情况下
    //  运行 compact，MongoDB 只返回估计值，而不执行任何类型的压缩。默认值：False
    bool dryrun;                /* Run only the estimation phase */
    uint32_t file_count;        /* Number of files seen */
    uint64_t free_space_target; /* Configured minimum space that should be recovered */
    uint64_t max_time;          /* Configured timeout */

    struct timespec begin;         /* Starting time */
    struct timespec last_progress; /* Last time a progress message was logged. */
};
