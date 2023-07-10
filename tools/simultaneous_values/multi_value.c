/*
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <assert.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

struct _state {
    volatile int value;
};
typedef struct _state state;

#define THREAD_COUNT 16
#define GRID_SIZE 4000
/* Read values of s->value are stored here. */
static int values[THREAD_COUNT][100];
/* Each time a different value is seen the tick counter is saved to this array. */
static unsigned long long timepoints[THREAD_COUNT][100];
/* This records how many values each CPU saw. */
static int measurements[THREAD_COUNT];

struct _thread_info {           /* Used as argument to thread_start(). */
    pthread_t thread_id;        /* ID returned by pthread_create(). */
    int       thread_num;       /* Application-defined thread #. */
    state *s;                   /* Shared state pointer. */
    /* This isn't the same thing is a memory barrier. */
    pthread_barrier_t *barrier; /* A barrier to synchronize thread start. */
};
typedef struct _thread_info thread_info;

/* Get the current value of the TSC register to use a tick count. */
unsigned long long rdtscl(void)
{
    unsigned int lo, hi;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

/* Thread worker function. */
static void *do_work(void *ctx) {
    thread_info *tinfo = (thread_info*)ctx;
    int count, cpu, next, last, id, iteration;
    unsigned long long current;
    count = iteration = 0;
    id = tinfo->thread_num;

    /*
     * Synchronization point. All threads will wait here until each thread has reached here. Without
     * this barrier the threads appear to be relatively synchronous.
     */
    pthread_barrier_wait(tinfo->barrier);
    cpu = sched_getcpu();
    /* We can assert our thread id is equal to our CPU id as we set thread affinity for each
     * thread.
     */
    assert(cpu == id);
    current = rdtscl();
    /* Save out CPU id to s->value. */
    tinfo->s->value = cpu;
    last = cpu;
    timepoints[id][count] = current;
    values[id][count] = cpu;
    count ++;
    while (iteration < 1000000) {
        /*
         * Read from the shared variable, if we see a different value to the one we saw last save it
         * and update our current value. Additionally save the tick count when we saw the value.
         *
         * If we haven't seen a new value in a while we will exit the loop as iteration will reach a
         * hard coded value.
         */
        next = tinfo->s->value;
        if (next != last) {
            current = rdtscl();
            last = next;
            timepoints[id][count] = current;
            values[id][count] = next;
            count ++;
            iteration = 0;
        }
        iteration++;
    }
    measurements[id] = count;
    /* Again assert we haven't changed CPU. */
    assert(sched_getcpu() == id);
    return (NULL);
}

int
main(int argc, char *argv[])
{
    int ret;
    state s;
    thread_info  *tinfo;
    pthread_barrier_t barrier;
    cpu_set_t cpuset;

    tinfo = calloc(THREAD_COUNT, sizeof(thread_info));
    pthread_barrier_init(&barrier, NULL, THREAD_COUNT);

    /* Start the threads. */
    for (int i = 0; i < THREAD_COUNT; i++) {
        tinfo[i].thread_num = i;
        tinfo[i].s = &s;
        tinfo[i].barrier = &barrier;
        ret = pthread_create(&tinfo[i].thread_id, NULL, &do_work, &tinfo[i]);
        assert(ret == 0);
        /* Set the threads CPU affinity. Helps prevent the thread from being moved between CPUs. */
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(tinfo[i].thread_id, sizeof(cpu_set_t), &cpuset);
    }

    /* Join the threads. */
    for (int i = 0; i < THREAD_COUNT; i++) {
        ret = pthread_join(tinfo[i].thread_id, NULL);
        assert(ret == 0);
    }

    free(tinfo);

    /* Tidy up the data for graphing. */
    /* Find the smallest timepoint. */
    unsigned long long smallest = ULLONG_MAX;
    for (int i = 0; i < THREAD_COUNT; i ++) {
        for (int j = 0; j < measurements[i]; j++) {
            if (timepoints[i][j] < smallest) {
                smallest = timepoints[i][j];
            }
        }
    }

    /*
     * Normalize the timepoints. Subtract smallest timepoint from all points and find the
     * largest.
     */
    unsigned long long largest = 0;
    for (int i = 0; i < THREAD_COUNT; i ++) {
        for (int j = 0; j < measurements[i]; j++) {
            timepoints[i][j] = timepoints[i][j] - smallest;
            if (timepoints[i][j] > largest) {
                largest = timepoints[i][j];
            }
        }
    }

    /* Add an extra 10% to the timeline. */
    unsigned long long offset_timepoint = largest * 1.1;
    /* Fit the range of the data into 4000 "ticks". */
    unsigned long long step = (unsigned long long)(offset_timepoint / GRID_SIZE);

    /* Dump the data in two formats. */
    bool human_readable = false;
    if (human_readable) {
        for (int i = 0; i < THREAD_COUNT; i ++) {
            printf("Cpu %d's values:\n", i);
            for (int j = 0; j < measurements[i] ; j++) {
                    printf("[%d,%llu], ",values[i][j],timepoints[i][j]);
            }
            printf("\n");
        }
    } else {
        for (int i = 0; i < THREAD_COUNT; i ++) {
            for (int j = 0; j < measurements[i] ; j++) {
                int stepped = (int)(timepoints[i][j] / step);
                printf("%d,%d",values[i][j],stepped);
                if (j < measurements[i] - 1) {
                    printf(",");
                }
            }
            printf("\n");
        }
    }
    return 0;
}
