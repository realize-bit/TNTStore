#ifndef OPTIONS_H
#define OPTIONS_H

#define DEBUG 0
#define PINNING 1
#define PATH "/scratch%lu/kvell/slab-%d-%lu-%lu"
#define FSST_PATH "/scratch0/kvell/fsst-%lu-%lu"

/* In memory structures */
#define RBTREE 0
#define BTREE 1
#define TNT 2

#define MEMORY_INDEX TNT
#define PAGECACHE_INDEX BTREE

/* Queue depth management */
#define QUEUE_DEPTH 64
// #define QUEUE_DEPTH 4
#define MAX_NB_PENDING_CALLBACKS_PER_WORKER (4*QUEUE_DEPTH)
#define NEVER_EXCEED_QUEUE_DEPTH 1 // Never submit more than QUEUE_DEPTH IO requests simultaneously, otherwise up to 2*MAX_NB_PENDING_CALLBACKS_PER_WORKER (very unlikely)
#define WAIT_A_BIT_FOR_MORE_IOS 0 // If we realize we don't have QUEUE_DEPTH IO pending when submitting IOs, check again if new incoming requests have arrived. Boost performance a tiny bit for zipfian workloads on AWS, but really not worthwhile

/* Page cache */
//#define PAGE_CACHE_SIZE (PAGE_SIZE * 20480)
//#define PAGE_CACHE_SIZE (PAGE_SIZE * 7864320) //30GB
#define PAGE_CACHE_SIZE (PAGE_SIZE * 3932160) //15GB
//#define PAGE_CACHE_SIZE (PAGE_SIZE * 1966080) //30GB
//#define PAGE_CACHE_SIZE (PAGE_SIZE * 2621440) //10GB
//#define PAGE_CACHE_SIZE (PAGE_SIZE * 786432) //3GB
#define MAX_PAGE_CACHE (PAGE_CACHE_SIZE / PAGE_SIZE)
#define MAX_SCAN_CACHE ((PAGE_CACHE_SIZE / PAGE_SIZE)/10)

/* Free list */
#define FREELIST_IN_MEMORY_ITEMS (256) // We need enough to never have to read from disk

#endif
