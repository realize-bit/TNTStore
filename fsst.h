#ifndef FSST_H
#define FSST_H 1

//struct filter_block {
//  /*
//  const FilterPolicy* policy_;
//  std::string keys_;             // Flattened key contents
//  std::vector<size_t> start_;    // Starting index in keys_ of each key
//  std::string result_;           // Filter data computed so far
//  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
//  std::vector<uint32_t> filter_offsets_;
//  */
//};

struct gc_context;
int get_fd_from_gtx(struct gc_context *gtx);
struct fsst_file *get_file_from_gtx(struct gc_context *gtx);
char *get_databuf_from_gtx(struct gc_context *gtx);
char *get_indexbuf_from_gtx(struct gc_context *gtx);
char *get_victbuf_from_gtx(struct gc_context *gtx);
struct io_context *get_io_context_from_gtx(struct gc_context *gtx);

struct fsst_file {
   int fd;
   uint64_t level;
   uint64_t seq;

   uint64_t largest;
   uint64_t smallest;

   uint64_t ioff_start;
   uint64_t file_size;
   uint64_t fsst_items;

   void *index_buf;
   void *page;
   struct fsst_file *sibling;
   struct fsst_file *child;
};

struct fsst_index {
   uint64_t key;
   uint64_t off;
   uint64_t sz;
   uint64_t PAD;

};


tree_entry_t *pick_garbage_node();
void read_item_async_from_fsst(struct slab_callback *callback);
void flush_batched_load(void);
void fsst_worker_init(void);
void sleep_until_fsstq_empty(void);
void cond_check_and_gc_wakeup(void);
void print_table(void);

#endif
