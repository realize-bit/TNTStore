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

//struct dna_block {
//  // buffer: string
//  // restarts: vector
//  // counter: int
//  // finished: bool
//  // last_key: string
//  int size;
//  char buffer[];
//};
//
//struct cell {
//  int fd;
//  uint64_t offset;
//  struct dna_block data;
//  struct dna_block index;
//  // last_key;
//  uint64_t num_entries;
//  bool closed;
//  bool pending_index_entry;
//};


int make_fsst(void);
tree_entry_t *pick_garbage_node();
void flush_batched_load(void);
void fsst_worker_init(void);

#endif
