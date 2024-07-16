#include "cpp-btree/btree_map.h"
#include "tnt_subtree.h"

using namespace std;
using namespace btree;

extern "C" {
static inline void set_inval(size_t *addr) {
  asm("btsl %1,%0" : "+m"(*(size_t *)addr) : "Ir"(63));
}
static inline int tas_inval(size_t *addr) {
  int oldbit;
  asm("lock; btsl %2,%1\n\tsbbl %0,%0" : "=r"(oldbit), "=m"(*addr) : "r"(63));
  return oldbit;
}
static inline unsigned char test_inval(size_t *addr) {
  unsigned char v;
  const size_t *p = addr;
  asm("btl %2,%1; setc %0" : "=qm"(v) : "m"(*p), "Ir"(63));
  return v;
}
subtree_t *subtree_create() {
  btree_map<uint64_t, struct index_entry> *b =
      new btree_map<uint64_t, struct index_entry>();
  return b;
}

int subtree_find(subtree_t *t, unsigned char *k, size_t len,
                 struct index_entry *e) {
  // printf("# \tLookup Debug hash 2: %hhu\n", *k);
  uint64_t hash = *(uint64_t *)k;
  // printf("# \tLookup Debug hash 3: %lu\n", hash);
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  auto i = b->find(hash);
  if (i != b->end()) {
    *e = i->second;
    return 1;
  } else {
    return 0;
  }
}
int subtree_set_invalid(subtree_t *t, unsigned char *k, size_t len) {
  uint64_t hash = *(uint64_t *)k;
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  auto i = b->find(hash);
  if (i != b->end()) {
    if (!tas_inval(&i->second.slab_idx)) {
      // printf("SET INVAL %lu (s, %lu)\n", hash, i->second.slab_idx);
      set_inval(&i->second.slab_idx);
      return 1;
    }
    // printf("Already INVAL %lu(s, %lu)\n", hash, i->second.slab_idx);
    return 0;
  } else {
    return 0;
  }
}

int subtree_delete(subtree_t *t, unsigned char *k, size_t len) {
  uint64_t hash = *(uint64_t *)k;
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  return b->erase(hash);
}

void subtree_insert(subtree_t *t, unsigned char *k, size_t len,
                    struct index_entry *e) {
  uint64_t hash = *(uint64_t *)k;
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  b->insert(make_pair(hash, *e));
}

struct index_scan subtree_find_n(subtree_t *t, unsigned char *k, size_t len,
                                 size_t n) {
  struct index_scan res;
  res.hashes = (uint64_t *)malloc(n * sizeof(*res.hashes));
  res.entries = (struct index_entry *)malloc(n * sizeof(*res.entries));
  res.nb_entries = 0;

  uint64_t hash = *(uint64_t *)k;
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  auto i = b->find_closest(hash);
  while (i != b->end() && res.nb_entries < n) {
    res.hashes[res.nb_entries] = i->first;
    res.entries[res.nb_entries] = i->second;
    res.nb_entries++;
    i++;
  }

  return res;
}
void subtree_allvalid_key(subtree_t *t, struct index_scan *res) {
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  auto i = b->begin();

  while (i != b->end()) {
    if (!test_inval(&i->second.slab_idx)) {
      res->hashes[res->nb_entries] = i->first;
      res->entries[res->nb_entries] = i->second;
      res->nb_entries++;
    }
    i++;
  }

  return;
}

void subtree_forall_keys(subtree_t *t, void (*cb)(uint64_t h, void *data),
                         void *data) {
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  auto i = b->begin();
  while (i != b->end()) {
    cb(i->first, data);
    i++;
  }
  return;
}
int subtree_forall_invalid(subtree_t *t, void (*cb)(void *data)) {
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  int count = 0;
  auto i = b->begin();
  while (i != b->end()) {
    if (!test_inval(&i->second.slab_idx)) {
      // 여기서 inval을 하면 더 빨리 tree free 가능
      // 문제가 생기느냐? tree가 free되는 순간이 온다면
      // 트리가 더이상 필요하지 않을 것 따라서 문제 없음
      // set_inval(&i->second.slab_idx);
      // printf("FSST: %lu ", i->first);
      cb((void *)&i->second);
      count++;
    }
    // else
    // printf("INVAL FSST: %lu\n", i->first);
    i++;
  }
  return count;
}
uint64_t subtree_next_key(subtree_t *t, unsigned char *k,
                          struct index_entry *e) {
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  uint64_t hash;
  auto i = b->begin();

  if (k != NULL) {
    hash = *(uint64_t *)k;
    i = b->find(hash);
  }

  while (i != b->end() && test_inval(&i->second.slab_idx)) i++;

  if (i == b->end()) return -1;

  if (!test_inval(&i->second.slab_idx)) *e = i->second;

  return i->first;
}

void subtree_free(subtree_t *t) {
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  delete b;
}
void subtree_all_free(subtree_t *t) {
  btree_map<uint64_t, struct index_entry> *b =
      static_cast<btree_map<uint64_t, struct index_entry> *>(t);
  b->erase(b->begin(), b->end());
  delete b;
}
}
