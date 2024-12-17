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
  subtree_t *t = (subtree_t*) malloc(sizeof(subtree_t));
  t->next = NULL;
  btree_map<uint64_t, uint64_t> *b =
      new btree_map<uint64_t, uint64_t>();
  t->tree = b;
  return t;
}

void subtree_set_slab(subtree_t *t, void *slab) {
  t->slab = slab;
  return;
}

void subtree_set_next(subtree_t *t, void *next) {
  t->next = next;
  return;
}

int subtree_find(subtree_t *t, unsigned char *k, size_t len,
                 struct index_entry *e) {
  // printf("# \tLookup Debug hash 2: %hhu\n", *k);
  uint64_t hash = *(uint64_t *)k;
  // printf("# \tLookup Debug hash 3: %lu\n", hash);
  btree_map<uint64_t, uint64_t> *b =
      static_cast<btree_map<uint64_t, uint64_t> *>(t->tree);
  auto i = b->find(hash);
  if (i != b->end()) {
    index_entry_t cur = {{(struct slab*)t->slab}, {(uint64_t)i->second}};
    *e = cur;
    return 1;
  } else {
    return 0;
  }
}
int subtree_set_invalid(subtree_t *t, unsigned char *k, size_t len) {
  uint64_t hash = *(uint64_t *)k;
  btree_map<uint64_t, uint64_t> *b =
      static_cast<btree_map<uint64_t, uint64_t> *>(t->tree);
  auto i = b->find(hash);
  if (i != b->end()) {
    if (!tas_inval(&i->second)) {
      // printf("SET INVAL %lu (s, %lu)\n", hash, i->second.slab_idx);
      set_inval(&i->second);
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
  btree_map<uint64_t, uint64_t> *b =
      static_cast<btree_map<uint64_t, uint64_t> *>(t->tree);
  return b->erase(hash);
}

void subtree_insert(subtree_t *t, unsigned char *k, size_t len,
                    struct index_entry *e) {
  uint64_t hash = *(uint64_t *)k;
  btree_map<uint64_t, uint64_t> *b =
      static_cast<btree_map<uint64_t, uint64_t> *>(t->tree);
  b->insert(make_pair(hash, e->slab_idx));
}

int subtree_forall_keys(subtree_t *t, void (*cb)(uint64_t h, int n, void *data),
                         void *data) {
  btree_map<uint64_t, uint64_t> *b =
      static_cast<btree_map<uint64_t, uint64_t> *>(t->tree);
  int n = 0;
  auto i = b->begin();
  while (i != b->end()) {
    cb(i->first, n++, data);
    i++;
  }
  return n;
}
int subtree_forall_invalid(subtree_t *t, void *data, void (*cb)(void *slab, uint64_t slab_idx)) {
  btree_map<uint64_t, uint64_t> *b =
      static_cast<btree_map<uint64_t, uint64_t> *>(t->tree);
  int count = 0;
  auto i = b->begin();
  while (i != b->end()) {
    if (!test_inval(&i->second)) {
      // 여기서 inval을 하면 더 빨리 tree free 가능
      // 문제가 생기느냐? tree가 free되는 순간이 온다면
      // 트리가 더이상 필요하지 않을 것 따라서 문제 없음
      // set_inval(&i->second.slab_idx);
      // printf("FSST: %lu ", i->first);
      cb(data, i->second);
      count++;
    }
    // else
    // printf("INVAL FSST: %lu\n", i->first);
    i++;
  }
  return count;
}


void subtree_free(subtree_t *t) {
  btree_map<uint64_t, uint64_t> *b =
      static_cast<btree_map<uint64_t, uint64_t> *>(t->tree);
  delete b;
  free(t);
}
void subtree_all_free(subtree_t *t) {
  btree_map<uint64_t, uint64_t> *b =
      static_cast<btree_map<uint64_t, uint64_t> *>(t->tree);
  b->erase(b->begin(), b->end());
  delete b;
  free(t);
}
}
