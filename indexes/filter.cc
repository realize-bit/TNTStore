#include "cuckoofilter.h"
#include "filter.h"

using cuckoofilter::CuckooFilter;

extern "C"
{
  filter_t *filter_create(size_t total_items) {
    CuckooFilter<size_t, 12> *filter = new CuckooFilter<size_t, 12>(total_items);
    return filter;
  }
  int filter_add(filter_t *f, unsigned char* k) {
      uint64_t hash = *(uint64_t*)k;
    CuckooFilter<size_t, 12> *i = static_cast<CuckooFilter<size_t, 12> *>(f);
    uint64_t err = i->Add(hash);
    if (err == cuckoofilter::Ok) 
      return 1;
    else
      std::cout<< err <<"\n";

    return 0;
  }
  int filter_contain(filter_t *f, unsigned char* k) {
      uint64_t hash = *(uint64_t*)k;
    CuckooFilter<size_t, 12> *i = static_cast<CuckooFilter<size_t, 12> *>(f);
    if (i->Contain(hash) == cuckoofilter::Ok) 
      return 1;
    return 0;
  }

  void filter_delete(filter_t *f) {
    delete(f);
  }
}
