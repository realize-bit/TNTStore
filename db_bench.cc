#include "db_bench.h"

#include <iostream>
#include <random>
#include <cassert>
#include <chrono>

extern "C" {
static double read_random_exp_range_ = 0.0;

struct KeyrangeUnit {
  int64_t keyrange_start;
  int64_t keyrange_access;
  int64_t keyrange_keys;
};
// A good 64-bit random number generator based on std::mt19937_64
class Random64 {
 private:
  std::mt19937_64 generator_;

 public:
  explicit Random64(uint64_t s) : generator_(s) {}

  // Generates the next random number
  uint64_t Next() { return generator_(); }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint64_t Uniform(uint64_t n) {
    return std::uniform_int_distribution<uint64_t>(0, n - 1)(generator_);
  }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(uint64_t n) { return Uniform(n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint64_t Skewed(int max_log) {
    return Uniform(uint64_t(1) << Uniform(max_log + 1));
  }
};

// From our observations, the prefix hotness (key-range hotness) follows
// the two-term-exponential distribution: f(x) = a*exp(b*x) + c*exp(d*x).
// However, we cannot directly use the inverse function to decide a
// key-range from a random distribution. To achieve it, we create a list of
// KeyrangeUnit, each KeyrangeUnit occupies a range of integers whose size is
// decided based on the hotness of the key-range. When a random value is
// generated based on uniform distribution, we map it to the KeyrangeUnit Vec
// and one KeyrangeUnit is selected. The probability of a  KeyrangeUnit being
// selected is the same as the hotness of this KeyrangeUnit. After that, the
// key can be randomly allocated to the key-range of this KeyrangeUnit, or we
// can based on the power distribution (y=ax^b) to generate the offset of
// the key in the selected key-range. In this way, we generate the keyID
// based on the hotness of the prefix and also the key hotness distribution.
class GenerateTwoTermExpKeys {
 public:
  // Avoid uninitialized warning-as-error in some compilers
  int64_t keyrange_rand_max_ = 0;
  int64_t keyrange_size_ = 0;
  int64_t keyrange_num_ = 0;
  std::vector<KeyrangeUnit> keyrange_set_;

  // Initiate the KeyrangeUnit vector and calculate the size of each
  // KeyrangeUnit.
  int InitiateExpDistribution(int64_t total_keys, int range_num,
                              double prefix_a, double prefix_b, double prefix_c,
                              double prefix_d) {
    int64_t amplify = 0;
    int64_t keyrange_start = 0;
    if (range_num <= 0) {
      keyrange_num_ = 1;
    } else {
      keyrange_num_ = range_num;
    }
    keyrange_size_ = total_keys / keyrange_num_;

    // Calculate the key-range shares size based on the input parameters
    for (int64_t pfx = keyrange_num_; pfx >= 1; pfx--) {
      // Step 1. Calculate the probability that this key range will be
      // accessed in a query. It is based on the two-term expoential
      // distribution
      double keyrange_p = prefix_a * std::exp(prefix_b * pfx) +
                          prefix_c * std::exp(prefix_d * pfx);
      if (keyrange_p < std::pow(10.0, -16.0)) {
        keyrange_p = 0.0;
      }
      // Step 2. Calculate the amplify
      // In order to allocate a query to a key-range based on the random
      // number generated for this query, we need to extend the probability
      // of each key range from [0,1] to [0, amplify]. Amplify is calculated
      // by 1/(smallest key-range probability). In this way, we ensure that
      // all key-ranges are assigned with an Integer that  >=0
      if (amplify == 0 && keyrange_p > 0) {
        amplify = static_cast<int64_t>(std::floor(1 / keyrange_p)) + 1;
      }

      // Step 3. For each key-range, we calculate its position in the
      // [0, amplify] range, including the start, the size (keyrange_access)
      KeyrangeUnit p_unit;
      p_unit.keyrange_start = keyrange_start;
      if (0.0 >= keyrange_p) {
        p_unit.keyrange_access = 0;
      } else {
        p_unit.keyrange_access =
            static_cast<int64_t>(std::floor(amplify * keyrange_p));
      }
      p_unit.keyrange_keys = keyrange_size_;
      keyrange_set_.push_back(p_unit);
      keyrange_start += p_unit.keyrange_access;
      // std::cout << "start: " << keyrange_start << "\n";
    }
    keyrange_rand_max_ = keyrange_start;

    // Step 4. Shuffle the key-ranges randomly
    // Since the access probability is calculated from small to large,
    // If we do not re-allocate them, hot key-ranges are always at the end
    // and cold key-ranges are at the begin of the key space. Therefore, the
    // key-ranges are shuffled and the rand seed is only decide by the
    // key-range hotness distribution. With the same distribution parameters
    // the shuffle results are the same.
    Random64 rand_loca(keyrange_rand_max_);
    for (int64_t i = 0; i < range_num; i++) {
      int64_t pos = rand_loca.Next() % range_num;
      assert(i >= 0 && i < static_cast<int64_t>(keyrange_set_.size()) &&
             pos >= 0 && pos < static_cast<int64_t>(keyrange_set_.size()));
      std::swap(keyrange_set_[i], keyrange_set_[pos]);
    }

    // Step 5. Recalculate the prefix start postion after shuffling
    int64_t offset = 0;
    for (auto &p_unit : keyrange_set_) {
      p_unit.keyrange_start = offset;
      offset += p_unit.keyrange_access;
    }

    return 0;
  }

  // Generate the Key ID according to the input ini_rand and key distribution
  int64_t DistGetKeyID(int64_t ini_rand, double key_dist_a, double key_dist_b) {
    int64_t keyrange_rand = ini_rand % keyrange_rand_max_;

    // Calculate and select one key-range that contains the new key
    int64_t start = 0, end = static_cast<int64_t>(keyrange_set_.size());
    while (start + 1 < end) {
      int64_t mid = start + (end - start) / 2;
      assert(mid >= 0 && mid < static_cast<int64_t>(keyrange_set_.size()));
      if (keyrange_rand < keyrange_set_[mid].keyrange_start) {
        end = mid;
      } else {
        start = mid;
      }
    }
    int64_t keyrange_id = start;

    // Select one key in the key-range and compose the keyID
    int64_t key_offset = 0, key_seed;
    if (key_dist_a == 0.0 || key_dist_b == 0.0) {
      key_offset = ini_rand % keyrange_size_;
    } else {
      double u =
          static_cast<double>(ini_rand % keyrange_size_) / keyrange_size_;
      key_seed = static_cast<int64_t>(
          ceil(std::pow((u / key_dist_a), (1 / key_dist_b))));
      Random64 rand_key(key_seed);
      key_offset = rand_key.Next() % keyrange_size_;
    }
    // std::cout << "rand: " << keyrange_rand_max_ << "\n";
    return keyrange_size_ * keyrange_id + key_offset;
  }
};

static Random64 *my_rand;

void init_rand(void) {
  my_rand =
      new Random64(std::chrono::system_clock::now().time_since_epoch().count());
}

void *create_rand_seed(uint64_t s) {
  Random64 *rand = new Random64(s);
  return rand;
}

uint64_t rand_next_seed(uint64_t key_seed) {
  Random64 rand_key(key_seed);
  return rand_key.Next();
}

uint64_t rand_next(void) {
  // return static_cast<Random64 *> (rand)->Next();
  return my_rand->Next();
}

void *init_exp_prefix(int64_t total_keys, int range_num, double prefix_a,
                      double prefix_b, double prefix_c, double prefix_d) {
  GenerateTwoTermExpKeys *gen_exp = new GenerateTwoTermExpKeys();
  gen_exp->InitiateExpDistribution(total_keys, range_num, prefix_a, prefix_b,
                                   prefix_c, prefix_d);
  return gen_exp;
  // p = static_cast<void *>(gen_exp);
  // std::cout << p << "\n";
  // return;
}

int64_t prefix_get_key(void *gen_exp, int64_t ini_rand, double key_dist_a,
                       double key_dist_b) {
  return static_cast<GenerateTwoTermExpKeys *>(gen_exp)->DistGetKeyID(
      ini_rand, key_dist_a, key_dist_b);
}

int64_t GetRandomKey(void *r, int64_t num) {
  Random64 *rand = static_cast<Random64 *>(r);
  uint64_t rand_int = rand->Next();
  int64_t key_rand;
  if (read_random_exp_range_ == 0) {
    key_rand = rand_int % num;
  } else {
    const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
    long double order = -static_cast<long double>(rand_int % kBigInt) /
                        static_cast<long double>(kBigInt) *
                        read_random_exp_range_;
    long double exp_ran = std::exp(order);
    uint64_t rand_num =
        static_cast<int64_t>(exp_ran * static_cast<long double>(num));
    // Map to a different number to avoid locality.
    const uint64_t kBigPrime = 0x5bd1e995;
    // Overflow is like %(2^64). Will have little impact of results.
    key_rand = static_cast<int64_t>((rand_num * kBigPrime) % num);
  }
  return key_rand;
}

int64_t PowerCdfInversion(double u, double a, double b) {
  double ret;
  ret = std::pow((u / a), (1 / b));
  return static_cast<int64_t>(ceil(ret));
}
}
