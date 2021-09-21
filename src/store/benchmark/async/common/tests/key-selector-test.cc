#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "store/benchmark/async/common/uniform_key_selector.h"
#include "store/benchmark/async/common/zipf_key_selector.h"

void PrintBuckets(const std::vector<uint64_t> &counts, size_t m, size_t k) {
    uint64_t bucket = 0;
    std::vector<uint64_t> buckets;
    uint64_t max = 0;
    for (size_t i = 1; i < counts.size(); ++i) {
        bucket += counts[i];
        if (i % m == m - 1) {
            if (bucket > max) {
                max = bucket;
            }
            buckets.push_back(bucket);
            bucket = 0;
        }
    }

    for (size_t i = 0; i < buckets.size(); ++i) {
        std::cout << "|";
        for (size_t j = 0; j < k * static_cast<double>(buckets[i]) / max; ++j) {
            std::cout << "=";
        }
        std::cout << std::endl;
    }
}

TEST(UniformKeySelector, CorrectDistribution) {
    size_t k = 100;
    std::vector<std::string> keys;
    for (size_t i = 0; i < k; ++i) {
        keys.push_back(std::to_string(i));
    }
    UniformKeySelector uks(keys);
    std::mt19937 rand;

    std::vector<uint64_t> counts(keys.size(), 0UL);
    size_t n = 1000 * keys.size();
    for (size_t i = 0; i < n; ++i) {
        counts[uks.GetKey(rand)]++;
    }

    PrintBuckets(counts, 10, 80);

    double chisq = 0.0;
    double alpha = 0.05;
    size_t df = k - 1;
    for (size_t i = 0; i < counts.size(); ++i) {
        double expected = n / counts.size();
        chisq += (counts[i] - expected) * (counts[i] - expected) / expected;
    }
    double chisqcritical = 123.225;
    EXPECT_TRUE(chisq < chisqcritical);
}

// New Zipf selector requires strictly positive exponent
//
// TEST(ZipfKeySelector, CorrectUniformDistribution) {
//   size_t k = 100;
//   std::vector<std::string> keys;
//   for (size_t i = 0; i < k; ++i) {
//     keys.push_back(std::to_string(i));
//   }
//   ZipfKeySelector uks(keys, 0.0);
//   std::mt19937 rand;

//   std::vector<uint64_t> counts(keys.size(), 0UL);
//   size_t n = 1000 * keys.size();
//   for (size_t i = 0; i < n; ++i) {
//     counts[uks.GetKey(rand)]++;
//   }

//   PrintBuckets(counts, 10, 80);

//   double chisq = 0.0;
//   double alpha = 0.05;
//   size_t df = k - 1;
//   for (size_t i = 0; i < counts.size(); ++i) {
//     double expected = n / counts.size();
//     chisq += (counts[i] - expected) * (counts[i] - expected) / expected;
//   }
//   double chisqcritical = 123.225;
//   EXPECT_TRUE(chisq < chisqcritical);
// }

TEST(ZipfKeySelector, ZipfVisual1) {
    size_t k = 100;
    std::vector<std::string> keys;
    for (size_t i = 0; i < k; ++i) {
        keys.push_back(std::to_string(i));
    }
    ZipfKeySelector uks(keys, 0.25);
    std::mt19937 rand;

    std::vector<uint64_t> counts(keys.size(), 0UL);
    size_t n = 1000 * keys.size();
    for (size_t i = 0; i < n; ++i) {
        counts[uks.GetKey(rand)]++;
    }

    PrintBuckets(counts, 10, 80);
}

TEST(ZipfKeySelector, ZipfVisual2) {
    size_t k = 100;
    std::vector<std::string> keys;
    for (size_t i = 0; i < k; ++i) {
        keys.push_back(std::to_string(i));
    }
    ZipfKeySelector uks(keys, 0.5);
    std::mt19937 rand;

    std::vector<uint64_t> counts(keys.size(), 0UL);
    size_t n = 1000 * keys.size();
    for (size_t i = 0; i < n; ++i) {
        counts[uks.GetKey(rand)]++;
    }

    PrintBuckets(counts, 10, 80);
}

TEST(ZipfKeySelector, ZipfVisual3) {
    size_t k = 100;
    std::vector<std::string> keys;
    for (size_t i = 0; i < k; ++i) {
        keys.push_back(std::to_string(i));
    }
    ZipfKeySelector uks(keys, 0.75);
    std::mt19937 rand;

    std::vector<uint64_t> counts(keys.size(), 0UL);
    size_t n = 1000 * keys.size();
    for (size_t i = 0; i < n; ++i) {
        counts[uks.GetKey(rand)]++;
    }

    PrintBuckets(counts, 10, 80);
}

TEST(ZipfKeySelector, ZipfVisual4) {
    size_t k = 100;
    std::vector<std::string> keys;
    for (size_t i = 0; i < k; ++i) {
        keys.push_back(std::to_string(i));
    }
    ZipfKeySelector uks(keys, 0.9999);
    std::mt19937 rand;

    std::vector<uint64_t> counts(keys.size(), 0UL);
    size_t n = 1000 * keys.size();
    for (size_t i = 0; i < n; ++i) {
        counts[uks.GetKey(rand)]++;
    }

    PrintBuckets(counts, 10, 80);
}

TEST(ZipfKeySelector, ZipfVisual5) {
    size_t k = 100;
    std::vector<std::string> keys;
    for (size_t i = 0; i < k; ++i) {
        keys.push_back(std::to_string(i));
    }
    ZipfKeySelector uks(keys, 1.25);
    std::mt19937 rand;

    std::vector<uint64_t> counts(keys.size(), 0UL);
    size_t n = 1000 * keys.size();
    for (size_t i = 0; i < n; ++i) {
        counts[uks.GetKey(rand)]++;
    }

    PrintBuckets(counts, 10, 80);
}

TEST(ZipfKeySelector, ZipfVisual6) {
    size_t k = 100;
    std::vector<std::string> keys;
    for (size_t i = 0; i < k; ++i) {
        keys.push_back(std::to_string(i));
    }
    ZipfKeySelector uks(keys, 1.5);
    std::mt19937 rand;

    std::vector<uint64_t> counts(keys.size(), 0UL);
    size_t n = 1000 * keys.size();
    for (size_t i = 0; i < n; ++i) {
        counts[uks.GetKey(rand)]++;
    }

    PrintBuckets(counts, 10, 80);
}
