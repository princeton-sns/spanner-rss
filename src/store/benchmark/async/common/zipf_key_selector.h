/**
 * Adapted from YCSB Java implementation.
 *
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights
 * reserved. <p> Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

#ifndef ZIPF_KEY_SELECTOR_H
#define ZIPF_KEY_SELECTOR_H

#include <mutex>

#include "store/benchmark/async/common/key_selector.h"

/**
 * Generates random integers according to a Zipf distribution.
 *
 * This implementation is a port of the RejectionInversionZipfSampler class
 * from the Apache Commons library, which is an implementation of the method
 * described in:
 *
 * Wolfgang Hörmann and Gerhard Derflinger.
 * "Rejection-inversion to generate variates from monotone discrete
 * distributions," ACM Transactions on Modeling and Computer Simulation
 * (TOMACS) 6.3 (1996): 169-184.
 *
 * Explanation from the Apache implementation:
 * The paper describes an algorithm for exponents larger than 1 (Algorithm ZRI).
 * The original method uses H(x) = (v + x)^(1 - q) / (1 - q)
 * as the integral of the hat function.
 *
 * This function is undefined for q = 1, which is the reason for the limitation
 * of the exponent. If instead the integral function H(x) = ((v + x)^(1 - q) -
 * 1) / (1 - q) is used, for which a meaningful limit exists for q = 1, the
 * method works for all positive exponents. The following implementation uses v
 * = 0 and generates integral number in the range [1, num_elements]. This is
 * different to the original method where v is defined to be positive and
 * numbers are taken from [0, i_max]. This explains why the implementation looks
 * slightly different.
 */
class ZipfKeySelector : public KeySelector {
   public:
    ZipfKeySelector(const std::vector<std::string> &keys, double exponent);

    int GetKey(std::mt19937 &rand) override;

   private:
    /** Threshold below which Taylor series will be used. */
    constexpr static double TAYLOR_THRESHOLD = 1e-8;
    /** 1/2. */
    constexpr static double F_1_2 = 0.5;
    /** 1/3. */
    constexpr static double F_1_3 = 1.0 / 3;
    /** 1/4. */
    constexpr static double F_1_4 = 0.25;
    /** Number of elements. */
    const int num_elements_;
    /** Exponent parameter of the distribution. */
    const double exponent_;
    /** h_integral(1.5) - 1. */
    const double h_integral_x1_;
    /** h_integral(num_elements_ + 0.5). */
    const double h_integral_num_elements_;
    /** 2 - h_integral_inv(h_integral(2.5) - h(2). */
    const double s_;
    /** Uniform generator */
    std::uniform_real_distribution<> dis_;

    static inline double helper1(const double x);
    static inline double helper2(const double x);

    double U(std::mt19937 &rand);
    double h_integral(const double x);
    double h(const double x);
    double h_integral_inv(const double x);
};

#endif /* ZIPF_KEY_SELECTOR_H */
