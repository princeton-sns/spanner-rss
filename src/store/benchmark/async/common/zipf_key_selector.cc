#include "store/benchmark/async/common/zipf_key_selector.h"

#include <cmath>
#include <iostream>

ZipfKeySelector::ZipfKeySelector(const std::vector<std::string> &keys,
                                 double exponent)
    : KeySelector(keys),
      num_elements_{static_cast<int>(keys.size())},
      exponent_{exponent},
      h_integral_x1_{h_integral(1.5) - 1},
      h_integral_num_elements_{h_integral(num_elements_ + F_1_2)},
      s_{2 - h_integral_inv(h_integral(2.5) - h(2))},
      dis_{0.0, 1.0} {
    if (keys.size() <= 0) {
        throw std::invalid_argument(
            "number of elements is not strictly positive: " +
            std::to_string(keys.size()));
    }

    if (exponent <= 0) {
        throw std::invalid_argument("exponent is not strictly positive: " +
                                    std::to_string(exponent));
    }
}

int ZipfKeySelector::GetKey(std::mt19937 &rand) {
    while (true) {
        const double u = h_integral_num_elements_ +
                         U(rand) * (h_integral_x1_ - h_integral_num_elements_);
        // u is uniformly distributed in (h_integral_x1_,
        // h_integral_num_elements_]

        const double x = h_integral_inv(u);
        int k = static_cast<int>(x + F_1_2);

        // Limit k to the range [1, num_elements_] if it would be outside
        // due to numerical inaccuracies.
        if (k < 1) {
            k = 1;
        } else if (k > num_elements_) {
            k = num_elements_;
        }

        // Here, the distribution of k is given by:
        //
        //   P(k = 1) = C * (h_integral(1.5) - h_integral_x1_) = C
        //   P(k = m) = C * (h_integral(m + 1/2) - h_integral(m - 1/2)) for
        //   m >=
        //   2
        //
        //   where C = 1 / (h_integral_num_elements_ - h_integral_x1_)

        if (k - x <= s_ || u >= h_integral(k + F_1_2) - h(k)) {
            // Case k = 1:
            //
            //   The right inequality is always true, because replacing k by
            //   1 gives u >= h_integral(1.5) - h(1) = h_integral_x1_ and u
            //   is taken from (h_integral_x1_, h_integral_num_elements_].
            //
            //   Therefore, the acceptance rate for k = 1 is P(accepted | k
            //   = 1) = 1 and the probability that 1 is returned as random
            //   value is P(k = 1 and accepted) = P(accepted | k = 1) * P(k
            //   = 1) = C = C / 1^exponent_
            //
            // Case k >= 2:
            //
            //   The left inequality (k - x <= s_) is just a short cut
            //   to avoid the more expensive evaluation of the right
            //   inequality (u >= h_integral(k + 0.5) - h(k)) in many cases.
            //
            //   If the left inequality is true, the right inequality is
            //   also true:
            //     Theorem 2 in the paper is valid for all positive
            //     exponents, because the requirements h'(x) =
            //     -exponent_/x^(exponent_ + 1) < 0 and
            //     (-1/hInverse'(x))'' = (1+1/exponent_) * x^(1/exponent_-1)
            //     >= 0 are both fulfilled. Therefore, f(x) = x -
            //     h_integral_inv(h_integral(x + 0.5) - h(x)) is a
            //     non-decreasing function. If k - x <= s_ holds, k - x <=
            //     s_ + f(k) - f(2) is obviously also true which is
            //     equivalent to -x
            //     <= -h_integral_inv(h_integral(k + 0.5) - h(k)),
            //     -h_integral_inv(u) <= -h_integral_inv(h_integral(k + 0.5)
            //     - h(k)), and finally u >= h_integral(k + 0.5) - h(k).
            //
            //   Hence, the right inequality determines the acceptance rate:
            //   P(accepted | k = m) = h(m) / (hIntegrated(m+1/2) -
            //   hIntegrated(m-1/2)) The probability that m is returned is
            //   given by P(k = m and accepted) = P(accepted | k = m) * P(k
            //   = m) = C * h(m) = C / m^exponent_.
            //
            // In both cases the probabilities are proportional to the
            // probability mass function of the Zipf distribution.

            return k - 1;
        }
    }
}

double ZipfKeySelector::U(std::mt19937 &rand) { return dis_(rand); }

double ZipfKeySelector::h_integral(const double x) {
    const double logX = std::log(x);
    return helper2((1 - exponent_) * logX) * logX;
}

double ZipfKeySelector::h(const double x) {
    return std::exp(-exponent_ * std::log(x));
}

double ZipfKeySelector::h_integral_inv(const double x) {
    double t = x * (1 - exponent_);
    if (t < -1) {
        // Limit value to the range [-1, +inf).
        // t could be smaller than -1 in some rare cases due to numerical
        // errors.
        t = -1;
    }
    return std::exp(helper1(t) * x);
}

inline double ZipfKeySelector::helper1(const double x) {
    if (std::abs(x) > TAYLOR_THRESHOLD) {
        return std::log1p(x) / x;
    } else {
        return 1 - x * (F_1_2 - x * (F_1_3 - F_1_4 * x));
    }
}

inline double ZipfKeySelector::helper2(const double x) {
    if (std::abs(x) > TAYLOR_THRESHOLD) {
        return std::expm1(x) / x;
    } else {
        return 1 + x * F_1_2 * (1 + x * F_1_3 * (1 + F_1_4 * x));
    }
}
