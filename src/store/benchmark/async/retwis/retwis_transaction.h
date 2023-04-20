/***********************************************************************
 *
 * store/benchmark/async/retwis/retwis_transaction.h:
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef RETWIS_TRANSACTION_H
#define RETWIS_TRANSACTION_H

#include <random>
#include <string>
#include <vector>

#include "store/benchmark/async/common/key_selector.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

namespace retwis
{

    class RetwisTransaction : public AsyncTransaction
    {
    public:
        RetwisTransaction(KeySelector *keySelector, int numKeys, std::mt19937 &rand, const std::string ttype);
        virtual ~RetwisTransaction();

    protected:
        inline const std::string &GetKey(int i) const
        {
            return keySelector->GetKey(keyIdxs[i]);
        }

        inline size_t GetNumKeys() const { return keyIdxs.size(); }

        const std::string &GetTransactionType() override { return ttype_; };

        KeySelector *keySelector;

    private:
        std::vector<int> keyIdxs;
        std::string ttype_;
    };

} // namespace retwis

#endif /* RETWIS_TRANSACTION_H */
