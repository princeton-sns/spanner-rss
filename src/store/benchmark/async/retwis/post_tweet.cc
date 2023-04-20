/***********************************************************************
 *
 * store/benchmark/async/retwis/post_tweet.cc:
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
#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis
{

    PostTweet::PostTweet(KeySelector *keySelector, std::mt19937 &rand)
        : RetwisTransaction(keySelector, 5, rand, "post_tweet") {}

    PostTweet::~PostTweet()
    {
    }

    Operation PostTweet::GetNextOperation(std::size_t op_index)
    {
        Debug("POST_TWEET %lu", op_index);
        if (op_index == 0)
        {
            return BeginRW();
        }
        else if (op_index < 7)
        {
            int k = (op_index - 1) / 2;
            if (op_index % 2 == 0)
            {
                return Put(GetKey(k), GetKey(k));
            }
            else
            {
                return GetForUpdate(GetKey(k));
            }
        }
        else if (op_index == 7)
        {
            return Put(GetKey(3), GetKey(3));
        }
        else if (op_index == 8)
        {
            return Put(GetKey(4), GetKey(4));
        }
        else if (op_index == 9)
        {
            return Commit();
        }
        else
        {
            return Wait();
        }
    }

} // namespace retwis
