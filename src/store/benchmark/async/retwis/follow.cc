/***********************************************************************
 *
 * store/benchmark/async/retwis/follow.cc:
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
#include "store/benchmark/async/retwis/follow.h"

namespace retwis
{

    Follow::Follow(KeySelector *keySelector, std::mt19937 &rand)
        : RetwisTransaction(keySelector, 2, rand, "follow") {}

    Follow::~Follow()
    {
    }

    Operation Follow::GetNextOperation(std::size_t op_index)
    {
        Debug("FOLLOW %lu", op_index);
        if (op_index == 0)
        {
            return BeginRW();
        }
        else if (op_index == 1)
        {
            return GetForUpdate(GetKey(0));
        }
        else if (op_index == 2)
        {
            return Put(GetKey(0), GetKey(0));
        }
        else if (op_index == 3)
        {
            return GetForUpdate(GetKey(1));
        }
        else if (op_index == 4)
        {
            return Put(GetKey(1), GetKey(1));
        }
        else if (op_index == 5)
        {
            return Commit();
        }
        else
        {
            return Wait();
        }
    }

} // namespace retwis
