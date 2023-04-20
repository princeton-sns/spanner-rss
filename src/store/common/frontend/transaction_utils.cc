/***********************************************************************
 *
 * store/common/frontend/transaction_utils.cc:
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
#include "store/common/frontend/transaction_utils.h"

Operation BeginRW()
{
    return Operation{BEGIN_RW, "", ""};
}

Operation BeginRO()
{
    return Operation{BEGIN_RO, "", ""};
}

Operation Wait()
{
    return Operation{WAIT, "", ""};
}

Operation Get(const std::string &key)
{
    return Operation{GET, key, ""};
}

Operation GetForUpdate(const std::string &key)
{
    return Operation{GET_FOR_UPDATE, key, ""};
}

Operation Put(const std::string &key,
              const std::string &value)
{
    return Operation{PUT, key, value};
}

Operation Commit()
{
    return Operation{COMMIT, "", ""};
}

Operation ROCommit(const std::unordered_set<std::string> &keys)
{
    return Operation{ROCOMMIT, "", "", keys};
}

Operation ROCommit(const std::unordered_set<std::string> &&keys)
{
    return Operation{ROCOMMIT, "", "", keys};
}
