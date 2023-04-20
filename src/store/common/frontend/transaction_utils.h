/***********************************************************************
 *
 * store/common/frontend/transaction_utils.h:
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
#ifndef TRANSACTION_UTILS_H
#define TRANSACTION_UTILS_H

#include <string>
#include <unordered_set>
#include <vector>

enum OperationType
{
    BEGIN_RW = 0,
    BEGIN_RO,
    GET,
    GET_FOR_UPDATE,
    PUT,
    COMMIT,
    ABORT,
    WAIT,
    ROCOMMIT,
};

struct Operation
{
    OperationType type;
    std::string key;
    std::string value;
    const std::unordered_set<std::string> keys;
};

Operation BeginRW();

Operation BeginRO();

Operation Wait();

Operation Get(const std::string &key);

Operation GetForUpdate(const std::string &key);

Operation Put(const std::string &key,
              const std::string &value);

Operation Commit();

Operation Abort();

Operation ROCommit(const std::unordered_set<std::string> &keys);

Operation ROCommit(const std::unordered_set<std::string> &&keys);

#endif
