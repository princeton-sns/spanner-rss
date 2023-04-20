/***********************************************************************
 *
 * store/common/backend/store.h:
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
#ifndef _STORE_H_
#define _STORE_H_

template <class K, class V>
class Store
{
public:
  Store();
  virtual ~Store();

  bool Get(const K &key, V &val) const;
  void Put(const K &key, const V &val);

private:
  std::map<K, V> store;
};

template <class K, class V>
Store<K, V>::Store()
{
}

template <class K, class V>
Store<K, V>::~Store()
{
}

template <class K, class V>
bool Store<K, V>::Get(const K &key, V &val) const
{
  auto itr = store.find(key);
  if (itr == store.end())
  {
    return false;
  }
  else
  {
    val = itr->second;
    return true;
  }
}

template <class K, class V>
void Store<K, V>::Put(const K &key, const V &val)
{
  store[key] = val;
}

#endif /* _STORE_H_ */
