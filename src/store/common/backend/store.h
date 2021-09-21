#ifndef _STORE_H_
#define _STORE_H_

template<class K, class V>
class Store {
 public:
  Store();
  virtual ~Store();

  bool Get(const K &key, V &val) const;
  void Put(const K &key, const V &val);

 private:
  std::map<K, V> store;
};

template<class K, class V>
Store<K, V>::Store() {
}

template<class K, class V>
Store<K, V>::~Store() {
}

template<class K, class V>
bool Store<K, V>::Get(const K &key, V &val) const {
  auto itr = store.find(key);
  if (itr == store.end()) {
    return false;
  } else {
    val = itr->second;
    return true;
  }
}

template<class K, class V>
void Store<K, V>::Put(const K &key, const V &val) {
  store[key] = val;
}


#endif /* _STORE_H_ */
