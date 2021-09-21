#ifndef PARTITIONER_H
#define PARTITIONER_H

#include <functional>
#include <set>
#include <string>
#include <random>
#include <vector>

enum partitioner_t {
  DEFAULT = 0,
  WAREHOUSE_DIST_ITEMS,
  WAREHOUSE,
};

class Partitioner {
 public:
  Partitioner() {}
  virtual ~Partitioner() {}
  virtual uint64_t operator()(const std::string &key, uint64_t numShards,
      int group, const std::vector<int> &txnGroups) = 0;
  uint64_t operator()(const std::string &key, uint64_t numShards,
      int group, const std::set<int> &txnGroups);
};

class DefaultPartitioner : public Partitioner {
 public:
  DefaultPartitioner() {}
  virtual ~DefaultPartitioner() {}

  virtual uint64_t operator()(const std::string &key, uint64_t numShards,
      int group, const std::vector<int> &txnGroups);
};

class WarehouseDistItemsPartitioner : public Partitioner {
 public:
  WarehouseDistItemsPartitioner(uint64_t numWarehouses) : numWarehouses(numWarehouses) {}
  virtual ~WarehouseDistItemsPartitioner() {}
  virtual uint64_t operator()(const std::string &key, uint64_t numShards,
      int group, const std::vector<int> &txnGroups);

 private:
  const uint64_t numWarehouses;
};

class WarehousePartitioner : public Partitioner {
 public:
  WarehousePartitioner(uint64_t numWarehouses, std::mt19937 &rd) :
      numWarehouses(numWarehouses), rd(rd) {}
  virtual ~WarehousePartitioner() {}

  virtual uint64_t operator()(const std::string &key, uint64_t numShards,
      int group, const std::vector<int> &txnGroups);

 private:
  const uint64_t numWarehouses;
  std::mt19937 &rd;
};



typedef std::function<uint64_t(const std::string &, uint64_t, int,
    const std::vector<int> &)> partitioner;

extern partitioner default_partitioner;
extern partitioner warehouse_partitioner;

partitioner warehouse_district_partitioner_dist_items(uint64_t num_warehouses);
partitioner warehouse_district_partitioner(uint64_t num_warehouses, std::mt19937 &rd);

#endif /* PARTITIONER_H */
