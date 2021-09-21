#include "store/common/stats.h"

#include "lib/assert.h"

#include <algorithm>
#include <fstream>
#include <iostream>

Stats::Stats() {
}

Stats::~Stats() {
}

void Stats::Increment(const std::string &key, int amount) {
  std::lock_guard<std::mutex> lock(mtx);
  statInts[key] += amount;
}

void Stats::IncrementList(const std::string &key, size_t idx, int amount) {
  std::lock_guard<std::mutex> lock(mtx);
  if (statIncLists[key].size() <= idx) {
    statIncLists[key].resize(idx + 1);
  }
  statIncLists[key][idx] += amount;
}

void Stats::Add(const std::string &key, int64_t value) {
  std::lock_guard<std::mutex> lock(mtx);
  statLists[key].push_back(value);
}

void Stats::AddList(const std::string &key, size_t idx, uint64_t value) {
  std::lock_guard<std::mutex> lock(mtx);
  if (statLoLs[key].size() <= idx) {
    statLoLs[key].resize(idx + 1);
  }
  statLoLs[key][idx].push_back(value);
}

void Stats::ExportJSON(std::ostream &os) {
  std::lock_guard<std::mutex> lock(mtx);
  os << "{" << std::endl;
  for (auto itr = statInts.begin(); itr != statInts.end(); ++itr) {
    os << "    \"" << itr->first << "\": " << itr->second;
    if (std::next(itr) != statInts.end() || statLists.size() > 0 ||
        statIncLists.size() > 0 || statLoLs.size() > 0) {
      os << ",";
    }
    os << std::endl;
  }
  for (auto itr = statLists.begin(); itr != statLists.end(); ++itr) {
    Debug("Writing stat list %s of length %lu.", itr->first.c_str(),
        itr->second.size());
    os << "    \"" << itr->first << "\": [";
    for (auto jtr = itr->second.begin(); jtr != itr->second.end(); ++jtr) {
      os << *jtr;
      if (std::next(jtr) != itr->second.end()) {
        os << ", ";
      }
    }
    os << "]";
    if (std::next(itr) != statLists.end() || statIncLists.size() > 0 ||
        statLoLs.size() > 0) {
      os << ",";
    }
    os << std::endl;
  }
  for (auto itr = statIncLists.begin(); itr != statIncLists.end(); ++itr) {
    Debug("Writing stat list %s of length %lu.", itr->first.c_str(),
        itr->second.size());
    os << "    \"" << itr->first << "\": [";
    for (auto jtr = itr->second.begin(); jtr != itr->second.end(); ++jtr) {
      os << *jtr;
      if (std::next(jtr) != itr->second.end()) {
        os << ", ";
      }
    }
    os << "]";
    if (std::next(itr) != statIncLists.end() || statLoLs.size() > 0) {
      os << ",";
    }
    os << std::endl;
  }
  /*for (auto itr = statLoLs.begin(); itr != statLoLs.end(); ++itr) {
    os << "    \"" << itr->first << "\": [" << std::endl;
    for (auto jtr = itr->second.begin(); jtr != itr->second.end(); ++jtr) {
      os << "        [";
      for (auto ktr = jtr->begin(); ktr != jtr->end(); ++ktr) {
        os << *ktr;
        if (std::next(ktr) != jtr->end()) {
          os << ", ";
        }
      }
      os << "]";
      if (std::next(jtr) != itr->second.end()) {
        os << ",";
      }
      os << std::endl;
    }
    os << "]";
    if (std::next(itr) != statLoLs.end()) {
      os << ",";
    }
    os << std::endl;
  }*/
  os << "}" << std::endl;
}

void Stats::ExportJSON(const std::string &file) {
  std::ofstream ofs(file);
  if (!ofs.fail()) {
    ExportJSON(ofs);
    ofs.close();
  }
}

void Stats::Merge(const Stats &other) {
  std::lock_guard<std::mutex> lock(mtx);
  for (const auto &s : other.statInts) {
    statInts[s.first] += s.second;
  }
  for (const auto &l : other.statIncLists) {
    if (statIncLists[l.first].size() < l.second.size()) {
      statIncLists[l.first].resize(l.second.size());
    }
    for (size_t i = 0; i < l.second.size(); ++i) {
      statIncLists[l.first][i] += l.second[i];
    }
  }
  for (const auto &l : other.statLists) {
    statLists[l.first].insert(statLists[l.first].end(), l.second.begin(),
        l.second.end());
  }
  for (const auto &lol : other.statLoLs) {
    statLoLs[lol.first].insert(statLoLs[lol.first].end(), lol.second.begin(),
        lol.second.end());
  }
}
