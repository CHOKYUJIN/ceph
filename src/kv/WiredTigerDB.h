// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef WIRED_DB_STORE_H
#define WIRED_DB_STORE_H

#include "include/types.h"
#include "include/buffer_fwd.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <string>
#include <memory>
#include <errno.h>
#include <chrono>
#include "common/errno.h"
#include "common/dout.h"
#include "include/ceph_assert.h"
#include "include/common_fwd.h"
#include "common/Formatter.h"
#include "common/Cond.h"
#include "common/ceph_context.h"
#include "common/PriorityCache.h"
#include "common/pretty_binary.h"
#include "wiredtiger.h"

enum {
  l_wiredtiger_first = 34300,
  l_wiredtiger_gets,
  l_wiredtiger_txns,
  l_wiredtiger_get_latency,
  l_wiredtiger_submit_latency,
  l_wiredtiger_submit_sync_latency,
  l_wiredtiger_last,
};

#define KEY_DELIMETER '\57' // '/' in ascii
#define TABLE_NAME "table:db"
#define MAX_RETRY_MS_TIME 120000 // 2 minutes

/**
 * Uses WiredTiger to implement the KeyValueDB interface
 */
class WiredTigerDB : public KeyValueDB {
  public:
  CephContext *cct;
  PerfCounters *logger;
  std::string path;
  std::map<std::string,std::string> kv_options;
  void *priv;
  std::string options_str;

  // wiredtiger 
  WT_CONNECTION *conn;

  // For get()
  WT_SESSION *first_session;


  WiredTigerDB(CephContext *c, const std::string &path, std::map<std::string,std::string> opt, void *p) :
    cct(c),
    logger(NULL),
    path(path),
    kv_options(opt),
    priv(p),
    conn(nullptr),
    first_session(nullptr)
  {}

  ~WiredTigerDB() override;

  int init(std::string options_str) override;
  static int _test_init(const std::string& dir);

  int do_open(std::ostream &out,
			  bool create_if_missing,
			  bool open_readonly,
			  const std::string& sharding_text);

  /// Opens underlying db
  int open(std::ostream &out, const std::string& cfs="") override;

  /// Creates underlying db if missing and opens it
  int create_and_open(std::ostream &out, const std::string& cfs="") override;

  int open_read_only(std::ostream &out, const std::string& cfs="") override; 

  void close() override;

  int repair(std::ostream &out) override;

  PerfCounters *get_perf_counters() override
  {
    return logger;
  }

  class WiredTigerDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    WiredTigerDB *db;

    WT_SESSION *trx_session;
    WT_CURSOR *trx_cursor;

    explicit WiredTigerDBTransactionImpl(WiredTigerDB *_db);
    ~WiredTigerDBTransactionImpl() override;

    void set(
      const std::string &prefix,
      const std::string &k,
      const ceph::bufferlist &bl) override;
    void set(
      const std::string &prefix,
      const char *k,
      size_t keylen,
      const ceph::bufferlist &bl) override;
    void rmkey(
      const std::string &prefix,
      const std::string &k) override;
    void rmkey(
      const std::string &prefix,   ///< [in] Prefix to search for
      const char *k,	      ///< [in] Key to remove
      size_t keylen) override;
    void rmkeys_by_prefix(
      const std::string &prefix
      ) override;
    void rm_range_keys(
      const std::string &prefix,
      const std::string &start,
      const std::string &end) override;
    void merge(const std::string &prefix, const std::string &key,
	    const ceph::bufferlist  &value) override;
  };

  KeyValueDB::Transaction get_transaction() override {
    return std::make_shared<WiredTigerDBTransactionImpl>(this);
  }

  int submit_transaction(KeyValueDB::Transaction t) override;

  int get(
    const std::string &prefix,
    const std::set<std::string> &key,
    std::map<std::string, ceph::bufferlist> *out
    ) override;
  int get(
    const std::string &prefix,
    const std::string &key,
    ceph::bufferlist *out
    ) override;
  int get(
    const std::string &prefix,
    const char *key,
    size_t keylen,
    ceph::bufferlist *out) override;


  class WiredTigerDBWholeSpaceIteratorImpl : public KeyValueDB::WholeSpaceIteratorImpl {
    WiredTigerDB *db;
    WT_CURSOR *cursor;
  public:
    explicit WiredTigerDBWholeSpaceIteratorImpl(const WiredTigerDB* db, const KeyValueDB::IteratorOpts opts) : db(const_cast<WiredTigerDB*>(db)) {
      WT_CURSOR *temp_cursor;
      int r = db->first_session->open_cursor(db->first_session, TABLE_NAME, NULL, NULL, &temp_cursor);
      if (r != 0) {
        ceph_abort();
      }
      this->cursor = temp_cursor;
    }
    ~WiredTigerDBWholeSpaceIteratorImpl() override;

    int seek_to_first() override;
    int seek_to_first(const std::string &prefix) override;
    int seek_to_last() override;
    int seek_to_last(const std::string &prefix) override;
    int upper_bound(const std::string &prefix, const std::string &after) override;
    int lower_bound(const std::string &prefix, const std::string &to) override;
    bool valid() override;
    int next() override;
    int prev() override;
    std::string key() override;
    std::pair<std::string,std::string> raw_key() override;
    bool raw_key_is_prefixed(const std::string &prefix) override;
    ceph::bufferlist value() override;
    ceph::bufferptr value_as_ptr() override;
    int status() override;
    size_t key_size() override;
    size_t value_size() override;
  };

  WholeSpaceIterator get_wholespace_iterator(IteratorOpts opts = 0) override;

  /// Utility
  static std::string combine_strings(const std::string &prefix, const std::string &value) {
    std::string out = prefix;
    out.push_back(KEY_DELIMETER);
    out.append(value);
    return out;
  }

  static void combine_strings(const std::string &prefix,
			      const char *key, size_t keylen,
			      std::string *out) {
    out->reserve(prefix.size() + 1 + keylen);
    *out = prefix;
    out->push_back(KEY_DELIMETER);
    out->append(key, keylen);
  }

  static int split_key(std::string &in, std::string *prefix, std::string *key);

  uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) override {
      return -EOPNOTSUPP;
  }

  virtual int64_t get_cache_usage() const override {
    return -EOPNOTSUPP;
  }

  virtual int64_t get_cache_usage(std::string prefix) const override {
    return -EOPNOTSUPP;
  }

  int set_cache_size(uint64_t s) override {
    return -EOPNOTSUPP;
  }

  virtual std::shared_ptr<PriorityCache::PriCache>
      get_priority_cache() const override {
    return nullptr;
  }

  virtual std::shared_ptr<PriorityCache::PriCache>
      get_priority_cache(std::string prefix) const override {
    return nullptr;
  }

  int set_merge_operator(const std::string& prefix,
         std::shared_ptr<MergeOperator> mop) override;

  std::shared_ptr<MergeOperator> _find_merge_op(const std::string &prefix);
  
  void dump_db();
  void hex_dump(const void *value, size_t size);
};

#endif
