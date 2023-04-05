// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "common/perf_counters.h"
#include "common/PriorityCache.h"
#include "include/common_fwd.h"
#include "include/scope_guard.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "include/str_map.h"
#include "KeyValueDB.h"
#include "WiredTigerDB.h"
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_wiredtiger
#undef dout_prefix
#define dout_prefix *_dout << "wiredtiger: "
#define dtrace dout(30)
#define dwarn dout(0)
#define dinfo dout(0)

namespace fs = std::filesystem;

using std::function;
using std::list;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::Formatter;

WiredTigerDB::~WiredTigerDB()
{
  close();
}

int WiredTigerDB::init(string _options_str)
{
  options_str = _options_str;
  if(options_str.length()) {
    // TODO: parse 
    ;
  }
  return 0;
}

int WiredTigerDB::_test_init(const string& dir)
{
  return 0;
}

int WiredTigerDB::open(std::ostream &out, const std::string& cfs) 
{
  if (!cfs.empty()) {
    ceph_abort_msg("Not implemented");
  }
  return do_open(out, false, false, cfs);
}

int WiredTigerDB::create_and_open(ostream &out, const std::string& cfs)
{
  dinfo << __func__ << " is trying to open " << dendl;
  if (!cfs.empty()) {
    ceph_abort_msg("Not implemented");
  }
  return do_open(out, true, false, cfs);
}

int WiredTigerDB::open_read_only(std::ostream &out, const std::string& cfs) {
    if (!cfs.empty()) {
      ceph_abort_msg("Not implemented");
    }
    return do_open(out, false, true, cfs);
}


int WiredTigerDB::do_open(ostream &out,
			  bool create_if_missing,
			  bool open_readonly,
			  const std::string& sharding_text)
{
  // TODO: parse sharding_text
  // TODO: handle create_if_missing and open_readonly
  // Open a connection to the database, creating it if necessary.
  dinfo << __func__ << " is trying to open " << path.c_str() << dendl;

  int r = wiredtiger_open(path.c_str(), NULL, "create,cache_size=5GB,log=(enabled,recover=on),statistics=(all)", &conn);
  if(r) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
  }

  // Open a session handle for the database.
  conn->open_session(conn, NULL, NULL, &session);
  session->create(session, TABLE_NAME, "key_format=u,value_format=u");
  session->open_cursor(session, TABLE_NAME, NULL, NULL, &cursor);

  // Build Perf Counter
  PerfCountersBuilder plb(g_ceph_context, "wiredtiger", l_wiredtiger_first, l_wiredtiger_last);
  plb.add_u64_counter(l_wiredtiger_gets, "wiredtiger_get", "Gets");
  plb.add_u64_counter(l_wiredtiger_txns, "wiredtiger_transaction", "Transactions");
  plb.add_time_avg(l_wiredtiger_get_latency, "wiredtiger_get_latency", "Get Latency");
  plb.add_time_avg(l_wiredtiger_submit_latency, "wiredtiger_submit_latency", "Submit Latency");
  plb.add_time_avg(l_wiredtiger_submit_sync_latency, "wiredtiger_submit_sync_latency", "Submit Sync Latency");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);

  return 0;
}

void WiredTigerDB::close()
{
  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = nullptr;
  }

  cursor->close(cursor);
  session->close(session, NULL);
  conn->close(conn, NULL);
}

int WiredTigerDB::repair(std::ostream &out)
{
  // should close and re-open the database.
  cursor->close(cursor);
  session->close(session, NULL);
  conn->close(conn, NULL);

  conn->open_session(conn, NULL, NULL, &session);
  session->create(session, TABLE_NAME, "key_format=u,value_format=u");
  return 0;
}

WiredTigerDB::WiredTigerDBTransactionImpl::WiredTigerDBTransactionImpl(WiredTigerDB *_db) : db(_db) 
{
  dinfo << __func__ << "Create new Trx" << dendl;
  db->conn->open_session(db->conn, NULL, NULL, &trx_session);

  int r = trx_session->open_cursor(trx_session, TABLE_NAME, NULL, NULL, &trx_cursor);
  if(r) {
    derr << __func__ << ": Transaction Initialization failed, " << wiredtiger_strerror(r) << dendl;
    ceph_abort_msg("Transaction Initialization failed");
  }

  r = trx_session->begin_transaction(trx_session, "isolation=snapshot");
  if(r) {
    derr << __func__ << ": Transaction Initialization failed, " << wiredtiger_strerror(r) << dendl;
    ceph_abort_msg("Transaction Initialization failed");
  }
}

WiredTigerDB::WiredTigerDBTransactionImpl::~WiredTigerDBTransactionImpl()
{
  if (trx_cursor) {
    trx_cursor->close(trx_cursor);
  }
  trx_session->close(trx_session, NULL);
}

void WiredTigerDB::WiredTigerDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  dinfo << __func__ << ": prefix: " << prefix \
        << " key: " << k << " key size: " << k.size() \
        << " value : " << to_set_bl.buffers().front().c_str() << " value size: " << to_set_bl.length() << dendl;

  if(!to_set_bl.is_contiguous()) {
    ceph_abort_msg("not contiguous bufferlist: not implemented");
  } 

  string key = combine_strings(prefix, k);
  WT_ITEM key_item;
  key_item.data = key.data();
  key_item.size = key.size();
  trx_cursor->set_key(trx_cursor, &key_item);

  WT_ITEM value_item;
  value_item.data = to_set_bl.buffers().front().c_str();
  value_item.size = to_set_bl.length();
  trx_cursor->set_value(trx_cursor, &value_item);

  int r = trx_cursor->insert(trx_cursor);
  if(r) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
  }
}

void WiredTigerDB::WiredTigerDBTransactionImpl::set(
  const string &prefix,
  const char *k, size_t keylen,
  const bufferlist &to_set_bl)
{
  return set(prefix, string(k, keylen), to_set_bl);
}

void WiredTigerDB::WiredTigerDBTransactionImpl::rmkey(const string &prefix,
					        const string &k)
{
  dinfo << __func__ << ": prefix: " << prefix << " k: " << k << dendl;
  
  string key = combine_strings(prefix, k);
  WT_ITEM key_item;
  key_item.data = key.data();
  key_item.size = key.size();
  trx_cursor->set_key(trx_cursor, &key_item);

  int r = trx_cursor->remove(trx_cursor);
  if(r) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
  }
}

void WiredTigerDB::WiredTigerDBTransactionImpl::rmkey(const string &prefix,
                                                const char *k, size_t keylen)
{
  return rmkey(prefix, string(k, keylen));
}

void WiredTigerDB::WiredTigerDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first(); it->valid(); it->next()) {
    rmkey(prefix, it->key());
  }
}

void WiredTigerDB::WiredTigerDBTransactionImpl::rm_range_keys(const string &prefix,
                                                         const string &start,
                                                         const string &end)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->lower_bound(start); it->valid() && it->key() < end; it->next()) {
    rmkey(prefix, it->key());
  }
}

void WiredTigerDB::WiredTigerDBTransactionImpl::merge(const std::string &prefix, const std::string &key,
	                                                    const ceph::bufferlist  &value)
{
  dinfo << __func__ << ": prefix: " << prefix \
      << " key: " << key << " key size: " << key.size() \
      << " value : " << value.buffers().front().c_str() << " value size: " << value.length() << dendl;

  string k = combine_strings(prefix, key);
  WT_ITEM key_item;
  key_item.data = k.data();
  key_item.size = k.size();
  trx_cursor->set_key(trx_cursor, &key_item);

  WT_ITEM value_item;
  value_item.data = value.buffers().front().c_str();
  value_item.size = value.length();
  trx_cursor->set_value(trx_cursor, &value_item);

  int r = trx_cursor->insert(trx_cursor);
  if(r) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
  }
  
}

int WiredTigerDB::submit_transaction(KeyValueDB::Transaction t) 
{
  dinfo << __func__ << dendl;
  utime_t start = ceph_clock_now();
  WiredTigerDBTransactionImpl *impl = static_cast<WiredTigerDBTransactionImpl*>(t.get());
  int r = impl->trx_session->commit_transaction(impl->trx_session, NULL);
  utime_t lat = ceph_clock_now() - start;
  logger->inc(l_wiredtiger_txns);
  logger->tinc(l_wiredtiger_submit_latency, lat);
  if(r) {
    derr << __func__ << ": Transaction submission failed, " << wiredtiger_strerror(r) << dendl;
    return r;
  }  

  return 0;
}

int WiredTigerDB::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  dinfo << __func__ << " (multiple keys): prefix: " << prefix << dendl;
  utime_t start = ceph_clock_now();
  
  for (auto &key : keys) {
    dinfo << __func__ << ": key: " << key << dendl;

    string k = combine_strings(prefix, key);
    WT_ITEM key_item;
    key_item.data = k.data();
    key_item.size = k.size();
    cursor->set_key(cursor, &key_item);
    
    int r = cursor->search(cursor);
    if(r) {
      derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
      return r;
    }

    WT_ITEM value_item;
    r = cursor->get_value(cursor, &value_item);
    if(r) {
      derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
      return -ENOENT;
    }
  
    dinfo << __func__ << " value: " << (const char *)value_item.data << " size: " << value_item.size << dendl;
    (*out)[key].append((const char *)value_item.data, value_item.size);
  }

  utime_t lat = ceph_clock_now() - start;
  logger->inc(l_wiredtiger_gets);
  logger->tinc(l_wiredtiger_get_latency, lat);

  return 0;
}

int WiredTigerDB::get(
    const string &prefix,
    const string &key,
    bufferlist *out)
{
  dinfo << __func__ << ": prefix: " << prefix << " key: " << key << dendl;
  ceph_assert(out && (out->length() == 0));
  utime_t start = ceph_clock_now();

  int r = 0;
  string k = combine_strings(prefix, key);
  WT_ITEM key_item;
  key_item.data = k.data();
  key_item.size = k.size();
  cursor->set_key(cursor, &key_item);

  r = cursor->search(cursor);
  if(r) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return -ENOENT;
  }

  WT_ITEM value_item;
  r = cursor->get_value(cursor, &value_item);
  if(r) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return -ENOENT;
  }

  dinfo << __func__ << " value: " << (const char *)value_item.data << " size: " << value_item.size << dendl;
  out->append((const char *)value_item.data, value_item.size);
  
  utime_t lat = ceph_clock_now() - start;
  logger->inc(l_wiredtiger_gets);
  logger->tinc(l_wiredtiger_get_latency, lat);
  
  return r;
}

int WiredTigerDB::get(
  const string& prefix,
  const char *key,
  size_t keylen,
  bufferlist *out)
{
  return get(prefix, string(key, keylen), out);
}

WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::~WiredTigerDBWholeSpaceIteratorImpl()
{
  if (cursor) {
    cursor->close(cursor);
  }
}
int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::seek_to_first()
{
  dinfo << __func__ << dendl;
  int r = cursor->reset(cursor);
  if(r) {
    derr << __func__ << ": reset() failed, " << wiredtiger_strerror(r) << dendl;
  }
  r = cursor->next(cursor);
  if(r) {
    derr << __func__ << ": next() failed, " << wiredtiger_strerror(r) << dendl;
  }

  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::seek_to_first(const string &prefix)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  cursor->reset(cursor);

  int exact;
  WT_ITEM key_item;
  key_item.data = prefix.data();
  key_item.size = prefix.size();
  cursor->set_key(cursor, &key_item);
 
  int r = cursor->search_near(cursor, &exact);
  if(r) {
    derr << __func__ << ": searching by prefix failed, " << wiredtiger_strerror(r) << dendl;
  }
  // TODO: exact >= 0
  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::seek_to_last()
{
  dinfo << __func__ << dendl;

  int r = cursor->reset(cursor);
  if(r) {
    derr << __func__ << ": reset() failed, " << wiredtiger_strerror(r) << dendl;
  }
  r = cursor->prev(cursor);
  if(r) {
    derr << __func__ << ": prev() failed, " << wiredtiger_strerror(r) << dendl;
  }

  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::seek_to_last(const string &prefix)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  cursor->reset(cursor);

  int exact;
  WT_ITEM key_item;
  key_item.data = prefix.data();
  key_item.size = prefix.size();
  cursor->set_key(cursor, &key_item);

  int r = cursor->search_near(cursor, &exact);
  if(r) {
    derr << __func__ << ": searching by prefix failed, " << wiredtiger_strerror(r) << dendl;
  }
  // TODO: exact <= 0
  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  cursor->reset(cursor);

  std::string bound = combine_strings(prefix, after);
  WT_ITEM key_item;
  key_item.data = bound.data();
  key_item.size = bound.size();
  cursor->set_key(cursor, &key_item);

  int exact;
  int r = cursor->search_near(cursor, &exact);
  if(r) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return -1;
  }

  if (exact <= 0) {
      next();
  } 

  return 0;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  cursor->reset(cursor);

  std::string bound = combine_strings(prefix, to);  
  WT_ITEM key_item;
  key_item.data = bound.data();
  key_item.size = bound.size();
  cursor->set_key(cursor, &key_item);

  int exact;
  int r = cursor->search_near(cursor, &exact);
  if(r) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return -1;
  }

  if (exact < 0) {
      next();
  }

  return 0;
}

bool WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::valid()
{
  dinfo << __func__ << dendl;
  WT_ITEM value_item;
  int r = cursor->get_value(cursor, &value_item);

  if (r == WT_NOTFOUND) {
    return false;
  } else if (r != 0) {
    derr << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return false;
  }

  return true;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::next()
{ 
  dinfo << __func__ << dendl;

  int r = cursor->next(cursor);
  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::prev()
{
  dinfo << __func__ << dendl;

  int r = cursor->prev(cursor);
  return r;
}

string WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::key()
{
  dinfo << __func__ << dendl;

  WT_ITEM key_item;
  cursor->get_key(cursor, &key_item);
  
  std::string in_key = string((const char *)key_item.data, key_item.size);
  std::string out_key;
  int r = split_key(in_key, 0, &out_key);
  if (r < 0) {
    derr << __func__ << ": split_key failed, " << wiredtiger_strerror(r) << dendl;
    return "";
  }

  dinfo << "in_key: " << in_key << " size of in_key : " << in_key.length() \
        << " out_key: " << out_key << " size of out_key: " << out_key.length() << dendl;
  
  return out_key;
}

pair<string,string> WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::raw_key()
{
  dinfo << __func__ << dendl;

  WT_ITEM key_item;
  cursor->get_key(cursor, &key_item);
  
  std::string in_key = string((const char *)key_item.data, key_item.size);
  std::string prefix, out_key;
  split_key(in_key, &prefix, &out_key);
  return std::make_pair(prefix, out_key);
}

bool WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::raw_key_is_prefixed(const string &prefix) 
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  WT_ITEM key_item;
  cursor->get_key(cursor, &key_item);

  dinfo << __func__ << ": Current cursor: " << string((const char *)key_item.data) << " size: " << key_item.size << dendl;
  if (memcmp(key_item.data, prefix.c_str(), prefix.length()) == 0) {
    return true;
  }

  return false;
}

bufferlist WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::value()
{
  dinfo << __func__ << dendl;
  WT_ITEM value_item;
  cursor->get_value(cursor, &value_item);

  bufferlist bl;
  bl.append(bufferptr((const char *)value_item.data, value_item.size));
  return bl;
}

bufferptr WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::value_as_ptr()
{
  dinfo << __func__ << dendl;
  WT_ITEM value_item;
  cursor->get_value(cursor, &value_item);

  return ceph::bufferptr((const char *)value_item.data, value_item.size);
 
}

size_t WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::key_size()
{
  dinfo << __func__ << dendl;

  WT_ITEM key;
  cursor->get_key(cursor, &key);

  return key.size;
}

size_t WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::value_size()
{
  dinfo << __func__ << dendl;

  WT_ITEM value;
  cursor->get_value(cursor, &value);

  return value.size;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::status()
{
  return 0;
}

WiredTigerDB::WholeSpaceIterator WiredTigerDB::get_wholespace_iterator(IteratorOpts opts)
{
  return WholeSpaceIterator(new WiredTigerDBWholeSpaceIteratorImpl(this, opts));
}

int WiredTigerDB::set_merge_operator(
  const string& prefix,
  std::shared_ptr<KeyValueDB::MergeOperator> mop)
{
  return -EOPNOTSUPP;
  //ceph_assert(db == nullptr);
  //merge_ops.push_back(std::make_pair(prefix, mop));
  //return 0;
}

int WiredTigerDB::split_key(std::string &in, std::string *prefix, std::string *key) {
  size_t prefix_len = 0;

  // Find separator inside Slice
  char* separator = (char*) memchr(in.data(), KEY_DELIMETER, in.size());
  if (separator == NULL)
    return -EINVAL;
  prefix_len = size_t(separator - in.data());
  if (prefix_len >= in.size())
    return -EINVAL;

  // Fetch prefix and/or key directly from Slice
  if (prefix) {
    *prefix = std::string(in.data(), prefix_len);
    dinfo << __func__ << ": prefix: " << *prefix << " size: " << prefix->size() << dendl;
  }

  if (key) {
    *key = std::string(separator+1, in.size()-prefix_len-1);
    dinfo << __func__ << ": out_key: " << *key  << " size: " << key->size() << dendl;
  }
  return 0;
}

void WiredTigerDB::dump_db() {
  WT_CURSOR *cursor;

  int r = session->open_cursor(session, TABLE_NAME, NULL, NULL, &cursor);
  if (r != 0) {
    ceph_abort();
  }

  while (cursor->next(cursor) == 0) {
    WT_ITEM key;
    cursor->get_key(cursor, &key);

    std::string key_str((const char *)key.data, key.size);
    dinfo << __func__ << " key: " << key_str << " key_size: " << key_str.size() << dendl;
  }

  cursor->close(cursor);
}