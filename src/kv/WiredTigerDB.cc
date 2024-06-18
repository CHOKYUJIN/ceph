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
#define dwarn dout(20)
#define dinfo dout(10)
#define ddebug dout(10)

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

  int r = wiredtiger_open(path.c_str(), NULL, "create,cache_size=5GB,log=(enabled,recover=on)", &conn);
  if(r) {
    dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    ceph_abort_msg("DB Open failed");
  }

  // Open a session handle for the database.
  conn->open_session(conn, NULL, NULL, &first_session);
  first_session->create(first_session, TABLE_NAME, "key_format=u,value_format=u");

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
  dinfo << __func__ << " is trying to close WiredTiger" << dendl;

  first_session->close(first_session, NULL);
  conn->close(conn, NULL);
}

int WiredTigerDB::repair(std::ostream &out)
{
  // should close and re-open the database.
  dinfo << __func__ << " is trying to repair WiredTiger" << dendl;

  first_session->close(first_session, NULL);
  conn->close(conn, NULL);

  int r = wiredtiger_open(path.c_str(), NULL, "create,cache_size=5GB,log=(enabled,recover=on),statistics=(all)", &conn);
  if(r) {
    dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    ceph_abort_msg("Repair failed");
  }
  conn->open_session(conn, NULL, NULL, &first_session);

  return 0;
}

WiredTigerDB::WiredTigerDBTransactionImpl::WiredTigerDBTransactionImpl(WiredTigerDB *_db) : db(_db) 
{
  dinfo << __func__ << "Create new Trx" << dendl;
  db->conn->open_session(db->conn, NULL, NULL, &trx_session);

  int r = trx_session->open_cursor(trx_session, TABLE_NAME, NULL, NULL, &trx_cursor);
  if(r) {
    dtrace << __func__ << ": Transaction Initialization: open_cursor failed, " << wiredtiger_strerror(r) << dendl;
    ceph_abort_msg("Transaction Initialization: open_cursor failed");
  }

  r = trx_session->begin_transaction(trx_session, "isolation=snapshot");
  if(r) {
    dtrace << __func__ << ": Transaction Initialization: begin_transaction failed, " << wiredtiger_strerror(r) << dendl;
    ceph_abort_msg("Transaction Initialization: begin_transaction failed");
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
  string key, vid;
  string c_key = combine_strings(prefix, k);
  std::chrono::milliseconds retry_expire(MAX_RETRY_MS_TIME);
  auto end_time = std::chrono::system_clock::now() + retry_expire;

  // TODO: Retry loop
  split_key_with_vid(c_key, key, vid);
  if(vid.length() != 0) {
    set_with_vid(key, vid, to_set_bl);
    return;
  }

  if (to_set_bl.is_contiguous() && to_set_bl.length() > 0) {
    dinfo << __func__ << ": contiguous" << dendl;

    dinfo << __func__ << ": prefix: " << prefix \
          << " key: " << k << " key size: " << k.size() \
          << " value : " << to_set_bl.buffers().front().c_str() << " value size: " << to_set_bl.length() << dendl;

    ddebug << __func__ << ": key: " << key << " key size: " << key.size() << dendl;

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
      dtrace << __func__ << ": failed (contiguous bufferlist), " << wiredtiger_strerror(r) << dendl;
      /* TODO: if r == WT_ROLLBACK, rollback_transaction and begin_transaction and retry???????????? */
      if ((trx_session->rollback_transaction(trx_session, NULL)) != 0) 
        ceph_abort_msg("set: WT_ROLLBACK rollback_transaction failed");
      if ((trx_session->begin_transaction(trx_session, NULL)) != 0) 
        ceph_abort_msg("set: WT_ROLLBACK begin_transaction failed");
    }
  } else {
    dinfo << __func__ << ": non-contiguous" << dendl;

    dinfo << __func__ << ": prefix: " << prefix \
          << " key: " << k << " key size: " << k.size() << dendl;

    bufferlist copied_bl = to_set_bl;
    WT_ITEM key_item;
    key_item.data = key.data();
    key_item.size = key.size();
    trx_cursor->set_key(trx_cursor, &key_item);

    WT_ITEM value_item;
    value_item.data = copied_bl.buffers().front().c_str();
    value_item.size = copied_bl.length();
    trx_cursor->set_value(trx_cursor, &value_item);

    int r = trx_cursor->insert(trx_cursor);
    if(r) {
      dtrace << __func__ << ": failed (non-contiguous bufferlist), " << wiredtiger_strerror(r) << dendl;
      /* TODO: if r == WT_ROLLBACK, rollback_transaction and begin_transaction and retry????????? */
      if ((trx_session->rollback_transaction(trx_session, NULL)) != 0) 
        ceph_abort_msg("set: WT_ROLLBACK rollback_transaction failed");
      if ((trx_session->begin_transaction(trx_session, NULL)) != 0) 
        ceph_abort_msg("set: WT_ROLLBACK begin_transaction failed");
    }
  }
}

void WiredTigerDB::WiredTigerDBTransactionImpl::set(
  const string &prefix,
  const char *k, size_t keylen,
  const bufferlist &to_set_bl)
{
  return set(prefix, string(k, keylen), to_set_bl);
}

void WiredTigerDB::WiredTigerDBTransactionImpl::set_with_vid(
  const std::string &key, 
  const std::string &vid,
  const ceph::bufferlist &to_set_bl) 
{
  // TODO: Retry loop
  if (to_set_bl.is_contiguous() && to_set_bl.length() > 0) {
    dinfo << __func__ << ": contiguous" << dendl;

    dinfo << __func__ << " key: " << key << " key size: " << key.size() \
          << " value : " << to_set_bl.buffers().front().c_str() << " value size: " << to_set_bl.length() << dendl;

    ddebug << __func__ << ": key: " << key << " key size: " << key.size() << " vid: " << vid << dendl;

    WT_ITEM key_item;
    key_item.data = key.data();
    key_item.size = key.size();
    key_item.vid = vid.data();
    key_item.vid_size = vid.size();
    trx_cursor->set_key_with_vid(trx_cursor, &key_item);

    WT_ITEM value_item;
    value_item.data = to_set_bl.buffers().front().c_str();
    value_item.size = to_set_bl.length();
    value_item.vid = vid.data();
    value_item.vid_size = vid.size();
    trx_cursor->set_value_with_vid(trx_cursor, &value_item);

    int r = trx_cursor->insert(trx_cursor);
    if(r) {
      dtrace << __func__ << ": failed (contiguous bufferlist), " << wiredtiger_strerror(r) << dendl;
      /* TODO: if r == WT_ROLLBACK, rollback_transaction and begin_transaction and retry???????????? */
      if ((trx_session->rollback_transaction(trx_session, NULL)) != 0) 
        ceph_abort_msg("set: WT_ROLLBACK rollback_transaction failed");
      if ((trx_session->begin_transaction(trx_session, NULL)) != 0) 
        ceph_abort_msg("set: WT_ROLLBACK begin_transaction failed");
    }
  } else {
    dinfo << __func__ << ": non-contiguous" << dendl;

    ddebug << __func__ << ": key: " << key << " key size: " << key.size() << " vid: " << vid << dendl;

    bufferlist copied_bl = to_set_bl;
    WT_ITEM key_item;
    key_item.data = key.data();
    key_item.size = key.size();
    key_item.vid = vid.data();
    key_item.vid_size = vid.size();
    trx_cursor->set_key_with_vid(trx_cursor, &key_item);

    WT_ITEM value_item;
    value_item.data = copied_bl.buffers().front().c_str();
    value_item.size = copied_bl.length();
    value_item.vid = vid.data();
    value_item.vid_size = vid.size();
    trx_cursor->set_value_with_vid(trx_cursor, &value_item);

    int r = trx_cursor->insert(trx_cursor);
    if(r) {
      dtrace << __func__ << ": failed (non-contiguous bufferlist), " << wiredtiger_strerror(r) << dendl;
      /* TODO: if r == WT_ROLLBACK, rollback_transaction and begin_transaction and retry????????? */
      if ((trx_session->rollback_transaction(trx_session, NULL)) != 0) 
        ceph_abort_msg("set: WT_ROLLBACK rollback_transaction failed");
      if ((trx_session->begin_transaction(trx_session, NULL)) != 0) 
        ceph_abort_msg("set: WT_ROLLBACK begin_transaction failed");
    }
  }
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
    dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
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

  std::shared_ptr<MergeOperator> mop = db->_find_merge_op(prefix);
  ceph_assert(mop);

  int r = 0;
  string k = combine_strings(prefix, key);
  std::chrono::milliseconds retry_expire(MAX_RETRY_MS_TIME);
  auto end_time = std::chrono::system_clock::now() + retry_expire;

  bool is_retry = false;
  do {
    WT_ITEM key_item;
    key_item.data = k.data();
    key_item.size = k.size();
    trx_cursor->set_key(trx_cursor, &key_item);

    r = trx_cursor->search(trx_cursor);
    if(r == WT_NOTFOUND) {
      std::string new_value;
      mop->merge_nonexistent(value.buffers().front().c_str(), value.length(), &new_value);

      WT_ITEM value_item;
      value_item.data = new_value.data();
      value_item.size = new_value.size();
      trx_cursor->set_value(trx_cursor, &value_item);

      r = trx_cursor->insert(trx_cursor);
      if(r) {
        if(!is_retry) {
          dtrace << __func__ << ": merge_nonexistent failed, " << wiredtiger_strerror(r) << " retry..." << dendl;
          is_retry = true;
        }
        trx_session->commit_transaction(trx_session, NULL);
        trx_session->reset_snapshot(trx_session);
        trx_session->begin_transaction(trx_session, "isolation=snapshot");
        continue;
      } else {
        if(!is_retry) 
          dinfo << __func__ << ": merge_nonexistent success" << " key: " << key << " key size: " << key.size() << dendl;
        else 
          dinfo << __func__ << ": merge_nonexistent (retry) success" << " key: " << key << " key size: " << key.size() << dendl;
          
        break;
      }
    } else if (!r) {
      WT_ITEM old_value_item;
      r = trx_cursor->get_value(trx_cursor, &old_value_item);
      if(r) {
        dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
        ceph_abort_msg("After search, Cannot get value from the cursor");
      }

      std::string new_value;
      mop->merge((const char *)old_value_item.data, old_value_item.size, value.buffers().front().c_str(), value.length(), &new_value);
      
      WT_ITEM value_item;
      value_item.data = new_value.data();
      value_item.size = new_value.size();
      trx_cursor->set_value(trx_cursor, &value_item);

      r = trx_cursor->update(trx_cursor);
      if(r) {
        if(!is_retry) {
          dtrace << __func__ << ": merge (existent) failed, " << wiredtiger_strerror(r) << " retry..." << dendl;
          is_retry = true;
        }
        trx_session->commit_transaction(trx_session, NULL);
        trx_session->reset_snapshot(trx_session);
        trx_session->begin_transaction(trx_session, "isolation=snapshot");
        continue;
      } else {
        if(!is_retry) 
          dinfo << __func__ << ": merge (existent) success" << " key: " << key << " key size: " << key.size() << dendl;
        else 
          dinfo << __func__ << ": merge (existent) (retry) success" << " key: " << key << " key size: " << key.size() << dendl;

        break;
      }
    } else {
      ceph_abort("not implemented miscellaneaous merge error");
    }
  } while(std::chrono::system_clock::now() < end_time);
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
    dtrace << __func__ << ": Transaction submission failed, " << wiredtiger_strerror(r) << dendl;
    // (bluestore_debug_omit_kv_commit = false) would assert this error return
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
  WT_SESSION *session;
  WT_CURSOR *cursor;

  conn->open_session(conn, NULL, NULL, &session);
  session->open_cursor(session, TABLE_NAME, NULL, NULL, &cursor);

  for (auto &key : keys) {
    dinfo << __func__ << ": key: " << key << dendl;

    string k = combine_strings(prefix, key);
    WT_ITEM key_item;
    key_item.data = k.data();
    key_item.size = k.size();
    cursor->set_key(cursor, &key_item);
    
    int r = cursor->search(cursor);
    if(r) {
      dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
      cursor->close(cursor);
      session->close(session, NULL);
      return -ENOENT;
    }

    WT_ITEM value_item;
    r = cursor->get_value(cursor, &value_item);
    if(r) {
      dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
      cursor->close(cursor);
      session->close(session, NULL);
      return -ENOENT;
    }
  
    dinfo << __func__ << " value: " << (const char *)value_item.data << " size: " << value_item.size << dendl;
    (*out)[key].append((const char *)value_item.data, value_item.size);
  }

  utime_t lat = ceph_clock_now() - start;
  logger->inc(l_wiredtiger_gets);
  logger->tinc(l_wiredtiger_get_latency, lat);

  cursor->close(cursor);
  session->close(session, NULL);

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
  WT_SESSION *session;
  WT_CURSOR *cursor;
  WT_ITEM key_item;

  conn->open_session(conn, NULL, NULL, &session);
  session->open_cursor(session, TABLE_NAME, NULL, NULL, &cursor);

  key_item.data = k.data();
  key_item.size = k.size();
  cursor->set_key(cursor, &key_item);

  r = cursor->search(cursor);
  if(r) {
    dtrace << __func__ << ": search() failed, " << wiredtiger_strerror(r) << dendl;
    cursor->close(cursor);
    session->close(session, NULL);
    return -ENOENT;
  }

  WT_ITEM value_item;
  r = cursor->get_value(cursor, &value_item);
  if(r) {
    dtrace << __func__ << ": get_value() failed, " << wiredtiger_strerror(r) << dendl;
    cursor->close(cursor);
    session->close(session, NULL);
    return -ENOENT;
  }

  dinfo << __func__ << " value: " << (const char *)value_item.data << " size: " << value_item.size << dendl;
  out->append((const char *)value_item.data, value_item.size);
  
  utime_t lat = ceph_clock_now() - start;
  logger->inc(l_wiredtiger_gets);
  logger->tinc(l_wiredtiger_get_latency, lat);
  
  cursor->close(cursor);
  session->close(session, NULL);

  return 0;
}

int WiredTigerDB::get(
  const string& prefix,
  const char *key,
  size_t keylen,
  bufferlist *out)
{
  return get(prefix, string(key, keylen), out);
}

WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::WiredTigerDBWholeSpaceIteratorImpl(const WiredTigerDB* _db, const KeyValueDB::IteratorOpts opts) : db(const_cast<WiredTigerDB*>(_db)) 
{
  dinfo << __func__ << "Create new Trx" << dendl;
  db->conn->open_session(db->conn, NULL, NULL, &iter_session);

  int r = iter_session->open_cursor(iter_session, TABLE_NAME, NULL, NULL, &iter_cursor);
  if(r) {
    dtrace << __func__ << ": Iterator Initialization failed, " << wiredtiger_strerror(r) << dendl;
    ceph_abort_msg("Iterator Initialization failed");
  }

  /*
  WT_CURSOR *temp_cursor;
  int r = db->first_session->open_cursor(db->first_session, TABLE_NAME, NULL, NULL, &temp_cursor);
  if (r != 0) {
    dtrace << __func__ << ": open_cursor failed, " << wiredtiger_strerror(r) << dendl;
    ceph_abort_msg("open_cursor failed");
  }
  this->cursor = temp_cursor;
  */
}

WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::~WiredTigerDBWholeSpaceIteratorImpl()
{
  if (iter_cursor) {
    iter_cursor->close(iter_cursor);
  }
  iter_session->close(iter_session, NULL);
}
int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::seek_to_first()
{
  dinfo << __func__ << dendl;
  int r = iter_cursor->reset(iter_cursor);
  if(r) {
    dtrace << __func__ << ": reset() failed, " << wiredtiger_strerror(r) << dendl;
  }
  r = iter_cursor->next(iter_cursor);
  if(r) {
    dtrace << __func__ << ": next() failed, " << wiredtiger_strerror(r) << dendl;
  }

  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::seek_to_first(const string &prefix)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  iter_cursor->reset(iter_cursor);

  int exact;
  WT_ITEM key_item;
  key_item.data = prefix.data();
  key_item.size = prefix.size();
  iter_cursor->set_key(iter_cursor, &key_item);
 
  int r = iter_cursor->search_near(iter_cursor, &exact);
  if(r) {
    dtrace << __func__ << ": searching by prefix failed, " << wiredtiger_strerror(r) << dendl;
  }
  // TODO: exact >= 0
  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::seek_to_last()
{
  dinfo << __func__ << dendl;

  int r = iter_cursor->reset(iter_cursor);
  if(r) {
    dtrace << __func__ << ": reset() failed, " << wiredtiger_strerror(r) << dendl;
  }
  r = iter_cursor->prev(iter_cursor);
  if(r) {
    dtrace << __func__ << ": prev() failed, " << wiredtiger_strerror(r) << dendl;
  }

  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::seek_to_last(const string &prefix)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  iter_cursor->reset(iter_cursor);

  int exact;
  WT_ITEM key_item;
  key_item.data = prefix.data();
  key_item.size = prefix.size();
  iter_cursor->set_key(iter_cursor, &key_item);

  int r = iter_cursor->search_near(iter_cursor, &exact);
  if(r) {
    dtrace << __func__ << ": searching by prefix failed, " << wiredtiger_strerror(r) << dendl;
    r = seek_to_last();
  }
  // TODO: exact <= 0
  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after)
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  iter_cursor->reset(iter_cursor);

  std::string bound = combine_strings(prefix, after);
  WT_ITEM key_item;
  key_item.data = bound.data();
  key_item.size = bound.size();
  iter_cursor->set_key(iter_cursor, &key_item);

  int exact;
  int r = iter_cursor->search_near(iter_cursor, &exact);
  if(r) {
    dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return -1;
  }

  if (exact <= 0) {
    r = next();
    if(r) {
      dtrace << __func__ << ": next() failed, " << wiredtiger_strerror(r) << dendl;
      return -1;
    }
  } 

  return 0;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to)
{
  dinfo << __func__ << ": prefix: " << prefix << ", to: " << to << dendl;

  iter_cursor->reset(iter_cursor);

  std::string bound = combine_strings(prefix, to);  
  WT_ITEM key_item;
  key_item.data = bound.data();
  key_item.size = bound.size();
  iter_cursor->set_key(iter_cursor, &key_item);

  int exact;
  int r = iter_cursor->search_near(iter_cursor, &exact);
  if(r) {
    dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return -1;
  }

  if (exact < 0) {
    r = next();
    if(r) {
      dtrace << __func__ << ": next() failed, " << wiredtiger_strerror(r) << dendl;
      return -1;
    }
  }

  return 0;
}

bool WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::valid()
{
  dinfo << __func__ << dendl;
  WT_ITEM value_item;
  int r = iter_cursor->get_value(iter_cursor, &value_item);

  if (r == WT_NOTFOUND) {
    return false;
  } else if (r != 0) {
    dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return false;
  }

  return true;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::next()
{ 
  dinfo << __func__ << dendl;

  int r = iter_cursor->next(iter_cursor);
  return r;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::prev()
{
  dinfo << __func__ << dendl;

  int r = iter_cursor->prev(iter_cursor);
  return r;
}

string WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::key()
{
  dinfo << __func__ << dendl;

  WT_ITEM key_item;
  iter_cursor->get_key(iter_cursor, &key_item);
  
  std::string in_key = string((const char *)key_item.data, key_item.size);
  std::string out_key;
  int r = split_key(in_key, 0, &out_key);
  if (r < 0) {
    dtrace << __func__ << ": split_key failed, " << wiredtiger_strerror(r) << dendl;
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
  iter_cursor->get_key(iter_cursor, &key_item);
  
  std::string in_key = string((const char *)key_item.data, key_item.size);
  std::string prefix, out_key;
  split_key(in_key, &prefix, &out_key);
  return std::make_pair(prefix, out_key);
}

bool WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::raw_key_is_prefixed(const string &prefix) 
{
  dinfo << __func__ << ": prefix: " << prefix << dendl;

  WT_ITEM key_item;
  iter_cursor->get_key(iter_cursor, &key_item);

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
  int r = iter_cursor->get_value(iter_cursor, &value_item);
  if (r != 0) {
    dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return bufferlist();
  }

  bufferlist bl;
  bl.append(bufferptr((const char *)value_item.data, value_item.size));
  return bl;
}

bufferptr WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::value_as_ptr()
{
  dinfo << __func__ << dendl;
  WT_ITEM value_item;
  int r = iter_cursor->get_value(iter_cursor, &value_item);
  if (r != 0) {
    dtrace << __func__ << ": failed, " << wiredtiger_strerror(r) << dendl;
    return bufferptr();
  }

  return ceph::bufferptr((const char *)value_item.data, value_item.size);
 
}

size_t WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::key_size()
{
  dinfo << __func__ << dendl;

  WT_ITEM key;
  iter_cursor->get_key(iter_cursor, &key);

  return key.size;
}

size_t WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::value_size()
{
  dinfo << __func__ << dendl;

  WT_ITEM value;
  iter_cursor->get_value(iter_cursor, &value);

  return value.size;
}

int WiredTigerDB::WiredTigerDBWholeSpaceIteratorImpl::status()
{
  return 0;
}

WiredTigerDB::WholeSpaceIterator WiredTigerDB::get_wholespace_iterator(IteratorOpts opts)
{
  // return WholeSpaceIterator(new WiredTigerDBWholeSpaceIteratorImpl(this, opts));
  return std::make_shared<WiredTigerDBWholeSpaceIteratorImpl>(this, opts);
}

int WiredTigerDB::set_merge_operator(
  const string& prefix,
  std::shared_ptr<KeyValueDB::MergeOperator> mop)
{
  ceph_assert(conn == nullptr);
  merge_ops.push_back(std::make_pair(prefix, mop));
  return 0;
}

std::shared_ptr<KeyValueDB::MergeOperator> WiredTigerDB::_find_merge_op(const std::string &prefix)
{
  for (const auto& i : merge_ops) {
    if (i.first == prefix) {
      return i.second;
    }
  }

  dinfo << __func__ << " No merge op for " << prefix << dendl;
  return NULL;
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

void WiredTigerDB::split_key_with_vid(const std::string &combined_key, std::string &key_out, std::string &vid_out) {
  string ver_delim("\0v", 2);
  string instance_delim("\0i", 2);

  size_t ver_pos = combined_key.find(ver_delim);
  size_t instance_pos = combined_key.find(instance_delim);

  if (ver_pos == string::npos || instance_pos == string::npos) {
    key_out = combined_key;
    vid_out = "";
    return;
  }

  // key_out = combined_key.substr(0, ver_pos) + combined_key.substr(instance_pos);
  // vid_out = combined_key.substr(ver_pos + 2, instance_pos - ver_pos - 2);

  key_out = combined_key.substr(0, ver_pos);
  vid_out = combined_key.substr(ver_pos);

  return;
}

void WiredTigerDB::dump_db() {
  WT_CURSOR *cursor;

  int r = first_session->open_cursor(first_session, TABLE_NAME, NULL, NULL, &cursor);
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


void WiredTigerDB::hex_dump(const void *value, size_t size) 
{
  dinfo << __func__ << dendl;
  unsigned char *p = (unsigned char *)value;

  for (size_t i = 0; i < size; i++) {
    dinfo << std::hex << (int)p[i] << " " << dendl;
  }
}
