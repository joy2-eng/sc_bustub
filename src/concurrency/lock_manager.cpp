//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

using namespace std;

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  //如果在RU中看到了S，IS ，SIX锁，事务转为终止，抛异常
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    //     For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
    //  TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  //进入收缩阶段就不能加锁了
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  //RC在收缩阶段可以加锁，但是只能加读，不能加写
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->GetState() == TransactionState::SHRINKING &&
      (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  //同样RR的收缩阶段也不能加锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  //==== 获取该表的锁请求队列 ====
  //维护了一个全局哈希(table_lock_map)，table_oid->该表的锁请求队列
  //对哈希表加锁维护
  table_lock_map_latch_.lock();
  //如果该表还没有锁队列，就创建一个
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  //从哈希表中取出队列
  auto request_queue = table_lock_map_[oid];
  //及时释放哈希表的锁，提高并发
  table_lock_map_latch_.unlock();


  //构建表锁对象，granted默认为false 未授予
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  bool flag; // flag == true 表示进行锁升级， ==false表示全新普通锁申请
  {
    //对该表的锁申请队列加锁
    std::lock_guard<std::mutex> lk = std::lock_guard<std::mutex>(request_queue->latch_);
    // 检查是否是锁升级
    flag = CheckLockUpdateTable(txn, request_queue, lock_mode, request);
    if (flag) {
      //检查锁升级后是否兼容，兼容则新锁的granted就会是true
      GrantNewLocksIfPossible(request_queue);
      bool is_grant = request->granted_;
      if (is_grant) {
        //升级完成，升级标志设置为空
        request_queue->upgrading_ = INVALID_TXN_ID;
      }
      if (is_grant) {
        //把新授予的锁加入到事务记录中
        InsertLockModeOfIdFromTxn(txn, lock_mode, oid);
        return true;
      }
    }
  }

  //如果是普通锁，就加入队列
  if (!flag) {
    request_queue->latch_.lock();
    request_queue->request_queue_.push_back(request);
    request_queue->latch_.unlock();
  }


  /**
   * condition_variable::wait ()在这里干了什么事：
   * 运行到这里时候走一遍lambda表达式，返回false则开睡
   * 睡眠等待唤醒(notify)，当其他线程释放锁并唤醒和这个锁有关的线程时，wait函数就会执行第二个参数对应的lambda表达式
   * 当返回false，就继续睡眠，返回true就可以继续向下了。
   */
  std::unique_lock<std::mutex> lk = std::unique_lock<std::mutex>(request_queue->latch_);
  request_queue->cv_.wait(lk, [this, &request, &request_queue]() {
    GrantNewLocksIfPossible(request_queue);
    return request->granted_;
  });

  //如果被唤醒后，当前事务被死锁检测线程标记
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_DEBUG("txn : %d Abort after get lock (tableId=%d) lockType=%s", txn->GetTransactionId(), oid,
              LockType(lock_mode).c_str());
    //解锁
    lk.unlock();
    //从队列和事务中删除该锁
    EraseRequestFromQueue(txn, oid);
    EraseLockModeOfIdFromTxn(txn, lock_mode, oid);
    //唤醒睡眠的其他线程
    request_queue->cv_.notify_all();
    return false;
  }

  //如果还涉及锁升级，就关掉
  if (request_queue->upgrading_ != INVALID_TXN_ID) {
    request_queue->upgrading_ = INVALID_TXN_ID;
  }
  // book keeping 加锁成功，记录进事务
  InsertLockModeOfIdFromTxn(txn, lock_mode, oid);
  //  LOG_DEBUG("txn : %d lock table (tableId=%d) lockType=%s", txn->GetTransactionId(), oid,
  //            LockType(request->lock_mode_).c_str());
  //  std::cout<<"txn :"<<txn->GetTransactionId()<<" lock table"<<" "<<oid<<" "<<LockType(request->lock_mode_)<<"
  //  lock"<<std::endl;
  return true;
}



auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  //在事务中检查一下 该事务是否持有这种表的锁 不持有则抛异常
  LockRequest request = GetTxnHoldLockOfTable(txn, oid);
  if (request.txn_id_ == INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  //三种可以让AP2转为收缩阶段的释放锁情况
  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
       (request.lock_mode_ == LockMode::EXCLUSIVE || request.lock_mode_ == LockMode::SHARED)) ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request.lock_mode_ == LockMode::EXCLUSIVE) ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && request.lock_mode_ == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // 多级锁校验：释放表锁前，必须确认该事务已没有这张表下残留的任何行锁
  auto row_s_set = txn->GetSharedRowLockSet()->find(oid);
  auto row_x_set = txn->GetExclusiveRowLockSet()->find(oid);
  if ((row_s_set != txn->GetSharedRowLockSet()->end() && !row_s_set->second.empty()) ||
      (row_x_set != txn->GetExclusiveRowLockSet()->end() && !row_x_set->second.empty())) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  //从队列中删除
  if (!EraseRequestFromQueue(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  //从全局中取出该表的锁等待队列
  table_lock_map_latch_.lock();
  auto list = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  //唤醒这些等待线程
  list->cv_.notify_all();
  // book keeping
  EraseLockModeOfIdFromTxn(txn, request.lock_mode_, oid);
  //  LOG_DEBUG("txn : %d unlock table (tableId=%d)", txn->GetTransactionId(), oid);

  //  std::cout<<"txn :"<<txn->GetTransactionId()<<" unlock table"<<" "<<oid<<" "<<LockType(request.lock_mode_)<<"
  //  lock"<<std::endl;
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  
  //行锁只能接收S X锁，其他三个都是表锁
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  //RU不会出现读锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  //进入收缩阶段也不能加锁了
  if ((txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->GetState() == TransactionState::SHRINKING &&
       lock_mode != LockMode::SHARED) ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
       txn->GetState() == TransactionState::SHRINKING)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  //收缩阶段不加锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  //在获取行锁时，事务必须持有所在表上对应的意象锁。
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  //全局哈希加锁，取出对应行的锁等待队列
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto row_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  
  //构建行锁对象，granted默认为false 未授予
  std::shared_ptr<LockRequest> request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  bool flag;
  {
    //给等待队列上锁
    std::lock_guard<std::mutex> lk = std::lock_guard<std::mutex>(row_queue->latch_);
    //检查是否为升级锁
    flag = CheckLockUpdateRow(txn, row_queue, lock_mode, request);
    if (flag) {   //如果需要升级
      GrantNewLocksIfPossible(row_queue);  //检测新的锁时都可以被正常授予
      bool is_grant = request->granted_;
      //    bool is_grant = CheckGrantLock(row_queue, txn, lock_mode, oid, rid);
      if (is_grant) {
        row_queue->upgrading_ = INVALID_TXN_ID;
      }
      if (is_grant) {
        InsertLockModeOfIdFromTxn(txn, lock_mode, oid, rid);
        return true;
      }
    }
  }

  if (!flag) {  //不是锁升级，就正常添加全新普通锁
    row_queue->latch_.lock();
    row_queue->request_queue_.push_back(request);
    row_queue->latch_.unlock();
  }

  std::unique_lock<std::mutex> lk = std::unique_lock<std::mutex>(row_queue->latch_);
  //开始wait等待 等待新锁的granted标记修改为true的时候
  row_queue->cv_.wait(lk, [&row_queue, &request, this]() {
    GrantNewLocksIfPossible(row_queue);
    return request->granted_;
  });

  //被死锁检测线程中止
  if (txn->GetState() == TransactionState::ABORTED) {
    lk.unlock();
    EraseRequestFromQueue(txn, rid);
    EraseLockModeOfIdFromTxn(txn, lock_mode, oid, rid);
    row_queue->cv_.notify_all();
    return false;
  }

  //升级锁标志置空
  if (row_queue->upgrading_ != INVALID_TXN_ID) {
    row_queue->upgrading_ = INVALID_TXN_ID;
  }
  // book keeping
  InsertLockModeOfIdFromTxn(txn, lock_mode, oid, rid);
  return true;
}



auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {

  //在事务中获取行锁
  LockRequest request = GetTxnHoldLockOfRow(txn, oid, rid);
  if (request.txn_id_ == INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  /**force
   * force == true 表示事务即将结束，也不需要再转换为收缩状态 即不需要考虑2PL 将所有的锁直接回收处理
   * force == false 就是正常的走
   */
  if (!force) {  //下面是三种转换为收缩阶段的情况
    if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
         (request.lock_mode_ == LockMode::EXCLUSIVE || request.lock_mode_ == LockMode::SHARED)) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request.lock_mode_ == LockMode::EXCLUSIVE) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && request.lock_mode_ == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  //从队列中删除该锁
  if (!EraseRequestFromQueue(txn, rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  //拿出该行的锁等待队列
  row_lock_map_latch_.lock();
  auto list = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  //唤醒等待队列上的线程
  list->cv_.notify_all();
  // book keeping 从事务记录中删除该锁
  EraseLockModeOfIdFromTxn(txn, request.lock_mode_, oid, rid);

  return true;
}

//从队列中删除对应的锁
auto LockManager::EraseRequestFromQueue(Transaction *txn, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  auto list = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  list->latch_.lock();
  auto item = list->request_queue_.begin();
  for (; item != list->request_queue_.end(); ++item) {
    if ((*item)->granted_ && (*item)->txn_id_ == txn->GetTransactionId()) {
      list->request_queue_.erase(item);
      list->latch_.unlock();
      return true;
    }
  }
  list->latch_.unlock();
  return false;
}

auto LockManager::GetTxnHoldLockOfTable(Transaction *txn, const table_oid_t &oid) -> LockRequest {
  table_lock_map_latch_.lock();
  LockRequest lock_request = LockRequest(INVALID_TXN_ID, LockMode::INTENTION_SHARED, oid);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    return lock_request;
  }
  auto list = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  list->latch_.lock();
  for (const auto &request : list->request_queue_) {
    if (request->granted_ && request->oid_ == oid && request->txn_id_ == txn->GetTransactionId()) {
      lock_request.lock_mode_ = request->lock_mode_;
      lock_request.txn_id_ = request->txn_id_;
      lock_request.oid_ = request->oid_;
      lock_request.rid_ = request->rid_;
      lock_request.granted_ = true;
      break;
    }
  }
  list->latch_.unlock();
  return lock_request;
}

//检查是否行锁升级，检查升级合法性 
auto LockManager::CheckLockUpdateRow(Transaction *txn, std::shared_ptr<LockRequestQueue> &queue, LockMode lock_mode,
                                     const std::shared_ptr<LockRequest> &request) -> bool {
  bool flag = false;
  auto item = queue->request_queue_.begin();
  for (; item != queue->request_queue_.end(); item++) {
    if (!(*item)->granted_) {
      break;
    }
    if ((*item)->txn_id_ == txn->GetTransactionId()) {
      if (CanLockUpgrade((*item)->lock_mode_, lock_mode)) {
        if (queue->upgrading_ != INVALID_TXN_ID) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }
        queue->upgrading_ = txn->GetTransactionId();
        flag = true;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    }
  }
  if (flag) {
    EraseLockModeOfIdFromTxn(txn, (*item)->lock_mode_, (*item)->oid_, (*item)->rid_);
    queue->request_queue_.erase(item);

    auto iter = queue->request_queue_.begin();
    for (; iter != queue->request_queue_.end(); iter++) {
      if (!(*iter)->granted_) {
        break;
      }
    }
    queue->request_queue_.insert(iter, request);
  }
  return flag;
}

//在加行锁之前查看事务中是否持有该表的意向锁
auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  bool flag = false;
  if (row_lock_mode == LockMode::SHARED) {
    flag |= txn->GetSharedTableLockSet()->find(oid) != txn->GetSharedTableLockSet()->end();
    flag |= txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end();
    flag |= txn->GetIntentionSharedTableLockSet()->find(oid) != txn->GetIntentionSharedTableLockSet()->end();
    flag |= txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end();
    flag |= txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
            txn->GetSharedIntentionExclusiveTableLockSet()->end();
  } else if (row_lock_mode == LockMode::EXCLUSIVE) {
    flag |= txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end();
    flag |= txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end();
    flag |= txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
            txn->GetSharedIntentionExclusiveTableLockSet()->end();
  }
  return flag;
}


//把新添加的锁记录添加到事务中去
void LockManager::InsertLockModeOfIdFromTxn(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
}

void LockManager::InsertLockModeOfIdFromTxn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                            const RID &rid) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      auto mp = txn->GetSharedRowLockSet();
      if (mp->find(oid) == mp->end()) {
        (*mp)[oid] = std::unordered_set<RID>();
      }
      (*mp)[oid].insert(rid);
    } break;
    case LockMode::EXCLUSIVE: {
      auto mp = txn->GetExclusiveRowLockSet();
      if (mp->find(oid) == mp->end()) {
        (*mp)[oid] = std::unordered_set<RID>();
      }
      (*mp)[oid].insert(rid);
    } break;
    default:
      break;
  }
}


auto LockManager::EraseRequestFromQueue(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  auto list = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  list->latch_.lock();
  auto item = list->request_queue_.begin();
  for (; item != list->request_queue_.end(); ++item) {
    if ((*item)->granted_ && (*item)->txn_id_ == txn->GetTransactionId()) {
      list->request_queue_.erase(item);
      list->latch_.unlock();
      return true;
    }
  }
  list->latch_.unlock();
  return false;
}

auto LockManager::GetTxnHoldLockOfRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> LockRequest {
  LockRequest lock_request = LockRequest(INVALID_TXN_ID, LockMode::INTENTION_SHARED, oid, rid);
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    return lock_request;
  }
  auto list = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  list->latch_.lock();
  for (const auto &request : list->request_queue_) {
    if (request->granted_ && request->oid_ == oid && lock_request.rid_ == rid) {
      lock_request.lock_mode_ = request->lock_mode_;
      lock_request.txn_id_ = request->txn_id_;
      lock_request.oid_ = request->oid_;
      lock_request.rid_ = request->rid_;
      lock_request.granted_ = true;
      break;
    }
  }
  list->latch_.unlock();
  return lock_request;
}



//检测是否可以执行锁升级
auto LockManager::CheckLockUpdateTable(Transaction *txn, std::shared_ptr<LockRequestQueue> &queue, LockMode lock_mode,
                                       const std::shared_ptr<LockRequest> &request) -> bool {
  bool flag = false;
  auto item = queue->request_queue_.begin();
  //对队列中已经授予的锁依次进行操作
  for (; item != queue->request_queue_.end(); item++) {
    //若遍历到未授予的部分直接退出
    if (!(*item)->granted_) {
      break;
    }
    if ((*item)->txn_id_ == txn->GetTransactionId()) {  //是否有升级需求
      if (CanLockUpgrade((*item)->lock_mode_, lock_mode)) {  //升级是否合法 ，不合法就到else中抛出异常
        if (queue->upgrading_ != INVALID_TXN_ID) {  //队列中还有其他事务等待升级吗，用upgrading标志
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }
        queue->upgrading_ = txn->GetTransactionId();
        flag = true;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    }
  }
  // 如果是锁升级
  if (flag) {
    //事务中对该锁的记录删除
    EraseLockModeOfIdFromTxn(txn, (*item)->lock_mode_, (*item)->oid_);
    //锁队列中也删除
    queue->request_queue_.erase(item);
    // erase from queue

    //把升级锁加入到已经授权锁最后面
    //当然若经过不了GrantNewLocksIfPossible检验就会是一个未授予的锁
    auto iter = queue->request_queue_.begin();
    for (; iter != queue->request_queue_.end(); iter++) {
      if (!(*iter)->granted_) {
        break;
      }
    }
    queue->request_queue_.insert(iter, request);
    queue->upgrading_ = txn->GetTransactionId();
    GrantNewLocksIfPossible(queue);
  }
  return flag;
}

//从事务中删除对应锁
void LockManager::EraseLockModeOfIdFromTxn(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
}

void LockManager::EraseLockModeOfIdFromTxn(Transaction *txn, LockMode lockMode, const table_oid_t &oid,
                                           const RID &rid) {
  switch (lockMode) {
    case LockMode::EXCLUSIVE: {
      auto mp = txn->GetExclusiveRowLockSet();
      if (mp->find(oid) != mp->end()) {
        (*mp)[oid].erase(rid);
      }
      if ((*mp)[oid].empty()) {
        mp->erase(oid);
      }
    } break;
    case LockMode::SHARED: {
      auto mp = txn->GetSharedRowLockSet();
      if (mp->find(oid) != mp->end()) {
        (*mp)[oid].erase(rid);
      }
      if ((*mp)[oid].empty()) {
        mp->erase(oid);
      }
    } break;
    default:
      break;
  }
}


//升级是否合法
auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED && requested_lock_mode != LockMode::INTENTION_SHARED) {
    return true;
  }
  if (curr_lock_mode == LockMode::SHARED &&
      (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
      (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && requested_lock_mode == LockMode::EXCLUSIVE) {
    return true;
  }
  return false;
}




//这个函数的作用是用来检查锁队列中各个锁是否相互兼容，
//不兼容是不可以的（granted为false）反之  返回true  
//这里的true和false是锁的性质表示该锁是否已经被成功授予（granted）
void LockManager::GrantNewLocksIfPossible(std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  //预处理，先设置为true
  if (!lock_request_queue->request_queue_.empty()) {
    lock_request_queue->request_queue_.front()->granted_ = true;
  }
  //用来存放已经遍历通过的锁类型
  std::unordered_set<LockMode> lock_mode_set;
  for (auto &item : lock_request_queue->request_queue_) {
    //第一个锁顺利通过
    if (lock_mode_set.empty()) {
      lock_mode_set.insert(item->lock_mode_);
    } else {  //后面的锁都必须保证和前面的都兼容
      bool flag = true;
      for (const auto &lock_mode : lock_mode_set) {
        if (!AreLocksCompatible(lock_mode, item->lock_mode_)) {  //这个函数是判断是否兼容
          flag = false;
          break;
        }
      }
      if (flag) {
        item->granted_ = true;
        lock_mode_set.insert(item->lock_mode_);
      } else {
        break;
      }
    }
  }
}

//判断两个锁类型是否兼容
auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  if (l1 == LockMode::INTENTION_SHARED && l2 == LockMode::EXCLUSIVE) {
    return false;
  }
  if (l1 == LockMode::INTENTION_EXCLUSIVE && l2 != LockMode::INTENTION_SHARED && l2 != LockMode::INTENTION_EXCLUSIVE) {
    return false;
  }
  if (l1 == LockMode::SHARED && l2 != LockMode::INTENTION_SHARED && l2 != LockMode::SHARED) {
    return false;
  }
  if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE && l2 != LockMode::INTENTION_SHARED) {
    return false;
  }
  if (l1 == LockMode::EXCLUSIVE) {
    return false;
  }
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if(find(waits_for_[t1].begin() , waits_for_[t1].end() , t2) == waits_for_[t1].end()){
    waits_for_[t1].push_back(t2);
  }
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  auto iter = find(waits_for_[t1].begin() , waits_for_[t1].end() , t2);
  if(iter != waits_for_[t1].end()){
    waits_for_[t1].erase(iter);
  }
  waits_for_latch_.unlock();
}

//递归找环
auto LockManager::Dfs(txn_id_t tid, std::vector<txn_id_t> &path, std::unordered_map<int, bool> &is_vis) -> bool{
  if(is_vis[tid]){
    return true;  //再次递归到当前递归栈中的节点，有环
  }

  path.push_back(tid);//添加进路径
  is_vis[tid] = true; //标记

  for(auto child : waits_for_[tid]){
    if(Dfs(child , path , is_vis)){
      return true;
    }
  }
  path.pop_back(); //回溯到上一个节点
  return false;
}


auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  //传给上层的参数置为空
  *txn_id = INVALID_TXN_ID;
  //拷贝所有有出边的节点tid ， 并将他们从小到大排序(要求对于多条边要从小到大遍历)
  waits_for_latch_.lock();
  unordered_map<int , bool> is_vis;
  vector<int> path;
  vector<int> v(waits_for_.size());
  for(auto &pair : waits_for_){
    v.push_back(pair.first);
  }
  waits_for_latch_.unlock();
  sort(v.begin() , v.end());

  for(auto s : v){
    if(!is_vis[s] && Dfs(s , path , is_vis)){
      //有环，找到tid最大的（即最新的事务）
      for(int path_txn_id : path){
        if(*txn_id < path_txn_id){
          *txn_id = path_txn_id;
        }
      }
      break;
    }
  }
  return *txn_id != INVALID_TXN_ID;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for(auto u : waits_for_){
    for(auto s : u.second){
      edges.emplace_back(u.first , s);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  //死锁检测的开关，只要开着就循环检测
  while (enable_cycle_detection_) {
    //休眠指定的时间间隔，周期性的进行死锁检测
    std::this_thread::sleep_for(cycle_detection_interval);
    { 
      // 用于接收HasCycle输出：需要abort的受害者事务ID
      shared_ptr<txn_id_t> txn_id = make_shared<txn_id_t>();
      waits_for_latch_.lock();
      //每次检测重新构建完整waits_for_图
      waits_for_.clear();
      waits_for_latch_.unlock();
      
      //hold_set:持有该锁的事务集合 wait_set:等待该锁的事务集合
      unordered_set<txn_id_t> hold_set;
      unordered_set<txn_id_t> wait_set;

      // ========== 第一步：遍历表锁，构建等待图边 ==========
      table_lock_map_latch_.lock();
      for(const auto &item : table_lock_map_){
        item.second->latch_.lock();
        for(const auto &request : item.second->request_queue_){
          if(request->granted_){
            //已经授予的锁
            hold_set.insert(request->txn_id_);
          }else{
            //请求未授予
            wait_set.insert(request->txn_id_);
          }
        }
        item.second->latch_.unlock();

        //根据锁分别找到持有该锁和等待该锁的事务,并将两个事务建立边
        for(const auto &wait : wait_set){
          for(const auto &hold : hold_set){
            if(txn_manager_->GetTransaction(wait)->GetState() != TransactionState::ABORTED &&
               txn_manager_->GetTransaction(hold)->GetState() != TransactionState::ABORTED){
                AddEdge(wait , hold);
              }
          }
        }
        //处理完一张表，清空再去处理下一张表
        wait_set.clear();
        hold_set.clear();
      }
      table_lock_map_latch_.unlock();

      // ========== 第二步：遍历行锁，构建等待图边 ==========
      row_lock_map_latch_.lock();
      for(const auto &item : row_lock_map_){
        item.second->latch_.lock();
        for(const auto &request : item.second->request_queue_){
          if(request->granted_){
            hold_set.insert(request->txn_id_);
          }else{
            wait_set.insert(request->txn_id_);
          }
        }
        item.second->latch_.unlock();
        for (const auto &wait : wait_set) {
          for (const auto &hold : hold_set) {
            if (txn_manager_->GetTransaction(wait)->GetState() != TransactionState::ABORTED &&
                txn_manager_->GetTransaction(hold)->GetState() != TransactionState::ABORTED) {
              AddEdge(wait, hold);
            }
          }
        }
        wait_set.clear();
        hold_set.clear();
      }
      row_lock_map_latch_.unlock();

      // ========== 第三步：DFS检测等待图是否存在死锁环 ==========
      if(HasCycle(txn_id.get())){
        // 检测到死锁，txn_id保存环内最大txn_id，调用事务管理器abort受害者事务
        txn_manager_->Abort(txn_manager_->GetTransaction(*txn_id));
      }
    }
  }
}

}  // namespace bustub
