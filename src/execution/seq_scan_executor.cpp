//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

using namespace std;

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
: AbstractExecutor(exec_ctx) ,
plan_(plan){}

void SeqScanExecutor::Init() {
    //根据table的oid获取table的info
    table_oid_t tid = plan_->GetTableOid();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(tid);
    // ---------- DELETE 删除场景 ----------
  if (exec_ctx_->IsDelete() && !(exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(tid) ||
                                  exec_ctx_->GetTransaction()->IsTableExclusiveLocked(tid) ||
                                  exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(tid))) {
    // 当前事务还没有持有 IX / X / SIX 表锁，则需要获取表级意向排他锁 IX
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                tid)) {
      throw ExecutionException("can not get lock");
    }
  } else {
    // ---------- 普通查询读场景 ----------
    // 读未提交隔离级别：读不加锁，跳过加锁逻辑
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !(exec_ctx_->GetTransaction()->IsTableExclusiveLocked(tid) ||
          exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(tid) ||
          exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(tid) ||
          exec_ctx_->GetTransaction()->IsTableSharedLocked(tid) ||
          exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(tid))) {
      // 事务尚未持有任意表锁，获取表级意向共享锁 IS
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                  tid)) {
        throw ExecutionException("can not get lock");
      }
    }
  }
    iterator_ = make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> tup;
  // 循环遍历表记录，直到找到有效元组或者迭代结束
  while (!iterator_->IsEnd()) {
    // ========== 行锁加锁逻辑 ==========
    if (exec_ctx_->IsDelete() &&
        !exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), iterator_->GetRID())) {
      // DELETE操作：给当前行加 X 排他行锁
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                plan_->GetTableOid(), iterator_->GetRID())) {
        throw ExecutionException("can not get lock");
      }
    } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      // 普通读操作，读未提交除外：给当前行加 S 共享行锁；已经持有锁就不用重复加
      if (!exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), iterator_->GetRID()) &&
          !exec_ctx_->GetTransaction()->IsRowSharedLocked(plan_->GetTableOid(), iterator_->GetRID())) {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                  plan_->GetTableOid(), iterator_->GetRID())) {
          throw ExecutionException("can not get lock");
        }
      }
    }

    // 取出当前迭代位置的元组
    tup = iterator_->GetTuple();

    // 当前元组已经被标记删除 is_deleted_=true
    if (tup.first.is_deleted_) {
      // 不是delete操作，并且不是读未提交，释放这一行锁（被删除行不需要返回上层）
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
          !exec_ctx_->IsDelete()) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), iterator_->GetRID(),
                                               true);
      }
      // 元组已删除，跳过这条，迭代器前进，继续while循环找下一条
      ++(*iterator_);
    } else {
      // ---------- 元组有效，检查过滤谓词 filter ----------
      if (plan_->filter_predicate_) {
        // 执行过滤条件表达式计算
        const Value &value = plan_->filter_predicate_->Evaluate(&tup.second, table_info_->schema_);
        if (value.GetAs<bool>()) {
          // 满足过滤条件，输出tuple、rid给上层算子
          *tuple = tup.second;
          *rid = iterator_->GetRID();
        } else {
          // 不满足过滤条件，跳过，释放锁(RC)，迭代前进
          if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
              !exec_ctx_->IsDelete()) {
            exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(),
                                                   iterator_->GetRID(), false);
          }
          ++(*iterator_);
          continue;
        }
      } else {
        // 没有过滤条件，直接返回当前元组
        *tuple = tup.second;
        *rid = iterator_->GetRID();
      }

      // ===== READ_COMMITTED 核心行为：读完一行立刻释放S行锁（Strict‑2PL）=====
      // REPEATABLE_READ 不在这里释放S锁，S锁等到事务commit/abort才释放(Rigorous‑2PL)
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !exec_ctx_->IsDelete()) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), iterator_->GetRID(),
                                               false);
      }
      // 找到一条有效记录，跳出while，返回true给到上层Volcano模型
      break;
    }
  }

  // 迭代器走到末尾，没有更多数据
  if (iterator_->IsEnd()) {
    return false;
  }

  // 迭代器前进，为下一次Next调用做准备
  ++(*iterator_);
  return true;
}

}  // namespace bustub
