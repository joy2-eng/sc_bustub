//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "execution/executors/insert_executor.h"

using namespace std;

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_(move(child_executor)),
    table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())){}
    

void InsertExecutor::Init() {
    /**
   * 如果事务已经持有 IX / X / SIX 锁，则无需重复调用LockTable
   * 重复加锁会破坏2PL两阶段锁状态机，导致进入SHRINKING阶段报错
   */
    if (!(exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(table_info_->oid_) ||
        exec_ctx_->GetTransaction()->IsTableExclusiveLocked(table_info_->oid_) ||
        exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(table_info_->oid_))) {
    // 对目标表加 INTENTION_EXCLUSIVE（IX）意向排他表锁
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                table_info_->oid_)) {
      throw ExecutionException("can not get lock");
    }
  }
    child_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(is_end_){
        return false;
    }
    int32_t insert_count = 0;
    RID emit_rid;
    //即将插入的tuple
    Tuple to_insert_tuple{};
    //tuple的信息
    TupleMeta meta{};

    while(child_->Next(&to_insert_tuple , &emit_rid)){
        //将获取到的新turple插入到表中
        optional<RID> new_rid = table_info_->table_->InsertTuple(meta , to_insert_tuple , exec_ctx_->GetLockManager() , exec_ctx_->GetTransaction() , table_info_->oid_);
        if(!new_rid){
            continue;
        }else{
            //为新加入的行加写锁
            auto txn = exec_ctx_->GetTransaction();
            if (!txn->IsRowExclusiveLocked(table_info_->oid_, new_rid.value())) {
            if (!exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, new_rid.value())) {
                throw ExecutionException("cannot get row X lock for insert tuple");
            }
            }

        }
        ++insert_count;
        //tuple相关联的所有索引都要更新
        for(auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)){
            //根据tuble生成一个索引需要的key
            auto index_key = to_insert_tuple.KeyFromTuple(table_info_->schema_ , index_info->key_schema_,
                                                            index_info->index_->GetKeyAttrs());
            //索引里插入一条记录（key————rid）
            index_info->index_->InsertEntry(index_key , new_rid.value() , exec_ctx_->GetTransaction());
        }
    }
    vector<Value> values{};
    values.emplace_back(TypeId::INTEGER , insert_count);
    *tuple = Tuple(values , &GetOutputSchema());
    is_end_ = true;
    return true;
}

}  // namespace bustub
