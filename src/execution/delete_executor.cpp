//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,
      plan_(plan) , 
      child_executor_(move(child_executor)) , 
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())){}

void DeleteExecutor::Init() {
    child_executor_->Init();
    //删除之前初始化时对表加上IX锁
    auto txn = exec_ctx_->GetTransaction();
    if (!(txn->IsTableIntentionExclusiveLocked(table_info_->oid_) ||
        txn->IsTableExclusiveLocked(table_info_->oid_) ||
        txn->IsTableSharedIntentionExclusiveLocked(table_info_->oid_))) {
    if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
      throw ExecutionException("can not get table IX lock");
    }
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(is_end_){
        return false;
    }
    int32_t delete_count = 0;
    RID delete_rid;
    Tuple delete_tuple{};

    while(child_executor_->Next(&delete_tuple , &delete_rid)){
        //删除之前拿到行锁
        auto txn = exec_ctx_->GetTransaction();
        if (!txn->IsRowExclusiveLocked(table_info_->oid_, delete_rid)) {
            if (!exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, delete_rid)) {
                throw ExecutionException("can not get row X lock for delete");
            }
        }
        //取出rid的meta标记为已删除，再重新放回
        TupleMeta meta = table_info_->table_->GetTupleMeta(delete_rid);
        meta.is_deleted_ = true;
        table_info_->table_->UpdateTupleMeta(meta , delete_rid);
        ++delete_count;
        //要删除的tuple的每一列的索引都要更新
        for (auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)){
            auto index_key =
                delete_tuple.KeyFromTuple(table_info_->schema_ , index_info->key_schema_ , index_info->index_->GetKeyAttrs());
            index_info->index_->DeleteEntry(index_key , delete_rid , exec_ctx_->GetTransaction());
        }
    }
    vector<Value> values{};
    values.emplace_back(TypeId::INTEGER , delete_count);
    *tuple = Tuple(values , &GetOutputSchema());
    is_end_ = true;
    return true;
}

}  // namespace bustub
