//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

using namespace std;

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
      child_executor_ = move(child_executor);
      plan_ = plan;
}

void UpdateExecutor::Init() {
  table_id_ = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id_);
  index_list_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple update_tuple;
  RID update_rid;
  int count = 0;
  if(child_executor_ == nullptr){
    return false;
  }
  while(child_executor_->Next(&update_tuple, &update_rid)){
    //is_delete标记为删除，不是物理删除
    TupleMeta meta = table_info_->table_->GetTupleMeta(update_rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta , update_rid);

    //根据任务语句，计算出新元组的内容
    vector<Value>values;
    for(auto&it : plan_->target_expressions_){
      Value value = it -> Evaluate(&update_tuple , table_info_->schema_);
      values.push_back(value);
    }
    //创建元组并插入
    Tuple u_tuple(values , &table_info_->schema_);
    TupleMeta meta_temp{};
    optional<RID> insert_rid = table_info_->table_->InsertTuple(meta_temp , u_tuple);
    //更新全部索引,删除旧索引，插入新索引
    for(auto &index_info_tmp : index_list_){
      if(index_info_tmp != nullptr){
        index_info_tmp->index_->DeleteEntry(update_tuple.KeyFromTuple(table_info_->schema_, index_info_tmp->key_schema_,
                                                                      index_info_tmp->index_->GetKeyAttrs()),
                                            update_rid, exec_ctx_->GetTransaction());
        index_info_tmp->index_->InsertEntry(u_tuple.KeyFromTuple(table_info_->schema_, index_info_tmp->key_schema_,
                                                                 index_info_tmp->index_->GetKeyAttrs()),
                                            insert_rid.value(), exec_ctx_->GetTransaction());
      }
    }
    count++;
  }
  child_executor_ = nullptr;
  vector<Value>values;
  values.emplace_back(TypeId::INTEGER , count);
  *tuple = Tuple(values , &GetOutputSchema());
  return true;
}

}  // namespace bustub
