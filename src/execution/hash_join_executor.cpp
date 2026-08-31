//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

using namespace std;

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_child_ = move(left_child);
  right_child_ = move(right_child);
}

/**
 * > HashJoin 核心思想：**构建 + 探测两阶段**
1. **Build 构建阶段**：拿**右表**全部数据，构建哈希表`ht_`
2. **Probe 探测阶段**：遍历**左表**，拿左表 join key 去哈希表查匹配，把所有 join 结果全部算出来存到`output_`数组
3. `Next()`只负责简单迭代`output_`，直接往外吐已经算好的元组。
 */
void HashJoinExecutor::Init() {
  //拿到左右两边join key的表达时（即需要比较的字段）
  vector<AbstractExpressionRef> left_expr = plan_->left_key_expressions_;
  vector<AbstractExpressionRef> right_expr = plan_->right_key_expressions_;
  uint32_t right_count = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
  uint32_t left_count = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();

  //初始化左右子执行器
  left_child_->Init();
  right_child_->Init();

  Tuple produce_tuple;
  RID produce_rid;
  HashJoinKey key;  //jion 的键 ， 可以多列
  HashJoinValue value;

  // =========【build阶段 ， 处理右表 ， 构建哈希表ht_】==========
  //循环读取右表所有tuple
  while(right_child_->Next(&produce_tuple , &produce_rid)){
    //组合出当前tuple的哈希key
    for(auto &it : right_expr){
      key.keys_.emplace_back(it->Evaluate(&produce_tuple , plan_->GetRightPlan()->OutputSchema()));
    }
    /**
     * 进行重复组合
     * 如果有多个匹配项就都写进同一个ht_[key]
     * ht_是unordered<HashJoinKey , value<Tuple>>
     */
    if(ht_.find(key) == ht_.end()){
      //目前找到的第一个，新建vector并把当前tuple放入
      ht_[key] = vector<Tuple>{produce_tuple};
    }else{
      //已经存在相同的key ， 直接追加进去
      ht_[key].push_back(produce_tuple);
    }
    key.keys_.clear(); //清空，准备处理下一个right—tuple
  }

  // ==========【Probe探测阶段 ,遍历左表，和哈希表匹配，生成结果存入output_】==========
  if(plan_->GetJoinType() == JoinType::LEFT){
    //====== left join 逻辑 =======
    while(left_child_->Next(&produce_tuple , &produce_rid)){
      key.keys_.clear();
      //计算当前左元组的key
      for(auto &it : left_expr){
        key.keys_.emplace_back(it->Evaluate(&produce_tuple , plan_->GetLeftPlan()->OutputSchema()));
      }

      if(ht_.find(key) == ht_.end()){
        //没找到，前面拷贝left tuple 后面补null
        vector<Value>values;
        for(uint32_t i = 0 ; i < left_count ; i++){
          values.push_back(produce_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema() , i));
        }
        for(uint32_t i =0 ; i < right_count ; i++){
          values.push_back(
                  ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
        }
        output_.emplace_back(values , &GetOutputSchema());
      }else{
        //哈希表找到key：遍历该key对应的全部right tuple ， 一一匹配输出
        for(auto &it : ht_[key]){  //一一取出
          vector<Value> values;
          for(uint32_t j = 0 ; j < left_count ; j++){
            values.push_back(produce_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema() , j));
          }
          for(uint32_t j = 0 ; j < right_count ; j++){
            values.push_back(it.GetValue(&plan_->GetRightPlan()->OutputSchema() , j));
          }
          output_.emplace_back(values , &GetOutputSchema());
        }
      }
    }
  }else if(plan_->GetJoinType() == JoinType::INNER){
    //===== INNER 逻辑 =====
    while(left_child_->Next(&produce_tuple , &produce_rid)){
      key.keys_.clear();
      //计算当前left tuple 的 key
      for(auto &it : left_expr){
        key.keys_.emplace_back(it->Evaluate(&produce_tuple , plan_->GetLeftPlan()->OutputSchema()));
      }
      //找到就输出，找不到就丢弃
      if(ht_.find(key) != ht_.end()){
        for(const auto &tuple : ht_[key]){
          vector<Value> values;
          for(uint32_t j = 0 ; j < left_count ; j++){
            values.push_back(produce_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema() , j));
          }
          for(uint32_t j = 0 ; j < right_count ; j++){
            values.push_back(tuple.GetValue(&plan_->GetRightPlan()->OutputSchema() , j));
          }
          output_.emplace_back(values , &GetOutputSchema());
        }
      }
    }
  }
  //设置迭代器指向output结果数组开头，Next靠这个迭代器取数据
  iterator_ = output_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == output_.end()) {
    return false;
  }
  *tuple = *iterator_;
  iterator_++;
  return true;
}

}  // namespace bustub
