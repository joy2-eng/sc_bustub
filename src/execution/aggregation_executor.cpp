//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

using namespace std;

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_(move(child)) ,
    //哈希表初始化，key存放的是分组key(vector)  value存放的是当前聚集值vector
    aht_(plan->GetAggregates(),plan->GetAggregateTypes()) , 
    //初始化迭代器指向开头
    aht_iterator_(aht_.Begin()){}

void AggregationExecutor::Init() {
    //Init函数的任务是初始化哈希表，将key和value放入
    child_->Init();
    Tuple tuple;
    RID rid;
    //从子结点获取tuple和rid
    while (child_->Next(&tuple , &rid))
    {
        auto aggregate_key = MakeAggregateKey(&tuple);
        auto aggregate_value = MakeAggregateValue(&tuple);
        aht_.InsertCombine(aggregate_key , aggregate_value);
    }
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //获取格式
    Schema schema(plan_->OutputSchema());
    if(aht_iterator_ != aht_.End()){
        //每次获取一个hashmap中的key，先把key放入数组
        vector<Value> value(aht_iterator_.Key().group_bys_);
        //再放数据
        for(const auto &aggergate : aht_iterator_.Val().aggregates_){
            value.push_back(aggergate);
        }
        //将数组传给上一层
        *tuple = {value , &schema};
        ++aht_iterator_;
        successful_ = true;
        return true;
    }

    //处理空表情况， 当前为空表，且想获得统计信息时 ， 只有countstar返回0 ，其他返回null
    //例如空表执行 select count(*) from t1
    if(!successful_){
        successful_ = true;
        //groupbys为空会打印出一行数据，否则直接return false
        if(plan_->group_bys_.empty()){
            vector<Value> value;
            for(auto aggregate : plan_->agg_types_){
                switch (aggregate) {
                    case AggregationType::CountStarAggregate:
                        value.push_back(ValueFactory::GetIntegerValue(0));
                        break;
                    case AggregationType::CountAggregate:
                    case AggregationType::SumAggregate:
                    case AggregationType::MinAggregate:
                    case AggregationType::MaxAggregate:
                        value.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
                        break;
                }
            }
            *tuple = {value , &schema};
            successful_ = true;
            return true;
        }
        return false;
    }   
    return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
