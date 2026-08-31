#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,
    plan_(plan),
    child_executor_(move(child_executor)) {}

void TopNExecutor::Init() {
    // 初始化子执行器，让下层算子（比如SeqScan）跑起来 ， 后面next正常拿数据
    child_executor_->Init();

    Tuple producer_tuple;
    RID producer_rid;

    //为priority_queue定义lambda比较器
    auto comp = [this](const Tuple &left_tuple , const Tuple &right_tuple) {
        bool flag = false;
        //遍历每一个order by 排序键
        for(auto &it : plan_->order_bys_){  
            //it.first 是排序方式（ASC/DESC）
            //it.second 是tuple中要排序的字段值
            const Value left_value = it.second->Evaluate(&left_tuple , child_executor_->GetOutputSchema());
            const Value right_value = it.second->Evaluate(&right_tuple , child_executor_->GetOutputSchema());

            //判断两个值是否相等
            bool is_equal = left_value.CompareEquals(right_value) == CmpBool::CmpTrue;
            if(!is_equal){
                bool is_less_than = left_value.CompareLessThan(right_value) == CmpBool::CmpTrue;
                if(it.first == OrderByType::ASC || it.first == OrderByType::DEFAULT) {
                    //升序 ：left<right -> return true
                    flag = is_less_than;
                }else if(it.first == OrderByType::DESC) {
                    //降序：left>right -> return false
                    flag = !is_less_than;
                }else{
                    BUSTUB_ASSERT(true , "not enter here!");
                }
                break;

            }
        }
        return flag;
    }; 
    
    priority_queue<Tuple , vector<Tuple> , decltype(comp)> pq(comp);

    //优先队列维护最多plan_->n_个元素（limit数量）
    while(child_executor_ -> Next(&producer_tuple , &producer_rid)){
        if(pq.size() < this->plan_->n_) {
            //没满，直接压入
            pq.push(producer_tuple);
        } else {
            //满了，跟堆顶比较 ，差的退出
            if(comp(producer_tuple , pq.top())) {
                pq.pop();
                pq.push(producer_tuple);
            }
        }
    }

    //结果全部倒进output 因为堆顶放的是最茶元素，最后需要reverse反转
    //vector<Tuple> out_puts_;
    while(!pq.empty()){
        out_puts_.push_back(pq.top());
        pq.pop();
    }
    reverse(out_puts_.begin() , out_puts_.end());
    //后面next就迭代这个iterator往外输出tuple
    iterator_ = out_puts_.begin();
}


auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == out_puts_.end()) {
    return false;
  }
  *tuple = *iterator_;
  iterator_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return out_puts_.size(); };



}  // namespace bustub
