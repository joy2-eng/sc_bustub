//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

using namespace std;

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  void RightChildInit();
  void LeftChildInit();

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  //记录两表的信息
  unique_ptr<AbstractExecutor> left_executor_;
  unique_ptr<AbstractExecutor> right_executor_;
  //对两表都建立一个缓存   因为next无法返回重新遍历，所以用一个缓存代替
  vector<pair<Tuple , RID>> left_tuples_;
  vector<pair<Tuple , RID>> right_tuples_;
  //对两表的索引 ， 如果输出一个tuple后右表没有遍历到最后，需要继续遍历不能拿下一个next
  uint32_t left_index_;
  uint32_t right_index_;
  /**
   * 1. `is_null_ = true`：**专门给 LEFT JOIN 用的标记**。
   * 含义：对于当前这一条左元组，是否**还没有匹配到任何右元组**。
    true：还没匹配到；一旦匹配成功，置为 false。
    循环结束后，如果 is_null_仍然是 true：左表这条记录没有匹配，LEFT JOIN 要输出【左元组 + 全部 null 的右元组】
   */
  bool is_null_{true};
};

}  // namespace bustub
