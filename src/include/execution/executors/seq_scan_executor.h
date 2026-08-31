//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  std::unique_ptr<TableIterator>iterator_;
  TableInfo *table_info_ = nullptr;
};
}  // namespace bustub

/*
# p0.03-string-scan
./bin/bustub-sqllogictest ../test/sql/p0.03-string-scan.slt --verbose

# p3.00-primer
./bin/bustub-sqllogictest ../test/sql/p3.00-primer.slt --verbose

# p3.01-seqscan
./bin/bustub-sqllogictest ../test/sql/p3.01-seqscan.slt --verbose

# p3.02-insert（你当前调试）
./bin/bustub-sqllogictest ../test/sql/p3.02-insert.slt --verbose

# p3.03-update
./bin/bustub-sqllogictest ../test/sql/p3.03-update.slt --verbose

# p3.04-delete
./bin/bustub-sqllogictest ../test/sql/p3.04-delete.slt --verbose

# p3.05-index-scan
./bin/bustub-sqllogictest ../test/sql/p3.05-index-scan.slt --verbose

# p3.06-empty-table
./bin/bustub-sqllogictest ../test/sql/p3.06-empty-table.slt --verbose

# p3.07-simple-agg
./bin/bustub-sqllogictest ../test/sql/p3.07-simple-agg.slt --verbose

# p3.08-group-agg-1
./bin/bustub-sqllogictest ../test/sql/p3.08-group-agg-1.slt --verbose

# p3.09-group-agg-2
./bin/bustub-sqllogictest ../test/sql/p3.09-group-agg-2.slt --verbose

# p3.10-simple-join
./bin/bustub-sqllogictest ../test/sql/p3.10-simple-join.slt --verbose

# p3.11-multi-way-join
./bin/bustub-sqllogictest ../test/sql/p3.11-multi-way-join.slt --verbose

# p3.12-repeat-execute
./bin/bustub-sqllogictest ../test/sql/p3.12-repeat-execute.slt --verbose

# p3.13-nested-index-join
./bin/bustub-sqllogictest ../test/sql/p3.13-nested-index-join.slt --verbose

# p3.14-hash-join
./bin/bustub-sqllogictest ../test/sql/p3.14-hash-join.slt --verbose



# p3.16-sort-limit
./bin/bustub-sqllogictest ../test/sql/p3.16-sort-limit.slt --verbose

# p3.17-topn
./bin/bustub-sqllogictest ../test/sql/p3.17-topn.slt --verbose

# p3.18-integration-1
./bin/bustub-sqllogictest ../test/sql/p3.18-integration-1.slt --verbose

# p3.19-integration-2
./bin/bustub-sqllogictest ../test/sql/p3.19-integration-2.slt --verbose

# leaderboard q1/q2/q3
./bin/bustub-sqllogictest ../test/sql/p3.leaderboard-q1.slt --verbose
./bin/bustub-sqllogictest ../test/sql/p3.leaderboard-q2.slt --verbose
./bin/bustub-sqllogictest ../test/sql/p3.leaderboard-q3.slt --verbose

# subquery
./bin/bustub-sqllogictest ../test/sql/subquery.slt --verbose

# update.slt
./bin/bustub-sqllogictest ../test/sql/update.slt --verbose
*/
