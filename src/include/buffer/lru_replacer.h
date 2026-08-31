// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // lru_replacer.h
// //
// // Identification: src/include/buffer/lru_replacer.h
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #pragma once

// #include <list>
// #include <mutex>  // NOLINT
// #include <vector>

// #include "buffer/replacer.h"
// #include "common/config.h"
// #include "buffer/lru_k_replacer.h"

// namespace bustub {

// //构造函数
// LRUKReplacer::LRUKReplacer(size_t num_frames , size_t k):replacer_size_(num_frames),k_(k){
//   is_accessible_.resize(num_frames + 1);
//   current_size_ = 0;
// }

// //访问一个帧的函数
// void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type){
//   if(frame_id>static_cast<int>(replacer_size_)){
//     throw std::exception();
//   }
//   use_count_[frame_id]++;
//   if(use_count_[frame_id]==k_){
//     if(history_map_.count(frame_id) != 0U){
//       auto it = history_map_[frame_id];
//       history_list_.erase(it);
//     }
//     history_map_.erase(frame_id);
//     //从历史列表删除后加入到缓存列表
//     cache_list_.push_front(frame_id);
//     cache_map_[frame_id] = cache_list_.begin();
//   }else if(use_count_[frame_id] > k_){  //大于k且在cache中则提前到头部
//     if(cache_map_.count(frame_id) != 0U){
//       auto it = cache_map_[frame_id];
//       cache_list_.erase(it);
//     }
//     cache_list_.push_front(frame_id);
//     cache_map_[frame_id] = cache_list_.begin();
//   }else{
//     if (history_map_.count(frame_id) == 0U) {
//       history_list_.push_front(frame_id);
//       history_map_[frame_id] = history_list_.begin();
//     }
//   }
// }

// //设置是否可驱逐函数
// void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable){
//   std::lock_guard<std::mutex> guard(latch_);
//   if(frame_id>static_cast<int>(replacer_size_)){
//     throw std::exception();
//   }
//   if(use_count_[frame_id] == 0U){
//     return;
//   }
//   if(!is_accessible_[frame_id] && set_evictable){
//     current_size_++;
//   }
//   if(is_accessible_[frame_id] && !set_evictable){
//     current_size_--;
//   }
//   is_accessible_[frame_id] = set_evictable;
// }


// //驱逐函数
// auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
//   std::lock_guard<std::mutex> guard(latch_);

//   //先驱逐history_list
//   *frame_id = 0 ;
//   if(history_list_.empty() && cache_list_.empty()){
//     return false;
//   }

//   auto it = history_list_.end();
//   while(it != history_list_.begin()){
//     it--;
//     if(is_accessible_[*it] == false)
//     {
//       continue;
//     }
//     history_map_.erase(*it);
//     *frame_id = *it;
//     use_count_[*it] = 0;
//     is_accessible_[*it] = false;
//     current_size_--;
//     history_list_.erase(it);
//     return true;
//   }
//   it = cache_list_.end();
//   while(it != cache_list_.begin()){
//     it--;
//     if(is_accessible_[*it] == false){
//       continue;
//     }
//     *frame_id = *it;
//     use_count_[*it] = 0;
//     current_size_--;
//     is_accessible_[*it] = false;
//     cache_map_.erase(*it);
//     cache_list_.erase(it);
//     return true;
//   }
//   return false;
// }


// //定向驱逐函数
// void LRUKReplacer::Remove(frame_id_t frame_id){
//   std::lock_guard<std::mutex> guard(latch_);
//   if(!is_accessible_[frame_id]){
//     return;
//   }
//   if(frame_id > static_cast<int>(replacer_size_)){
//     return;
//   }
//   //数据记录删除
//   if(use_count_[frame_id] <k_){
//     //在history
//     auto it = history_map_[frame_id];
//     history_list_.erase(it);
//     history_map_.erase(frame_id);
//   }else{
//     //在cache 
//     auto it = cache_map_[frame_id];
//     cache_list_.erase(it);
//     cache_map_.erase(frame_id);
//   }
//   //使用次数清零
//   use_count_[frame_id] = 0;

//   //当前可驱逐大小减一
//   is_accessible_[frame_id] = false;
//   current_size_ --;
// }

// auto LRUKReplacer::Size() -> size_t { return current_size_; }

// }  





