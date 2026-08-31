//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

//分配一个新page并访问
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  const std::lock_guard<std::mutex> guard(latch_);
  int new_page_id = AllocatePage();
  frame_id_t frame_id;

  //可用page
  if(!free_list_.empty()){
    //先从空闲列表中找
    frame_id = free_list_.back();
    free_list_.pop_back();
  }else{
    //在替换器中驱逐
    if(!replacer_->Evict(&frame_id)){
      return nullptr;
    }
    if(pages_[frame_id].IsDirty()){
      disk_manager_->WritePage(pages_[frame_id].GetPageId(),pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }
  page_table_[new_page_id] = frame_id;
  auto &current_page = pages_[frame_id];
  current_page.page_id_ = new_page_id;
  current_page.ResetMemory();  //这一页数据清零
  current_page.is_dirty_ = false;
  current_page.pin_count_ = 1;


  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id , false);
  *page_id = new_page_id;
  return &current_page;
}

//访问一个已有的page
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  const std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  
  //如果page在缓存中
  if(page_table_.find(page_id) != page_table_.end()){
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    //pin>0不能被驱逐
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id , false);
    return &pages_[frame_id];
  }

  //为该page分配frame
  if(!free_list_.empty()){
    frame_id = free_list_.back();
    free_list_.pop_back();
  }else{
    if(!replacer_->Evict(&frame_id)){
      return nullptr;
    }
    if(pages_[frame_id].IsDirty()){
      disk_manager_->WritePage(pages_[frame_id].GetPageId() , pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }

  page_table_[page_id] = frame_id;

  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].page_id_ = page_id;

  disk_manager_->ReadPage(page_id , pages_[frame_id].data_);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id , false);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  if(page_id == INVALID_PAGE_ID){
    return false;
  }
  if(page_table_.find(page_id) == page_table_.end()){
    return false;
  }
  auto frame_id = page_table_[page_id];

  pages_[frame_id].is_dirty_ |= is_dirty;
  if(pages_[frame_id].pin_count_>0){
    pages_[frame_id].pin_count_--;
    if(pages_[frame_id].pin_count_ == 0){
      replacer_->SetEvictable(frame_id , true);
    }
    return true;
  }
  return false;
}


//将指定的page数据刷新到磁盘
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  //判断page_id是否有效
  if(page_id == INVALID_PAGE_ID){
    return false;
  }
  //是否在缓冲区
  if(page_table_.find(page_id) == page_table_.end()){
    return false;
  }
  auto frame_id = page_table_[page_id];

  //刷盘
  disk_manager_->WritePage(pages_[frame_id].GetPageId() , pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  const std::lock_guard<std::mutex> guard(latch_);
  for(size_t i = 0 ; i<pool_size_ ; i++){
    if(pages_[i].is_dirty_ == true && pages_[i].page_id_ != INVALID_PAGE_ID){
      disk_manager_->WritePage(pages_[i].GetPageId() , pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  if(page_table_.find(page_id) == page_table_.end()){
    return true;
  }
  auto frame_id = page_table_[page_id];
  if(pages_[frame_id].pin_count_ != 0){
    return false;
  }
  //删除前先判断需不需要刷盘
  if(pages_[frame_id].IsDirty()){
    disk_manager_->WritePage(pages_[frame_id].GetPageId() , pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }

  //page中的信息清空
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

//调取一个普通页面
auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

//调取一个读页面
auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);  //抓取一个页面
  page->RLatch();   //加读锁
  return {this, page};
}

//调取一个写页面
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

//创建一个普通页面
auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
