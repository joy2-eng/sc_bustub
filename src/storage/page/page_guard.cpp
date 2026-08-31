#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    if(this->bpm_ != nullptr){
        Drop();
    }
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;

    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
}

//页面使用完对页面的处理
void BasicPageGuard::Drop() {
    if(bpm_ != nullptr && page_ != nullptr){
        bpm_->UnpinPage(page_->GetPageId() , is_dirty_);
        bpm_ = nullptr;
        page_ = nullptr;
        is_dirty_ = false;
    }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
    if(this != &that){
        //清空数据
        Drop();
        //转移数据
        this->bpm_ = that.bpm_;
        this->page_ = that.page_;
        this->is_dirty_ = that.is_dirty_;

        //置为默认值
        that.bpm_ = nullptr;
        that.page_ = nullptr;
        that.is_dirty_ = false;
    }
    return *this;
}

BasicPageGuard::~BasicPageGuard(){Drop();};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept{
    Drop();
    guard_ = std::move(that.guard_);
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
    Drop();
    guard_ = std::move(that.guard_);    
    return *this;
}

//read后释放页面
void ReadPageGuard::Drop() {
    if(guard_.page_ == nullptr){
        return;
    }
    //先解锁再unpin
    guard_.page_->RUnlatch();
    guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {Drop();}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept{
    Drop();
    guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
    Drop();
    guard_ = std::move(that.guard_);
    return *this;
}

//write后释放页面
void WritePageGuard::Drop() {
    if(guard_.page_ == nullptr){
        return;
    }
    guard_.page_->WUnlatch();
    guard_.Drop();
}

WritePageGuard::~WritePageGuard() {Drop();}  // NOLINT

}  // namespace bustub
