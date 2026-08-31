//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
using namespace std;

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ValueType value = array_[index].second;
  return value;
}

//二分查找目标key的下标
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 1;  //内部节点的0处key无效
  int r = GetSize()-1;
  int ans = r + 1;

  while(l <= r){
    int mid = (l + r) >> 1;
    if(comparator(array_[mid].first , key) >= 0){
      ans = mid ;
      r = mid - 1;
    }else{
      l = mid + 1;
    }
  }
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value){
  array_[index].second = value;
}

//根据下标读取第index个子节点的页 ID，向下遍历的关键
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValue(int index) const -> page_id_t{
  return static_cast<page_id_t>(ValueAt(index));
}

//头部填充[0]的、指针value
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirstOf(const page_id_t &value){
  int n = GetSize();
  for(int i = n ; i > 0 ; i--){
    swap(array_[i] , array_[i-1]);
  }
  array_[0] = make_pair(KeyType() , value);
  IncreaseSize(1);
}

//插入成功返回插入下标 因为满了不能插入则返回-1 重复插入返回-2
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const page_id_t &value, const KeyComparator &comparator) -> int{
  int index = Lookup(key, comparator);
  MappingType tmp = array_[index];
  int n = GetSize();
  array_[index] = std::make_pair(key, value);
  for (int i = index + 1; i <= n; i++) {
    std::swap(tmp, array_[i]);
  }
  IncreaseSize(1);
  return GetSize();
}

//根据下标直接删除一个分界 Key + 子节点指针
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseAt(int index){
  int n = GetSize();
  for(int i = index ; i < n - 1 ; i++){
    swap(array_[i] , array_[i+1]);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKeyAt(const KeyType &key, const KeyComparator &comparator) -> bool{
  int index = Lookup(key , comparator);
  int n = GetSize();
  bool is_success = false;
  if(index >= 0 && index < n &&comparator(array_[index].first , key) == 0){
    EraseAt(index);
    is_success = true;
  }
  return is_success;
}

//用于将旧节点的一半元素移动到新节点(recipient代表新节点)
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient){
  int n = GetSize();
  int j = 1; //中间节点的【0】不存放key
  for(int i = n / 2 ; i < n ; i++){
    recipient->array_[j++] = array_[i];
  } // 实际拿走了j-1个元素
  //设置新节点的size_
  recipient->IncreaseSize(j);//【0】+ j-1
  //更新旧节点的size_
  this->IncreaseSize(-(j - 1));
}

//将本节点的第一个有效元素递给兄弟节点作为末尾元素  需要修改父结点，这里没实现
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient){
  int n = GetSize();
  MappingType tem = array_[1];
  //覆盖掉第一个
  for(int i = 1 ; i + 1 < n ; i++){
    array_[i] = array_[i+1];
  }
  int rn = recipient->GetSize();
  BUSTUB_ASSERT(rn +1 < recipient->GetMaxSize(),"B_PLUS_TREE_INTERNAL_PAGE_TYPE MoveFirstToEndOf recipient size + 1 < maxSize");
  recipient->array_[rn] = tem;
  recipient->IncreaseSize(1);
  this->IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  int n = GetSize();
  int rn = recipient->GetSize();
  BUSTUB_ASSERT(n + rn - 2 < GetMaxSize(), "MoveAllto throw Exception beacause n+rn-1>=InternalMaxSize");
  for (int i = 1; i < n; i++) {
    recipient->array_[rn++] = array_[i];
  }
  recipient->IncreaseSize(n - 1);
  this->IncreaseSize(-(n - 1));
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
