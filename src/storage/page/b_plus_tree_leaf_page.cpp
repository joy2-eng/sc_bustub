//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
using namespace std;

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}


//返回下标
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> int{
  int l = 0;
  int r = GetSize()-1;
  int ans = r + 1;

  while(l <= r){
    int mid = (l + r) >> 1;
    if(comparator(array_[mid].first , key) >= 0){
      ans = mid;
      r = mid -1;
    }else{
      l = mid + 1;
    }
  }
  return ans ;
}

//获取下一个相邻leafpage的id
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE:: GetNextPageId() const -> page_id_t{
  return next_page_id_;
}

//设置下一个相邻leafpage的id
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE:: SetNextPageId(page_id_t next_page_id){
next_page_id_ = next_page_id;
}

//根据下标取出对应的key
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  KeyType key = array_[index].first;
  return key;
}

//根据下标取出对应的value
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType{
  ValueType value = array_[index].second;
  return value;
}

//获取整个键值对
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetObjAt(int index) const -> const MappingType &{
  const MappingType &res = array_[index];
  return res;
}

//根据下标直接删除键值对
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  int i = index;
  for(i = index ; i < GetSize()-1 ; i++){
    swap(array_[i] , array_[i + 1]);
  }
  IncreaseSize(-1);
}

//根据key删除
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKeyAt(const KeyType &key, const KeyComparator &comparator) -> bool{
  int index = Lookup(key , comparator);
  int n = GetSize();
  bool is_success = false;
  if(index >= 0 && index < n && comparator(key , array_[index].first) == 0){
    RemoveAt(index);
    is_success = true;
  }
  return is_success;
}

//插入成功返回插入下标 因为满了不能插入则返回-1 重复插入返回-2
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int{
      int index = Lookup(key , comparator);
      //重复插入
      if (index < GetSize() && comparator(key , array_[index].first) == 0){
        return -2;
      }
      if(GetSize() < GetMaxSize()){
        MappingType tem = make_pair(key , value);
        for(int i = GetSize() ; i > index ; i--){
          array_[i] = array_[i-1];
        }
        array_[index] = tem;
        IncreaseSize(1);
        return index;
      }else{
        return -1; //页满
      }
    }


//将本节点的后一半元素移动到目标节点
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient){
  int n = GetSize();
  int rn = recipient->GetSize();
  //如果越界则打印日志加引发崩溃
  if(rn + n/2 >= recipient->GetMaxSize()){
    printf("rn=%d n=%d Maxsize=%d\n", rn, n, recipient->GetMaxSize());
  }
  BUSTUB_ASSERT(rn + n/2 < recipient->GetMaxSize(), "Con not move half to recipient ");//不满足条件则退出
  int ri = 0 ;//叶子节点从0开始
  for(int i = n / 2 ; i < n ; i++){
    recipient->array_[ri++] = array_[i];
  }
  IncreaseSize(-ri);
  recipient->IncreaseSize(ri);
}

//右边的第一个移动到左边末尾  需要修改父结点，这里没实现
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient){
  int n = GetSize();
  if(n >= 1){
    MappingType tmp = array_[0];
    for(int i = 0 ; i < n ; i++){
      swap(array_[i] , array_[i+1]);
    }
    int rn = recipient->GetSize();
    recipient->array_[rn] = tmp;
    recipient->IncreaseSize(1);
    this->IncreaseSize(-1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  int n = GetSize();
  int rn = recipient->GetSize();
  BUSTUB_ASSERT(n + rn < GetMaxSize(), "leafPage MoveAllto function error because n+rn>=MaxSize");
  for (int i = 0; i < n; i++) {
    recipient->array_[rn++] = array_[i];
  }
  recipient->IncreaseSize(n);
  this->IncreaseSize(-n);
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
