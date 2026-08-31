#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
using namespace bustub;
using namespace std;


namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

Context::~Context(){
  write_set_.clear();
  read_set_.clear();
  header_page_.reset();
  root_page_id_  = INVALID_PAGE_ID;
}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }



/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  bool is_success;
  //cout << key << endl ;
  //得到最终插入的叶子页面
  page_id_t leaf_page_id = InsertGetKeyAt(key , comparator_ , ctx);
  //cout << "get leaf_page_id = " << leaf_page_id << endl;

  //获取叶子的写锁和页面指针
  WritePageGuard leaf_page_guard = move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *basic_page = leaf_page_guard.AsMut<BPlusTreePage>();
  auto *leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(basic_page);
  
  //找到插入位置
  int index = leaf_page->Lookup(key , comparator_);
  //cout<< "叶子节点插入位置： " << index << endl;

  //已经存在不能重复插入
  if(index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index) , key) == 0){
    return false;
  }else{
    //空间够直接添加
    if((leaf_page->GetSize() + 1) <= leaf_page->GetMaxSize()){
      leaf_page->Insert(key , value , comparator_);
    }else{
      //创建新叶子
      //cout << "创建" <<endl;
      page_id_t leaf_page_id_new;
      bpm_->NewPageGuarded(&leaf_page_id_new); //为其分配一个新页面
      //cout << "新页面 = " << leaf_page_id_new <<endl; 
      auto leaf_page_new_guard = bpm_->FetchPageWrite(leaf_page_id_new);
      //cout << "success" << endl;
      auto *basic_page_new = leaf_page_new_guard.AsMut<BPlusTreePage>();
      auto *leaf_page_new = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(basic_page_new);
      //初始化
      leaf_page_new->SetMaxSize(leaf_max_size_);
      leaf_page_new->SetSize(0);
      leaf_page_new->SetPageType(IndexPageType::LEAF_PAGE);
      leaf_page_new->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->MoveHalfTo(leaf_page_new);
      leaf_page->SetNextPageId(leaf_page_id_new);
      if(index <= (leaf_page->GetMaxSize() - 1) / 2){
        //插入左边
        leaf_page->Insert(key , value , comparator_);
      }else{
        //插入右边
        leaf_page_new->Insert(key , value, comparator_);
        leaf_page_new->MoveFirstToEndOf(leaf_page);
      }
      //将右叶子的0号位置key向上传
      KeyType mid_key = leaf_page_new->KeyAt(0);
      InsertIntoParent(leaf_page_id , mid_key , leaf_page_id_new , ctx);
    }
    is_success = true;
  }
  //cout << "end\n";
  return is_success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  page_id_t leaf_page_id = DeleteGetKeyAt(key , comparator_ , ctx);
  if(ctx.root_page_id_ == INVALID_PAGE_ID){
    return;
  }
  RemoveEntry(leaf_page_id , key , ctx);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  auto root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  ReadPageGuard root_page_guard = bpm_->FetchPageRead(root_page_id);
  auto *root_page = root_page_guard.As<BPlusTree::InternalPage>();
  while (!root_page->IsLeafPage()) {
    root_page_id = root_page->ValueAt(0);
    if (root_page_id != INVALID_PAGE_ID) {
      root_page_guard = bpm_->FetchPageRead(root_page_id);
      root_page = root_page_guard.As<BPlusTree::InternalPage>();
    }
  }
  auto *leaf_page = root_page_guard.As<BPlusTree::LeafPage>();
  return INDEXITERATOR_TYPE(bpm_, leaf_page, 0, std::move(root_page_guard));
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  (void)ctx;
  auto page_id = GetKeyAt(key, comparator_, ctx);
  if (page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  ReadPageGuard leaf_page_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  const auto *leaf_page = leaf_page_guard.As<BPlusTree::LeafPage>();
  int index = leaf_page->Lookup(key, comparator_);
  if (comparator_(leaf_page->KeyAt(index), key) != 0) {
    return INDEXITERATOR_TYPE();
  }
  return INDEXITERATOR_TYPE(bpm_, leaf_page, index, std::move(leaf_page_guard));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, nullptr, -1, ReadPageGuard());}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *p_header_page = page_guard.As<BPlusTreeHeaderPage>();
  return p_header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}


//寻找key所在的叶子
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t{
  //根据header_page_id 找当前root_page_id
  ReadPageGuard header_page_guard = bpm_ -> FetchPageRead(header_page_id_); //获取header page id的读锁
  auto *p_header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = p_header_page->root_page_id_;

  if(root_page_id == INVALID_PAGE_ID){
    return INVALID_PAGE_ID;
  }
  ctx.root_page_id_ = root_page_id;
  ctx.read_set_.push_back(move(header_page_guard)); //headerpage 的锁丢进去

  //获取root page id 的读锁
  ReadPageGuard root_page_guard = bpm_->FetchPageRead(root_page_id);
  auto *root_page = root_page_guard.As<BPlusTree::InternalPage>();
  ctx.read_set_.push_back(move(root_page_guard)); //rootpage 的锁丢进去

  while(!root_page->IsLeafPage()){
    int i = root_page -> Lookup(key , comparator);
    if(i != root_page->GetSize() && comparator(key , root_page->KeyAt(i)) == 0) {
      root_page_id = root_page->GetValue(i);
    }else{
      root_page_id = root_page->GetValue(i-1);
    }
    root_page_guard = bpm_->FetchPageRead(root_page_id);
    root_page = root_page_guard.As<BPlusTree::InternalPage>();
    ctx.read_set_.push_back(move(root_page_guard));
    //再释放头部锁
    ctx.read_set_.pop_front();
  }
  return root_page_id;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
//查找关键函数
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool{
  Context ctx;
  (void)ctx;

  //获取子页面id
  page_id_t leaf_page_id = GetKeyAt(key , comparator_ , ctx);
  if(leaf_page_id == INVALID_PAGE_ID){
    return false;
  }
  //先获取到叶子页面的守卫
  auto leaf_page_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  //从叶子守卫中获取真实的叶子页面
  auto *leaf_page = leaf_page_guard.As<LeafPage>();
  //在叶子中进行查找
  int i = leaf_page -> Lookup(key , comparator_);
  bool is_success = false;

  if(i >= 0 && i < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(i) , key) == 0){
    BUSTUB_ASSERT(result != nullptr , "result not nullptr");
    result->push_back(leaf_page->ValueAt(i));
    is_success = true;
  }
  return is_success;
}

//这个函数和getkeyat相似，只不过这里加的是写锁
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertGetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t{
  //取出root_page_id
  //cout << "fetch header\n";
  //cout << "header_page_id:" << header_page_id_ << endl;
  auto header_page_guard = bpm_ -> FetchPageWrite(header_page_id_);
  //cout << "success\n";
  auto *header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
  ctx.header_page_ = std::move(header_page_guard);
  page_id_t root_page_id = header_page -> root_page_id_;
  //std:://cout << "[InsertGetKeyAt] root_page_id=" << root_page_id 
          //<< " header_page_id_=" << header_page_id_ 
          //<< " INVALID=" << INVALID_PAGE_ID << std::endl;

  //没有树
  if(root_page_id == INVALID_PAGE_ID){
    bpm_ -> NewPageGuarded(&root_page_id);
    //cout << "fetch new root\n";
    //cout << "acquire page=" << root_page_id << endl;
    auto write_guard = bpm_ -> FetchPageWrite(root_page_id);
    //cout << "success\n";
    auto *p_leaf_page = write_guard.AsMut<BPlusTree::LeafPage>();
    p_leaf_page -> SetPageType(IndexPageType::LEAF_PAGE);
    p_leaf_page -> SetMaxSize(leaf_max_size_);
    p_leaf_page -> SetNextPageId(INVALID_PAGE_ID);
    p_leaf_page -> SetSize(0);
    SetRootPageId(root_page_id, ctx);
    //std::cout << "[LOCK3] acquire page=" << root_page_id 
          //<< " write_set_size=" << ctx.write_set_.size() << std::endl;
    ctx.write_set_.push_back(move(write_guard));
    //std::cout << "[LOCK3] acquire page=" << root_page_id 
          //<< " write_set_size=" << ctx.write_set_.size() << std::endl;
    ctx.root_page_id_ = root_page_id;
    return root_page_id;
  }
  ctx.root_page_id_ = root_page_id;
  //获取rootpage的写锁
  //cout << "fetch root\n";
  //cout << "acquire page=" << root_page_id << endl;
  WritePageGuard root_page_guard = bpm_->FetchPageWrite(root_page_id);
  //cout << "success\n";
  //将guard转为internalpage
  auto *root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
  //std::cout << "[LOCK1] acquire page=" << root_page_id 
          //<< " write_set_size=" << ctx.write_set_.size() << std::endl;
  ctx.write_set_.push_back(move(root_page_guard));
  //std::cout << "[LOCK1] acquire page=" << root_page_id 
          //<< " write_set_size=" << ctx.write_set_.size() << std::endl;
  while(!root_page->IsLeafPage()){
    int i = root_page->Lookup(key , comparator);
    if(i != root_page->GetSize() && comparator(key , root_page->KeyAt(i)) == 0){
      root_page_id = root_page->GetValue(i);
      //cout << 1 << endl;
    }else{
      root_page_id = root_page->GetValue(i - 1);
      //cout << 2 << endl;
    }
    //获取新页的写锁
    //cout << "fetch node\n";
    //cout << "acquire page=" << root_page_id << endl << "i is : " << i << endl;
    root_page_guard = bpm_->FetchPageWrite(root_page_id);
    //cout << "success\n";
    root_page = root_page_guard.AsMut<BPlusTree::InternalPage>(); 

    //螃蟹锁 如果中间某个节点的空间足够，即使下层传来新值也不会引发分裂 更不会向上传递，是一个安全节点
    //所以这种情况下上层节点都一定不会被修改，他们的写锁可以被释放
    if(root_page->GetSize() + 1 < root_page->GetMaxSize()){
      if(ctx.header_page_ != nullopt){
        ctx.header_page_.reset();
      }
      ctx.write_set_.clear();
    }
    //std::cout << "[LOCK2] acquire page=" << root_page_id 
          //<< " write_set_size=" << ctx.write_set_.size() << std::endl;
    ctx.write_set_.push_back(move(root_page_guard));
    //std::cout << "[LOCK2] acquire page=" << root_page_id 
          //<< " write_set_size=" << ctx.write_set_.size() << std::endl;
  }
  return root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t page_id, Context &ctx) {
  //  BUSTUB_ASSERT(ctx.header_page_!=std::nullopt,"ctx.header_page_==std::nullopt");
  auto guard = std::move(ctx.header_page_);
  auto *header_page = guard->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = page_id;
  ctx.root_page_id_ = page_id;  
  ctx.header_page_ = std::move(guard);
}

//发生分裂后向上传递
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(page_id_t leaf_page_left_id, KeyType key, page_id_t leaf_page_right_id, Context &ctx){
  //获取根节点
  page_id_t root_page_id = ctx.root_page_id_;

  // ==========分支1：旧根节点分裂，需要新建根节点==========
  /*- 原来的根节点变成**左孩子**(`leaf_page_left_id`)
- 分裂出来的新页面是**右孩子**(`leaf_page_right_id`)
- 必须**新建一层真正的根节点**，B + 树树高 +1*/
  if(root_page_id == leaf_page_left_id){
    page_id_t root_page_new_id; //创建新根

    bpm_->NewPageGuarded(&root_page_new_id);

    auto root_page_new_guard = bpm_->FetchPageWrite(root_page_new_id);
    auto *root_page_new = root_page_new_guard.AsMut<BPlusTreeInternalPage<KeyType , page_id_t , KeyComparator>>();
    root_page_new->SetPageType(IndexPageType::INTERNAL_PAGE);
    root_page_new->SetMaxSize(internal_max_size_);
    root_page_new->SetSize(0);
    root_page_new->InsertFirstOf(leaf_page_left_id); //增加新根指向第一个节点的索引
    root_page_new->Insert(key , leaf_page_right_id , comparator_);
    SetRootPageId(root_page_new_id, ctx);
  }else{    // ==========分支2：不是根分裂，处理普通父节点==========
    auto parent_page_guard = move(ctx.write_set_.back());
    page_id_t parent_page_id = parent_page_guard.PageId();
    ctx.write_set_.pop_back();
    auto *parent_page = parent_page_guard.template AsMut<BPlusTreeInternalPage<KeyType , page_id_t , KeyComparator>>();
    //cout << parent_page->GetSize() << " " << parent_page->GetMaxSize() <<endl;
    if(parent_page->GetSize() < parent_page->GetMaxSize()){
      //空间够直接插入
      //cout << 1 <<endl;
      //cout << parent_page->GetSize() << " " << parent_page->ValueAt(0) << parent_page->KeyAt(1) << " " << parent_page->ValueAt(1)<<endl;
      parent_page->Insert(key , leaf_page_right_id , comparator_);
      //cout << parent_page->GetSize() << " " << parent_page->ValueAt(0) << parent_page->KeyAt(1) << " " << parent_page->ValueAt(1) << " " << parent_page->KeyAt(2) 
      //<< " " << parent_page->ValueAt(2)<<endl;
    }else {  //内部节点进行分裂
      //cout << 2 <<endl;
      int index = parent_page->Lookup(key , comparator_); //假设不分裂会添加到哪个位置
      //cout << "index = " << index << endl;
      page_id_t parent_page_new_id;
      bpm_->NewPageGuarded(&parent_page_new_id);
      //cout << "新页面 = " <<parent_page_new_id << endl;
      auto parent_page_new_guard = bpm_->FetchPageWrite(parent_page_new_id);
      //cout << "success" << endl;
      auto parent_page_new = parent_page_new_guard.AsMut<InternalPage>();
      parent_page_new->SetPageType(IndexPageType::INTERNAL_PAGE);
      parent_page_new->SetMaxSize(internal_max_size_);
      parent_page_new->SetSize(0);
      //原父节点向新节点传递一半元素
      //cout << parent_page->GetSize() << " " << parent_page->ValueAt(0) << parent_page->KeyAt(1) << " " << parent_page->ValueAt(1) << " " << parent_page->KeyAt(2) 
      //<< " " << parent_page->ValueAt(2)<<endl;
      parent_page->MoveHalfTo(parent_page_new);
      //cout << parent_page_new->GetSize() <<parent_page_new->KeyAt(1) << " " << parent_page_new->ValueAt(1) << " " << parent_page_new->KeyAt(2) << " " << parent_page_new->ValueAt(2)<<endl;

      //判断插入左边或有右边
      if(index >= ((parent_page->GetMaxSize() + 1 ) + 1 ) / 2){
        //插入右边,插入右边需要将右边第一个元素移动到左边末尾，然后再插入
        parent_page_new->MoveFirstToEndOf(parent_page);
        parent_page_new->Insert(key , leaf_page_right_id , comparator_);
      }else{
        //插入左侧页面
        //cout << key << " " << leaf_page_right_id << endl;
        parent_page->Insert(key , leaf_page_right_id , comparator_);
      }
      //将[1]号位置上的key和value移动到[0]号位置，并把key删掉，只保留value
      KeyType mid_key = parent_page_new->KeyAt(1);
      page_id_t mid_page_id = parent_page_new->ValueAt(1);
      parent_page_new->EraseAt(1);
      parent_page_new->EraseAt(0);
      parent_page_new->InsertFirstOf(mid_page_id);
      //cout << parent_page_new->ValueAt(0) <<" " << parent_page_new->KeyAt(1) << " " << parent_page_new->ValueAt(1) <<endl;
      InsertIntoParent(parent_page_id , mid_key , parent_page_new_id , ctx);
    }
  }
}


//定位到删除key的页面
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteGetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t{
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
  ctx.header_page_ = std::move(header_page_guard);
  page_id_t root_page_id = header_page -> root_page_id_;
  ctx.root_page_id_ = root_page_id;
  if(ctx.root_page_id_ == INVALID_PAGE_ID){
    return INVALID_PAGE_ID;
  }
  WritePageGuard root_page_guard = bpm_->FetchPageWrite(root_page_id);
  auto *root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();

  ctx.write_set_.push_back(move(root_page_guard));
  while(!root_page->IsLeafPage()){
    int i = root_page->Lookup(key , comparator);
    if(i != root_page->GetSize() && comparator(key , root_page->KeyAt(i)) == 0){
      root_page_id = root_page->GetValue(i);
    }else{
      root_page_id = root_page->GetValue(i - 1);
    }
    root_page_guard = bpm_->FetchPageWrite(root_page_id);
    root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();

    //如果当前页面删除一个元素不会越过半满，则在此之前的页面也不会被修改了
    if(root_page->GetSize() - 1 >= root_page->GetMinSize()){
      ctx.write_set_.clear();
      ctx.header_page_.reset();
    }
    ctx.write_set_.push_back(move(root_page_guard));
  }
  return root_page_id;
}

//所有的删除逻辑
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(page_id_t basic_page_id, const KeyType &key, Context &ctx){
  //拿到当前页面的写锁
  WritePageGuard basic_page_guard = move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *basic_page = basic_page_guard.AsMut<BPlusTreePage>();

  //删除当前页面的指定key
  bool is_success = false;
  if(basic_page->IsLeafPage()){
    auto *leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(basic_page);
    is_success |= leaf_page->RemoveKeyAt(key, comparator_);
  }else{
    auto *internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
    is_success |= internal_page->RemoveKeyAt(key , comparator_);
  }
  if(!is_success){
    //该页面没有这个key
    return;
  }

  int root_page_id = ctx.root_page_id_;
  //根节点 size=0 整棵树空了，设置树为空，释放根页。
  if(basic_page_id == root_page_id && basic_page->GetSize() == 0){
    SetTreeEmpty(ctx);
    bpm_->DeletePage(root_page_id);
  }else if(basic_page_id == root_page_id && basic_page->GetSize() == 1 &&!basic_page->IsLeafPage()){
    //根是内部页，size=1：根节点只有一个指针，没有 key，根节点可以消掉，把它唯一的子节点作为新根，删除旧根页面
    auto *root_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
    SetRootPageId(root_page->ValueAt(0), ctx);
    bpm_->DeletePage(root_page_id);
  }else if(basic_page_id != root_page_id && basic_page->GetSize() < basic_page->GetMinSize()){
    //非根节点且不满足半满约束，考虑充分配还是合并
    //拿到当前页面的父页面的锁
    page_id_t parent_page_id = ctx.write_set_.back().PageId();
    BPlusTree::InternalPage *parent_page;
    WritePageGuard parent_page_guard;
    if(parent_page_id != INVALID_PAGE_ID){
      parent_page_guard = move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      parent_page = parent_page_guard.AsMut<BPlusTree::InternalPage>();
    }
    //获取兄弟页面和分隔key
    auto pair = GetSiblingPageId(parent_page , key , ctx);
    KeyType mid_key = pair.second;
    page_id_t sibling_id = pair.first;

    WritePageGuard sibling_page_guard = bpm_->FetchPageWrite(sibling_id);
    auto *sibling_page = sibling_page_guard.AsMut<BPlusTreePage>();

    if(sibling_page->GetSize() - 1 < sibling_page->GetMinSize()){
      //merge: 兄弟不够借了就合并
      int index = parent_page->Lookup(key , comparator_);
      
      if(index == 1 && comparator_(key , parent_page->KeyAt(1)) < 0){
        //sibling是basic的右兄弟，逻辑看GetSiblingPageId函数实现
        //统一让basic是右节点，sibling是左节点，进行交换
        swap(basic_page , sibling_page);
        WritePageGuard tmp = move(basic_page_guard);
        basic_page_guard = move(sibling_page_guard);
        sibling_page_guard = move(tmp);
        swap(basic_page_id , sibling_id);
      }
      if(!basic_page->IsLeafPage()){
        /*内部页面合并
        父节点的 mid_key（分隔键）要下放到合并后的子节点。
        1. 把父节点的`mid_key`插入到 sibling 内部页 与basic的0号位上的value组合成一个键值对
        2. 把 basic 所有剩下的 key‑pointer 全部 move 到 sibling；*/
        auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
        auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
        page_id_t mid_key_page_id  = basic_internal_page->ValueAt(0);
        sibling_internal_page->Insert(mid_key , mid_key_page_id , comparator_);
        basic_internal_page->MoveAllTo(sibling_internal_page);
      }else{
        //与leafpage sibling合并，直接move setnextpage即可
        auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
        auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
        basic_leaf_page->MoveAllTo(sibling_leaf_page);
        sibling_leaf_page->SetNextPageId(basic_leaf_page->GetNextPageId());
      }
      ctx.write_set_.push_back(move(parent_page_guard));
      RemoveEntry(parent_page_id , mid_key , ctx);
      bpm_->DeletePage(basic_page_id);
    }else{
      //borrow::兄弟节点够借就从兄弟节点借一个
      int index = parent_page->Lookup(key , comparator_);
      if(index == 1 && comparator_(key , parent_page->KeyAt(1)) < 0){
        //basic_page is previous of sibling_page
        if(!basic_page->IsLeafPage()){
          // 内部页借，从右兄弟拿第一个元素
          auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
          auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
          int m = 0;
          page_id_t first_page_id = sibling_internal_page->ValueAt(m);
          KeyType first_key = sibling_internal_page->KeyAt(m+1);
          //将父节点的midkey和兄弟节点的value[0]组成一个元素放入basic末尾
          basic_internal_page->Insert(mid_key , first_page_id , comparator_);
          //删掉0处元素整体前移一位
          sibling_internal_page->EraseAt(0);
          //0处的key设置为空
          sibling_internal_page->SetKeyAt(0 , KeyType());
          //将parentpage的mid_key替换为first_key,也就是将兄弟节点的第一个key上升
          ReplaceKeyAt(parent_page , mid_key , first_key , ctx);
        }else{
          //叶子
          auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
          auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
          //将兄弟节点的第一个元素移动给basicpage
          sibling_leaf_page->MoveFirstToEndOf(basic_leaf_page);
          //然后将sibling原本在1号位的key(现0号位)上移动
          KeyType second_key = sibling_leaf_page->KeyAt(0);
          ReplaceKeyAt(parent_page , mid_key , second_key , ctx);
        }
      }else{
        //sibling_page is previous of basic_page
        if(!basic_page->IsLeafPage()){
          //内部节点,将sibling的最后一个元素移动给basic
          auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
          auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
          int m = sibling_internal_page->GetSize() - 1;
          page_id_t last_page_id = sibling_internal_page->ValueAt(m);
          KeyType last_key = sibling_internal_page->KeyAt(m);
          /*过程：对于sibling的last_key last_value 
          basic_page 1号位的value 与 parent_page的mid_key结合为一个元素放入basic 1号位
          last_value 放入basic 0号位的value
          last_key 替换 parent_page的mid_key
          */
          sibling_internal_page->EraseAt(m);
          page_id_t basic_pointer_page_id = basic_internal_page->ValueAt(0);
          basic_internal_page->SetValueAt(0 , last_page_id);
          basic_internal_page->Insert(mid_key , basic_pointer_page_id , comparator_);
          ReplaceKeyAt(parent_page , mid_key , last_key , ctx);
        }else{
          //叶子
          /*
          叶子直接将last_key last_value放入basic 0号位
          last_key 替换 parent_page的mid_key
          */
          auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
          auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
          int m = sibling_leaf_page->GetSize() - 1;
          ValueType last_value = sibling_leaf_page->ValueAt(m);
          KeyType last_key = sibling_leaf_page->KeyAt(m);
          sibling_leaf_page->RemoveAt(m);
          basic_leaf_page->Insert(last_key , last_value , comparator_);
          ReplaceKeyAt(parent_page , mid_key , last_key ,ctx);
        }
      }
    }
  }
}

//将目标元素的key更换
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReplaceKeyAt(BPlusTree::InternalPage *page, const KeyType &src, const KeyType &dst, Context &ctx) {
  int index = page->Lookup(src, comparator_);
  BUSTUB_ASSERT(!(index < 0 || index >= page->GetSize()), "ReplaceKeyAt page_id source_key not in page");
  page->SetKeyAt(index, dst);
}

//树置为空
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetTreeEmpty(Context &ctx){
  auto header_page = move(ctx.header_page_);
  auto *p_header_page = header_page->AsMut<BPlusTreeHeaderPage>();
  p_header_page->root_page_id_ = INVALID_PAGE_ID;
  ctx.header_page_ = move(header_page);
}

//在父内部页中，找到当前节点的兄弟页 ID，以及父子之间的分隔 key
//只有最后一种情况返回的是右兄弟
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSiblingPageId(const BPlusTree::InternalPage *parent_page, const KeyType &key, Context &ctx)
-> std::pair<page_id_t, KeyType>{
  page_id_t sibling_page_id;
  KeyType parent_key;
  int index = parent_page->Lookup(key, comparator_);
  int n = parent_page->GetSize();
  if (index == n) {
    sibling_page_id = parent_page->ValueAt(index - 2);
    parent_key = parent_page->KeyAt(index - 1);
  } else if (index > 1 && index <= n - 1) {
    if (comparator_(key, parent_page->KeyAt(index)) == 0) {
      sibling_page_id = parent_page->ValueAt(index - 1);
      parent_key = parent_page->KeyAt(index);
    } else {
      sibling_page_id = parent_page->ValueAt(index - 2);
      parent_key = parent_page->KeyAt(index - 1);
    }
  } else {
    if (comparator_(key, parent_page->KeyAt(index)) == 0) {
      sibling_page_id = parent_page->ValueAt(index - 1);
      parent_key = parent_page->KeyAt(index);
    } else {
      sibling_page_id = parent_page->ValueAt(index);
      parent_key = parent_page->KeyAt(index);
    }
  }
  return std::make_pair(sibling_page_id, parent_key);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
