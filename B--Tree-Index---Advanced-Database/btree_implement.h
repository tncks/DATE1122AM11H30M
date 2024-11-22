#ifndef BTREE_IMPLEMENT_H
#define BTREE_IMPLEMENT_H

#include "btree_mgr.h"
#include "buffer_mgr.h"

// Structure that holds the actual data of an entry
typedef struct NodeData {
	RID rid;
} NodeData;

// Structure that represents a node in the B+ Tree
typedef struct Node {
	void ** pointers;
	Value ** keys;
	struct Node * parent;
	bool is_leaf;
	int num_keys;
	struct Node * next; // Used for queue.
} Node;

// Structure that stores additional information of B+ Tree
typedef struct BTreeManager {
	BM_BufferPool bufferPool;
	BM_PageHandle pageHandler;
	int order;
	int numNodes;
	int numEntries;
	Node * root;
	Node * queue;
	DataType keyType;
} BTreeManager;

//Structure that faciltates the scan operation on the B+ Tree
typedef struct ScanManager {
	int keyIndex;
	int totalKeys;
	int order;
	Node * node;
} ScanManager;









// Output and utility. (Functions to support printing of the B+ Tree & Functions to find an element (record) in the B+ Tree)

void enqueue(BTreeManager * treeManager, Node * new_node);
Node * dequeue(BTreeManager * treeManager);
int path_to_root(Node * root, Node * child);
// int find_range( node * root, int key_start, int key_end, bool verbose,
//         int returned_keys[], void * returned_pointers[]); 
Node * findLeaf(Node * root, Value * key);	 	   //node * find_leaf( node * root, int key, bool verbose );
NodeData * findRecord(Node * root, Value * key);   //record * find( node * root, int key, bool verbose );  //GOAL:  char * db_find(int64_t key); (api form)



// Insertion. (Functions to support addition of an element (record) in the B+ Tree)

NodeData * makeRecord(RID * rid);
Node * createNode(BTreeManager * treeManager);
Node * createLeaf(BTreeManager * treeManager);
int getLeftIndex(Node * parent, Node * left);
Node * insertIntoLeaf(BTreeManager * treeManager, Node * leaf, Value * key, NodeData * pointer);
Node * insertIntoLeafAfterSplitting(BTreeManager * treeManager, Node * leaf, Value * key, NodeData * pointer);

Node * insertIntoNode(BTreeManager * treeManager, Node * parent, int left_index, Value * key, Node * right);
Node * insertIntoNodeAfterSplitting(BTreeManager * treeManager, Node * parent, int left_index, Value * key, Node * right);

Node * insertIntoParent(BTreeManager * treeManager, Node * left, Value * key, Node * right);
Node * insertIntoNewRoot(BTreeManager * treeManager, Node * left, Value * key, Node * right);
Node * createNewTree(BTreeManager * treeManager, Value * key, NodeData * pointer);
// btree_mgr.c(h) -> RC insertKey(BTreeHandle *tree, Value *key, RID rid);  // node * insert( node * root, int key, int value );  //GOAL:  int db_insert(int64_t key, char * value); (api form)
// (step 1) I will change RC naming to the int (return type) and insertKey function name -> to the db_insert  
// (step 2) I will change char * value and  RID rid  adjustment for user usage(user call f with just char*string but internally function deal with change to struct form: RID)  (i mean, function parameter type customizing to fit GOAL)
// (step 3) I will implement the KRI (assignment algorithm), modifying the original code.
// (step 4) test.  (make && make run)



// Deletion. (Functions to support deleting of an element (record) in the B+ Tree)

int getNeighborIndex(Node * n);
Node * adjustRoot(Node * root);
Node * mergeNodes(BTreeManager * treeManager, Node * n, Node * neighbor, int neighbor_index, Value* k_prime);
Node * redistributeNodes(Node * root, Node * n, Node * neighbor, int neighbor_index, int k_prime_index, Value* k_prime);
Node * deleteEntry(BTreeManager * treeManager, Node * n, Value * key, void * pointer);
Node * delete(BTreeManager * treeManager, Value * key);  
// btree_mgr.c(h) -> RC deleteKey(BTreeHandle *tree, Value *key);    // node * db_delete( node * root, int key );  //GOAL:  int db_delete(int64_t key); (api form)
// (step 1) I will ..
// (step 2) I will ..

// void destroy_tree_nodes(node * root);   ->  uncomment it: need to implement or find relevant code in other area (in the future, later..)
// node * destroy_tree(node * root);       ->  uncomment it: need to implement or find relevant code in other area (in the future, later..) (these two lines)
Node * removeEntryFromNode(BTreeManager * treeManager, Node * n, Value * key, Node * pointer);


// Functions to support KEYS of multiple datatypes.
bool isLess(Value * key1, Value * key2);
bool isGreater(Value * key1, Value * key2);
bool isEqual(Value * key1, Value * key2);




/* USER API INTERFACE */

// marked as GOAL

/*
H_P * load_header(off_t off);
page * load_page(off_t off);

void reset(off_t off);
off_t new_page();
void freetouse(off_t fpo);
int cut(int length); // I will just no use this
int parser();        // ?: what's this 
void start_new_file(record rec);

int open_table(char * pathname);          // record_mgr.c(h)
char * db_find(int64_t key);			  // btree_implement.c(h)
int db_insert(int64_t key, char * value); // btree_implement.c(h) & btree_mgr.c(h)
int db_delete(int64_t key);				  // btree_implement.c(h) & btree_mgr.c(h)
*/

#endif // BTREE_IMPLEMENT_H
