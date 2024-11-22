#ifndef BPT_H
#define BPT_H
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <math.h>

/***********************************************************/
/***********************************************************/










/****** */ // dberror h [begin]
/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4

#define RC_PINNED_PAGES_IN_BUFFER 500 

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303
#define RC_BUFFER_ERROR 375

#define RC_ERROR 402
#define RC_PIN_PAGE_FAILED 403
#define RC_UNPIN_PAGE_FAILED 404
#define RC_MARK_DIRTY_FAILED 405
#define RC_BUFFER_SHUTDOWN_FAILED 406
#define RC_NULL_IP_PARAM 407
#define RC_FILE_DESTROY_FAILED 408
#define RC_IVALID_PAGE_SLOT_NUM 409
#define RC_SCHEMA_NOT_INIT 410
#define RC_MELLOC_MEM_ALLOC_FAILED 411
#define RC_BUFFER_PIN_PG 450

// Added new definitions for Record Manager
#define RC_RM_NO_TUPLE_WITH_GIVEN_RID 600
#define RC_SCAN_CONDITION_NOT_FOUND 601

// Added new definition for B-Tree
#define RC_ORDER_TOO_HIGH_FOR_PAGE 701
#define RC_INSERT_ERROR 702
#define RC_NO_RECORDS_TO_SCAN 703

#define RC_RM_NO_TUPLE_WITH_GIVEN_RID 600
#define RC_SCAN_CONDITION_NOT_FOUND 601

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(_rc,_message)						\
	do {										\
		RC_message=_message;					\
		return _rc;								\
	} while (0)									\

// check the return code and exit if it is an error
#define CHECK(_code)													\
	do {																\
		int _rc_internal = (_code);										\
		if (_rc_internal != RC_OK)										\
		{																\
			char *_message = errorMessage(_rc_internal);				\
			printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, _message); \
			free(_message);												\
			exit(1);													\
		}																\
	} while(0);

/****** */ // dberror h [end]
/****** */ // dt h [begin]
// define bool if not defined
#ifndef bool
    typedef short bool;
#define true 1
#define false 0
#endif

#define TRUE true
#define FALSE false
/****** */ // dt h [end]
/****** */ // tables h [begin]
// Data Types, Records, and Schemas
typedef enum DataType {
	DT_INT = 0,
	DT_STRING = 1
} DataType;

typedef struct Value {
	DataType dt;
	union v {
		int64_t intV;
		char *stringV;
	} v;
} Value;

typedef struct RID {
	int page; // 4
	int slot; // 4
} RID;  // total 8

typedef struct Record
{
	RID id;      // 8
	char *data;  // 120
} Record;

// information of a table schema: its attributes, datatypes, 
typedef struct Schema
{
	int numAttr;
	char **attrNames;
	DataType *dataTypes;
	int *typeLength;
	int *keyAttrs;
	int keySize;
} Schema;

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct RM_TableData
{
	char *name;
	Schema *schema;
	void *mgmtData;
} RM_TableData;

#define MAKE_STRING_VALUE(result, value)				\
		do {									\
			(result) = (Value *) malloc(sizeof(Value));				\
			(result)->dt = DT_STRING;						\
			(result)->v.stringV = (char *) malloc(strlen(value) + 1);		\
			strcpy((result)->v.stringV, value);					\
		} while(0);


#define MAKE_VALUE(result, datatype, value)				\
		do {									\
			(result) = (Value *) malloc(sizeof(Value));				\
			(result)->dt = datatype;						\
			switch(datatype)							\
			{									\
			case DT_INT:							\
			(result)->v.intV = value;					\
			break;								\
			}									\
		} while(0);


// debug and read methods
// extern Value *stringToValue (char *value);
// extern char *serializeTableInfo(RM_TableData *rel);
// extern char *serializeTableContent(RM_TableData *rel);
// extern char *serializeSchema(Schema *schema);
// extern char *serializeRecord(Record *record, Schema *schema);
// extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
// extern char *serializeValue(Value *val);
/****** */ // tables h [end]
/****** */ // expr h [begin]

// datatype for arguments of expressions used in conditions
typedef enum ExprType {
  EXPR_OP,
  EXPR_CONST,
  EXPR_ATTRREF
} ExprType;

typedef struct Expr {
  ExprType type;
  union expr {
    Value *cons;
    int attrRef;
    struct Operator *op;
  } expr;
} Expr;

// comparison operators
typedef enum OpType {
  OP_BOOL_AND,
  OP_BOOL_OR,
  OP_BOOL_NOT,
  OP_COMP_EQUAL,
  OP_COMP_SMALLER
} OpType;

typedef struct Operator {
  OpType type;
  Expr **args;
} Operator;

// expression evaluation methods
extern RC valueEquals (Value *left, Value *right, Value *result);
extern RC valueSmaller (Value *left, Value *right, Value *result);
extern RC boolNot (Value *input, Value *result);
extern RC boolAnd (Value *left, Value *right, Value *result);
extern RC boolOr (Value *left, Value *right, Value *result);
extern RC evalExpr (Record *record, Schema *schema, Expr *expr, Value **result);
extern RC freeExpr (Expr *expr);
extern void freeVal(Value *val);


#define CPVAL(_result,_input)						\
  do {									\
    (_result)->dt = _input->dt;						\
  switch(_input->dt)							\
    {									\
    case DT_INT:							\
      (_result)->v.intV = _input->v.intV;					\
      break;								\
    case DT_STRING:							\
      (_result)->v.stringV = (char *) malloc(strlen(_input->v.stringV) + 1);	\
      strcpy((_result)->v.stringV, _input->v.stringV);			\
      break;								\
    }									\
} while(0);

#define MAKE_BINOP_EXPR(_result,_left,_right,_optype)			\
    do {								\
      Operator *_op = (Operator *) malloc(sizeof(Operator));		\
      _result = (Expr *) malloc(sizeof(Expr));				\
      _result->type = EXPR_OP;						\
      _result->expr.op = _op;						\
      _op->type = _optype;						\
      _op->args = (Expr **) malloc(2 * sizeof(Expr*));			\
      _op->args[0] = _left;						\
      _op->args[1] = _right;						\
    } while (0);

#define MAKE_UNOP_EXPR(_result,_input,_optype)				\
  do {									\
    Operator *_op = (Operator *) malloc(sizeof(Operator));		\
    _result = (Expr *) malloc(sizeof(Expr));				\
    _result->type = EXPR_OP;						\
    _result->expr.op = _op;						\
    _op->type = _optype;						\
    _op->args = (Expr **) malloc(sizeof(Expr*));			\
    _op->args[0] = _input;						\
  } while (0);

#define MAKE_ATTRREF(_result,_attr)					\
  do {									\
    _result = (Expr *) malloc(sizeof(Expr));				\
    _result->type = EXPR_ATTRREF;					\
    _result->expr.attrRef = _attr;					\
  } while(0);

#define MAKE_CONS(_result,_value)					\
  do {									\
    _result = (Expr *) malloc(sizeof(Expr));				\
    _result->type = EXPR_CONST;						\
    _result->expr.cons = _value;					\
  } while(0);


/****** */ // expr h [end]
/****** */ // storage_mgr h [begin]
/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
	char *fileName;
	int totalNumPages;
	int curPagePos;
	void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *fHandle);
extern RC destroyPageFile (char *fileName);

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);
/****** */ // storage_mgr h [end]
/****** */ // btree_mgr h [begin]
// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);
/****** */ // btree_mgr h [end]
/****** */ // buffer_mgr h [begin]
// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
	RS_LRU = 1,
	RS_CLOCK = 2,
	RS_LFU = 3,
	RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
	char *pageFile;
	int numPages;
	ReplacementStrategy strategy;
	void *mgmtData; // use this one to store the bookkeeping info your buffer
	// manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
	PageNumber pageNum;
	char *data;
} BM_PageHandle;

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);
// Page Frame 
typedef struct bufferPageFrame
{
	bool dirty;
	int flNumber;
	SM_PageHandle data;
	int clientCount;
	PageNumber numOfPgs;
	int refNumber;
} bufferPageFrame;
/****** */ // buffer_mgr h [end]
/****** */ // buffer_mgr_stat h [begin]
// debug functions
void printPoolContent (BM_BufferPool *const bm);
void printPageContent (BM_PageHandle *const page);
char *sprintPoolContent (BM_BufferPool *const bm);
char *sprintPageContent (BM_PageHandle *const page);
/****** */ // buffer_mgr_stat h [end]
/****** */ // record_mgr h [begin]

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
  RM_TableData *rel;
  void *mgmtData;
} RM_ScanHandle;

// A data structure to represent Record Manager.
typedef struct RecordManager
{
	// Buffer Manager PageHandle
	BM_PageHandle pageHandle;
	// Buffer Manager Buffer Pool
	BM_BufferPool bufferPool;
	// Record ID
	RID recordID;
	// Holds the condition for scanning the records in the table
	Expr *condition;
	// Stores the total number of records in the table
	int totalRecordsInTable;
	// Stores the location of the first empty slots in table
	RID firstFreePage;
	// Stores the count of the number of records scanned
	int scanCount;
} RecordManager;

// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
RC attrOffset(Schema *schema, int attributeNumber, int *result);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);
/****** */ // record_mgr h [end]
/****** */ // test_helper h [begin]
// var to store the current test's name
extern char *testName;
#define traceDebug____ 0

// short cut for test information
#define TEST_INFO  __FILE__, testName, __LINE__, __TIME__

// check the return code and exit if it's and error
#define TEST_CHECK(_code)												\
	do {																\
		int _rc_internal = (_code);										\
		if (_rc_internal != RC_OK)										\
		{																\
			char *_message = errorMessage(_rc_internal);				\
			printf("[%s-%s-L%i-%s] FAILED: Operation returned error: %s\n",TEST_INFO, _message); \
			free(_message);												\
			exit(1);													\
		}																\
	} while(0);

// check whether two strings are equal
#define ASSERT_EQUALS_STRING(_expected,_real,_message)					\
	do {																\
		if (strcmp((_expected),(_real)) != 0)							\
		{																\
			printf("[%s-%s-L%i-%s] FAILED: expected <%s> but was <%s>: %s\n",TEST_INFO, _expected, _real, _message); \
			exit(1);													\
		}																\
		if(traceDebug____) printf("[%s-%s-L%i-%s] OK: expected <%s> and was <%s>: %s\n",TEST_INFO, _expected, _real, _message); \
	} while(0)

// check whether two ints are equals
#define ASSERT_EQUALS_INT(_expected,_real,_message)						\
	do {																\
		if ((_expected) != (_real))										\
		{																\
			printf("[%s-%s-L%i-%s] FAILED: expected <%i> but was <%i>: %s\n",TEST_INFO, _expected, _real, _message); \
			exit(1);													\
		}																\
		if(traceDebug____) printf("[%s-%s-L%i-%s] OK: expected <%i> and was <%i>: %s\n",TEST_INFO, _expected, _real, _message); \
	} while(0)

// check whether two ints are equals
#define ASSERT_TRUE(_real,_message)										\
	do {																\
		if (!(_real))													\
		{																\
			printf("[%s-%s-L%i-%s] FAILED: expected true: %s\n",TEST_INFO, _message); \
			exit(1);													\
		}																\
		if(traceDebug____) printf("[%s-%s-L%i-%s] OK: expected true: %s\n",TEST_INFO, _message); \
	} while(0)


// check that a method returns an error code
#define ASSERT_ERROR(_expected,_message)								\
	do {																\
		int result = (_expected);										\
		if (result == (RC_OK))											\
		{																\
			printf("[%s-%s-L%i-%s] FAILED: expected an error: %s\n",TEST_INFO, _message); \
			exit(1);													\
		}																\
		printf("[%s-%s-L%i-%s] OK: expected an error and was RC <%i>: %s\n",TEST_INFO,  result , _message); \
	} while(0)

// test worked
#define TEST_DONE()													\
	do {															\
		printf("[%s-%s-L%i-%s] OK: finished test\n\n",TEST_INFO);	\
	} while (0)

/****** */ // test_helper h [end]
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
char * db_find(int64_t key);			  // bpt.c(h)
int db_insert(int64_t key, char * value); // bpt.c(h) & btree_mgr.c(h)
int db_delete(int64_t key);				  // bpt.c(h) & btree_mgr.c(h)
*/

#endif // BPT_H
