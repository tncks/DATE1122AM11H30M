#include "dberror.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include "btree_implement.h"
#include "stdlib.h"

BTreeManager * treeMgr = NULL;

// Initializing Index Manager.
extern RC initIndexManager(void *mgmtData) {
	initStorageManager();
	printf("Storage Manager Initialized\n");
	if(mgmtData == NULL) printf(" ");
	return RC_OK;
}

// This is used for shutting down the Index Manager.
extern RC shutdownIndexManager() {
	treeMgr = NULL;
	printf("Index Manager Shut Down\n");
	return RC_OK;
}

// B+ tree with name "idxId: is created using this function"
RC createBtree(char *idxId, DataType keyType, int n) {
	if ((long unsigned int) n < (long unsigned int) PAGE_SIZE / sizeof(Node)) {
		// Initialize the members of our B+ Tree metadata structure.
		treeMgr = (BTreeManager *) calloc(1,sizeof(BTreeManager));
		treeMgr->numNodes = 0;		// No nodes initially.
		treeMgr->order = n + 2;		// Setting order of B+ Tree
		treeMgr->queue = NULL;		// No node for printing
		treeMgr->keyType = keyType;	// Set datatype to "keyType"
		treeMgr->root = NULL;		// No root node
		treeMgr->numEntries = 0;	// No entries initially
		

		// Initializing Buffer Manager
		BM_BufferPool * bufferMgr = (BM_BufferPool *) calloc(1, sizeof(BM_BufferPool));
		treeMgr->bufferPool = *bufferMgr;
	}
	else
	{
		return RC_ORDER_TOO_HIGH_FOR_PAGE;
	}
	

	char data[PAGE_SIZE];
	SM_FileHandle fileHandlPtr;

	// Create page file. 
	RC createPgFileResult = createPageFile(idxId);
	if (createPgFileResult == RC_OK)
		return createPgFileResult;

	// Open page file.  
	RC openPgFileResult = openPageFile(idxId, &fileHandlPtr);
	if (openPgFileResult != RC_OK)
		return openPgFileResult;

	// Write to the page. 
	RC writeBlockResult = writeBlock(0, &fileHandlPtr, data);
	if (writeBlockResult != RC_OK)
		return writeBlockResult;

	// Close page file. 
	RC closePgFileResult = closePageFile(&fileHandlPtr);
	if (closePgFileResult != RC_OK)
		return closePgFileResult;





	return RC_OK;
}

// From specified page "idxId" an existing B+ Tree is opened
RC openBtree(BTreeHandle **tree, char *idxId) {
	*tree = (BTreeHandle *) calloc(1,sizeof(BTreeHandle));
	(*tree)->mgmtData = treeMgr;


	if (RC_OK != initBufferPool(&treeMgr->bufferPool, idxId, 1000, RS_FIFO, NULL)) {
		return RC_BUFFER_ERROR;
	}
	else
	{
		return RC_OK;
	}
	
}

// Closing down of the B+ tree
RC closeBtree(BTreeHandle *tree) {
	BTreeManager * treeMgr = (BTreeManager*) tree->mgmtData;
	free(treeMgr);
	return RC_OK;
}

// Deletion of B+ tree
RC deleteBtree(char *idxId) {
	RC destroyPgFileResult = destroyPageFile(idxId);
	if (destroyPgFileResult == RC_OK){
		return RC_OK;
	}
	else
	{
		return destroyPgFileResult;
	}
}

// New entry/record is added
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
	BTreeManager *tMgr = (BTreeManager *) tree->mgmtData;

	if (tMgr->root == NULL) {
		tMgr->root = createNewTree(tMgr, key, makeRecord(&rid));
		return RC_OK;
	}
	

	// DEBUG START POINT
	for(int c=0; c < tMgr->root->num_keys; c++){
		if(tMgr->root->keys[c] != NULL) {
			Value *my__k = (Value *) tMgr->root->keys[c];

			if(my__k->dt == DT_INT) {
				;
			}
		}
	}
	// DEBUG LOOP END POINT
	Node * bTreeLeaf = findLeaf(tMgr->root, key);

	if (bTreeLeaf->num_keys < tMgr->order - 1) {
		bTreeLeaf = insertIntoLeaf(tMgr, bTreeLeaf, key, makeRecord(&rid));
	} else {
		tMgr->root = insertIntoLeafAfterSplitting(tMgr, bTreeLeaf, key, makeRecord(&rid));
	}

	return RC_OK;
}

// Stores the RID after seraching the B+ Tree for the specified key
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
	BTreeManager *tMgr = (BTreeManager *) tree->mgmtData;

	if (NULL != findRecord(tMgr->root, key)) {
		printf("Record Found!\n");
	} 
	else
	{
		return RC_IM_KEY_NOT_FOUND;
	}
	
	*result = findRecord(tMgr->root, key)->rid;
	return RC_OK;
}

// Number of nodes present in the B+ Tree are retreived 
extern RC getNumNodes(BTreeHandle *tree, int *result) {
	BTreeManager * tMgr = (BTreeManager *) tree->mgmtData;
	printf("Number of nodes present in the B+ Tree are retreived ");
	*result = tMgr->numNodes;
	return RC_OK;
}

// Number of entries present in the B+ Tree are retreived byt his function
extern RC getNumEntries(BTreeHandle *tree, int *result) {
	BTreeManager * tMgr = (BTreeManager *) tree->mgmtData;
	int debugmode = 0;
	if(debugmode) printf("Number of entries present in the B+ Tree are retreived by his function");
	*result = tMgr->numEntries;
	return RC_OK;
}

// datatype of the keys in the B+ Tree are retreived byt his function
extern RC getKeyType(BTreeHandle *tree, DataType *result) {
	BTreeManager * tMgr = (BTreeManager *) tree->mgmtData;
	printf("datatype of the keys in the B+ Tree are retreived byt his function");
	*result = tMgr->keyType;
	return RC_OK;
}

// In B+tree that record is deleted whose key is specified
RC deleteKey(BTreeHandle *tree, Value *key) {
	BTreeManager *tMgr = (BTreeManager *) tree->mgmtData;
	Node* delRec = delete(tMgr, key);
	if (key != NULL)
	{
		RC_FILE_DESTROY_FAILED;
	}
	else
	{
		tMgr->root = delRec;
	}
	
	return RC_OK;
}

// Function which is used to scan the entries in the B+ Tree is initialized
extern RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
	if(tree == NULL) printf(" ");
	// BTreeManager *tMgr = (BTreeManager *) tree->mgmtData;
	*handle = calloc(1, sizeof(BT_ScanHandle));
	ScanManager *sMeta = calloc(1,sizeof(ScanManager));
	Node * node = treeMgr->root;

	if (treeMgr->root == NULL) {
		return RC_NO_RECORDS_TO_SCAN;
	} else {
		for (; !node->is_leaf; ) {
    node = node->pointers[0];
    }

		(*handle)->mgmtData = sMeta;
	sMeta->keyIndex = 0;
	sMeta->totalKeys = node->num_keys;
	sMeta->node = node;
	sMeta->order = treeMgr->order;

	return RC_OK;
	}
}

// B+ Tree entries traversal is done using this function
RC nextEntry(BT_ScanHandle *handle, RID *result) {
	ScanManager * sMeta = (ScanManager *) handle->mgmtData;

	Node * n = sMeta->node;

	// Return error if current node is empty i.e. NULL
	if (sMeta->node != NULL) {
		if (sMeta->keyIndex < sMeta->totalKeys) {
		((NodeData *) n->pointers[0])->rid = ((NodeData *) n->pointers[sMeta->keyIndex])->rid;
		sMeta->keyIndex++;
	} else {
		// If all the entries on the leaf node have been scanned, Go to next node...
		if (n->pointers[sMeta->order - 1] != NULL) {
			n = n->pointers[sMeta->order - 1];
			sMeta->keyIndex = 1;
			sMeta->totalKeys = n->num_keys;
			sMeta->node = n;
			//((NodeData *) n->pointers[0])->rid;
		} else {
			return RC_IM_NO_MORE_ENTRIES;
		}
	}
	}
	else
	{
		return RC_IM_NO_MORE_ENTRIES;
	}
	
	// Store the record/result/RID.
	*result = ((NodeData *) n->pointers[0])->rid;
	return RC_OK;
}

// closing scan mechanism and freeing up resources.
RC closeTreeScan(BT_ScanHandle *handle) {
	ScanManager * sMeta = (ScanManager *) handle->mgmtData;
	sMeta = NULL;
	if (sMeta != NULL)
	{
		RC_ERROR;
	}
	else
	{
		free(handle);
	}
	
	return RC_OK;
}

// This function prints the B+ Tree
char *printTree(BTreeHandle *tree) {
	printf("printTree()\n");
	Node* n = NULL;
	int i=0;
	int rank=0;
	int nRank=0;
	BTreeManager *tMgr = (BTreeManager *) tree->mgmtData;
	// Node * bTNode = NULL;
	tMgr->queue = NULL;
	enqueue(tMgr, tMgr->root);
	while (tMgr->queue != NULL) {
		n = dequeue(tMgr);
		Node* myParent = n->parent;
		if(myParent != NULL){
			;
		} else {
			;
		}
		if(myParent != NULL && myParent->pointers[0] == n) {
		
			nRank = path_to_root(tMgr->root, n);
		
			if(nRank != rank) {
				rank = nRank;
				printf("\n");
			}
		}
	       
		printf("(%lx)", (unsigned long) n);
		for(int j=0;j < n->num_keys; j++){
		printf("%lx ", (unsigned long) n->pointers[j]);
    		Value *key = n->keys[j];
    		switch (tMgr->keyType) {
        		case DT_STRING:
					printf("%s ", key->v.stringV);
           		 	break;
        		case DT_INT:
					printf("%ld ", key->v.intV);
					break;
				default:
					break;
	       	}
		
		}
		if (!(n->is_leaf)) {
			for(i=0; i <= n->num_keys ; i++){
				enqueue(tMgr, (Node *) n->pointers[i]);
			}
		}
		if (n->is_leaf) {
		        printf("%lx ", (unsigned long) n->pointers[tMgr->order - 1]);
		} else {
		        printf("%lx ", (unsigned long) n->pointers[n->num_keys]);
		}

		printf("| ");
	}



		/*if (dequeue(tMgr)->parent != NULL && dequeue(tMgr) == dequeue(tMgr)->parent->pointers[0]) {
			int nRank = 0;
	//printf("\npass point 4\n");
			nRank = path_to_root(tMgr->root, bTNode);
			if (nRank != 0) {
				int rank = nRank;
				printf("\n");
			}
		}*/

	printf("\n");

	return '\0';
}
