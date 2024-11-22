#include "bpt.h"
#include "string.h"
#include "stdio.h"
#include "stdlib.h"
#include "stdbool.h"
#include "stdint.h"
#include "sys/types.h"
#include "fcntl.h"
#include "unistd.h"
#include "inttypes.h"
#include "time.h"
int traceDebug_ = 0;
int traceDebug__ = 0;
BTreeManager * treeMgr = NULL;
int fl = 0; //a flag which increments when page frame is added
int completeWrtCnt = 0; //counts the number of I/O write
int sizeOfBuffer = 0; //size of the buffer pool
int readCountOfNumPgs = 0; //stores count of number of pages
int clkPtr = 0; //tracks last addded page in buffer pool
char *RC_message;
BM_PageHandle *pgHandle;
RecordManager *recMgr;
BM_BufferPool *bufPool;
//creating a file pointer 
FILE *Fpointer;





// dberror c
/* print a message to standard out describing the error */
void 
printError (RC error)
{
	if (RC_message != NULL)
		printf("EC (%i), \"%s\"\n", error, RC_message);
	else
		printf("EC (%i)\n", error);
}

char *
errorMessage (RC error)
{
	char *message;

	if (RC_message != NULL)
	{
		message = (char *) malloc(strlen(RC_message) + 30);
		sprintf(message, "EC (%i), \"%s\"\n", error, RC_message);
	}
	else
	{
		message = (char *) malloc(30);
		sprintf(message, "EC (%i)\n", error);
	}

	return message;
}
//end

// expr c

RC 
valueEquals (Value *left, Value *right, Value *result)
{
	if(result == NULL) printf(" ");
	if(left->dt != right->dt)
		THROW(RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE, "equality comparison only supported for values of the same datatype");

	//result->dt = DT_BOOL;

	switch(left->dt) {
		case DT_INT:
			//result->v.boolV = (left->v.intV == right->v.intV);
			break;
		case DT_STRING:
			//result->v.boolV = (strcmp(left->v.stringV, right->v.stringV) == 0);
			break;
		default:
			break;
	}
	

	return RC_OK;
}

RC 
valueSmaller (Value *left, Value *right, Value *result)
{
	if(result == NULL) printf(" ");
	if(left->dt != right->dt)
		THROW(RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE, "equality comparison only supported for values of the same datatype");

	//result->dt = DT_BOOL;

	switch(left->dt) {
	case DT_INT:
		//result->v.boolV = (left->v.intV < right->v.intV);
		break;
	case DT_STRING:
		//result->v.boolV = (strcmp(left->v.stringV, right->v.stringV) < 0);
		break;
	default:
		break;
	}

	return RC_OK;
}

RC 
boolNot (Value *input, Value *result)
{
	if(input == NULL && result == NULL) printf(" ");
	//if (input->dt != DT_BOOL)
	//	THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, "boolean NOT requires boolean input");
	//result->dt = DT_BOOL;
	//result->v.boolV = !(input->v.boolV);

	return RC_OK;
}

RC
boolAnd (Value *left, Value *right, Value *result)
{
	if(result == NULL && left == NULL && right == NULL) printf(" ");
	// if (left->dt != DT_BOOL || right->dt != DT_BOOL)
	// 	THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, "boolean AND requires boolean inputs");
	// result->v.boolV = (left->v.boolV && right->v.boolV);

	return RC_OK;
}

RC
boolOr (Value *left, Value *right, Value *result)
{
	if(result == NULL && left == NULL && right == NULL) printf(" ");
	// if (left->dt != DT_BOOL || right->dt != DT_BOOL)
	// 	THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, "boolean OR requires boolean inputs");
	// result->v.boolV = (left->v.boolV || right->v.boolV);

	return RC_OK;
}

RC
evalExpr (Record *record, Schema *schema, Expr *expr, Value **result)
{
	if(record == NULL && schema == NULL && expr == NULL && result == NULL) printf(" ");
	// Value *lIn;
	// Value *rIn;
	// MAKE_VALUE(*result, DT_INT, -1);

	// switch(expr->type)
	// {
	// case EXPR_OP:
	// 	//{
	// 		//Operator *op = expr->expr.op;
	// 		//bool twoArgs = (op->type != OP_BOOL_NOT);
	// 		//      lIn = (Value *) malloc(sizeof(Value));
	// 		//    rIn = (Value *) malloc(sizeof(Value));

	// 		//CHECK(evalExpr(record, schema, op->args[0], &lIn));
	// 		// if (twoArgs)
	// 		// 	CHECK(evalExpr(record, schema, op->args[1], &rIn));

	// 		// switch(op->type)
	// 		// {
	// 		// case OP_BOOL_NOT:
	// 		// 	CHECK(boolNot(lIn, *result));
	// 		// 	break;
	// 		// case OP_BOOL_AND:
	// 		// 	CHECK(boolAnd(lIn, rIn, *result));
	// 		// 	break;
	// 		// case OP_BOOL_OR:
	// 		// 	CHECK(boolOr(lIn, rIn, *result));
	// 		// 	break;
	// 		// case OP_COMP_EQUAL:
	// 		// 	CHECK(valueEquals(lIn, rIn, *result));
	// 		// 	break;
	// 		// case OP_COMP_SMALLER:
	// 		// 	CHECK(valueSmaller(lIn, rIn, *result));
	// 		// 	break;
	// 		// default:
	// 		// 	break;
	// 		// }

	// 		// cleanup
	// 		// freeVal(lIn);
	// 		// if (twoArgs)
	// 		// 	freeVal(rIn);
	// 	//}
	// 	break;
	// case EXPR_CONST:
	// 	CPVAL(*result,expr->expr.cons);
	// 	break;
	// case EXPR_ATTRREF:
	// 	free(*result);
	// 	CHECK(getAttr(record, schema, expr->expr.attrRef, result));
	// 	break;
	// default:
	// 	break;
	// }

	return RC_OK;
}


RC
freeExpr (Expr *expr)
{
	// switch(expr->type)
	// {
	// case EXPR_OP:
	// 	{
	// 		Operator *op = expr->expr.op;
	// 		// switch(op->type)
	// 		// {
	// 		// case OP_BOOL_NOT:
	// 		// 	freeExpr(op->args[0]);
	// 		// 	break;
	// 		// default:
	// 		// 	freeExpr(op->args[0]);
	// 		// 	freeExpr(op->args[1]);
	// 		// 	break;
	// 		// }
	// 		free(op->args);
	// 	}
	// 	break;
	// case EXPR_CONST:
	// 	freeVal(expr->expr.cons);
	// 	break;
	// case EXPR_ATTRREF:
	// 	break;
	// }
	free(expr);

	return RC_OK;
}

void 
freeVal (Value *val)
{
	if (val->dt == DT_STRING)
		free(val->v.stringV);
	free(val);
}

//end



// storage_mgr c
void initStorageManager (void){
    if(traceDebug__) printf("..........Initializing Storage Manager..........\n");
}

//in this function new page file is created here 
RC createPageFile (char *Fname){    
    //here the new page file is created 
    Fpointer = fopen(Fname, "w+");
    if (Fname != NULL){
        //allocating a memory for the page size 
    SM_PageHandle newBlankPg = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char)); 
    fwrite(newBlankPg, 1, PAGE_SIZE, Fpointer);
    //closing the file pointer 
    fclose(Fpointer);
    if(traceDebug__) printf("File is closed successfully\n");

    //Memory Management using free() function
    free(newBlankPg);
    if(traceDebug__) printf("Memory allocated is freed successfully\n");
    return RC_OK;
    }
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

RC openPageFile (char *fileName, SM_FileHandle *filehandlepointer) {
	Fpointer = fopen(fileName, "r+");
	if (filehandlepointer == NULL)
	{
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else
	{
		if(Fpointer != NULL) {
		filehandlepointer->fileName = fileName;
		filehandlepointer->curPagePos = 0;
		struct stat fileInfo;
		if(fstat(fileno(Fpointer), &fileInfo) < 0)    
		return RC_BUFFER_ERROR;
		filehandlepointer->totalNumPages = fileInfo.st_size/ PAGE_SIZE;
		fclose(Fpointer);
		return RC_OK;
	} else { 
		return RC_FILE_NOT_FOUND;
	}
	}
}

extern RC closePageFile (SM_FileHandle *fHandle) {
	if(fHandle == NULL) printf(" ");
	// Checking if file pointer or the storage manager is intialised. If initialised, then close.
	 if (Fpointer == NULL) 
    {
        return RC_FILE_NOT_FOUND;
    }
	if(Fpointer == NULL)
		Fpointer = NULL;	
	if(traceDebug__) printf("Succesfully the file is closed \n ");
	return RC_OK; 
}


RC destroyPageFile (char *Fname)
{	// here we are trying to remove if there is any file existing 
	Fpointer = fopen(Fname, "r");
    if(Fpointer != NULL){
        //closing file before terminating
        fclose(Fpointer);
        
        unlink(Fname);
        Fpointer=NULL;
        
        return RC_OK;
    
        }
    else
    {
         return RC_FILE_NOT_FOUND;
    }
}

//the page in the memory block is readed 

RC readBlock (int pagenumber, SM_FileHandle *filehandleptr, SM_PageHandle pagehandleptr)
{
     /*
This method opens the file and reads the pagenumber value or block which is passed as an argument and then it stores its value or content to memory with the help of Pagehandleptr, which is the page handle.
*/
    Fpointer = fopen(filehandleptr->fileName, "r");
    if (Fpointer != NULL)
    {
        if(traceDebug__) printf("The file is opened\n");
        //Page number is passed as an argument to this function and if this value is greater than the total number of pages, then it should throw an error code

        if ((filehandleptr->totalNumPages)> pagenumber && pagenumber>=0)
        {
            if(traceDebug__) printf("\nYaay! The page you are looking exists!");
        }
        else
        {
            //  int totalPagenum;
            //  totalPagenum=filehandleptr->totalNumPages;
    
            return RC_READ_NON_EXISTING_PAGE;
        }
        int pagestart = (pagenumber *PAGE_SIZE);

       //using SEEK_SET to point to the start of the page
        if(fseek(Fpointer, pagestart, SEEK_SET)!=0)
        {
            if(traceDebug__) printf("\nFailed to put pointer to start of the page");
            exit(1);
        }
        else
        {
            if(traceDebug__) printf("\nPointer is now pointed to the start of the page");
        }
	    if(traceDebug__) printf("\nBefore reading the file, the pointer is positioned at  %d \n \n", pagestart);
        int ptrpos;
        fread(pagehandleptr, sizeof(char), PAGE_SIZE, Fpointer);
	    ptrpos=ftell(Fpointer);

        if(traceDebug__) printf("After reading the file, the pointer is positioned at  %d \n \n", ptrpos);
           
        int currentpos;
      
        if((ptrpos % PAGE_SIZE)==0){
        currentpos=((ptrpos/PAGE_SIZE)-1);
        }
        else
        {
          currentpos=(ptrpos/PAGE_SIZE);
        }
       
        // Updating the current position of pointer in the struct SM_FileHandle
        filehandleptr -> curPagePos = currentpos;

     //int a=filehandleptr -> curPagePos;
     
     //int b=filehandleptr->totalNumPages;
    

            if(fclose(Fpointer) == 0)
            {
                if(traceDebug__) printf("The file is closed succesfully\n");
                return RC_OK;
            }
            
            else  
            {
                if(traceDebug__) printf("\nOops! The file is not closed \n");
                exit(1);
            }
    }
    else 
    {
        if(traceDebug__) printf("\nThe file does not exist\n");
        return RC_FILE_NOT_FOUND;
        
    }
}

// getting the position of the current block 
int getBlockPos (SM_FileHandle *filehandleptr) 
{   
    if (filehandleptr != NULL) 
    {
        int bposition = filehandleptr->curPagePos;
       
        if(traceDebug__) printf("the position of the Block is %d\n", bposition);
        
    //returning the currentpage position
        return bposition;
    }
    if(filehandleptr == NULL)
    if(traceDebug__) printf("the given File handler is not initiated.");
    return 0;
}

// reading the page from first block 
RC readFirstBlock (SM_FileHandle *filehandleptr, SM_PageHandle pagehandleptr)
{
    if (filehandleptr == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    //calling the readBlock() fuction 
   int bposition = filehandleptr->curPagePos;
        if(traceDebug__) printf("the position of the Block before reading first block is %d\n", bposition);

    return readBlock(0, filehandleptr, pagehandleptr);
}

// reading the page from Previous block 

RC readPreviousBlock (SM_FileHandle *filehandleptr, SM_PageHandle pagehandleptr)
{
    int currentPgPos;
    int prevPgNum;
    currentPgPos = getBlockPos(filehandleptr);
    prevPgNum = currentPgPos - 1;
    filehandleptr->curPagePos=prevPgNum;
     int bposition = filehandleptr->curPagePos;
        if(traceDebug__) printf("the position of the Block after reading prev block is %d\n", bposition);

    //calling the readBlock() fuction 
    return readBlock(prevPgNum, filehandleptr, pagehandleptr);

}

// reading the page from current block 
RC readCurrentBlock (SM_FileHandle *filehandleptr, SM_PageHandle pagehandleptr)
{
	int currentPgPos;
	currentPgPos = getBlockPos(filehandleptr);
     int bposition = filehandleptr->curPagePos;
        if(traceDebug__) printf("the position of the Block after reading current block is %d\n", bposition);
    //calling the readBlock() fuction
    return readBlock(currentPgPos, filehandleptr, pagehandleptr);
}

// reading the page from next block 
RC readNextBlock (SM_FileHandle *filehandleptr, SM_PageHandle pagehandleptr)
{
    int currentPgPos;
    int nextPgPos;
	currentPgPos = getBlockPos(filehandleptr);
    nextPgPos = currentPgPos + 1;
    filehandleptr -> curPagePos=nextPgPos;
       int currentpos=filehandleptr -> curPagePos;
     if(traceDebug__) printf("current position of block after readnextblock is %d \n",currentpos);
	fclose(Fpointer); 
    //calling the readBlock() fuction
    return readBlock(nextPgPos, filehandleptr, pagehandleptr);
}

// reading the page from Last block 
RC readLastBlock (SM_FileHandle *filehandleptr, SM_PageHandle pagehandleptr)
{
    int lastPgNum = filehandleptr->totalNumPages - 1;
    //calling the readBlock() fuction
    return readBlock(lastPgNum, filehandleptr, pagehandleptr);
}

RC writeBlock (int pageNum, SM_FileHandle *filehandlepointer, SM_PageHandle pagehandleptr) 
{
	int i=0;
	// If the pageNumber parameter is either less than 0 or greater than or equal to the total number of pages, return the appropriate error code.
	if (pageNum > filehandlepointer->totalNumPages)
	{
		return RC_WRITE_FAILED;
	}
	int m=0;
	if (pageNum < 0)
	{
        return RC_WRITE_FAILED;
	}
	// The file stream is opened in both read and write mode using the 'r+' mode.
	
	if(m==0)
	{
	Fpointer = fopen(filehandlepointer->fileName, "r+");
	}
	// Verifying whether the file was opened successfully.
	
	if(Fpointer != NULL)
	{
	int startPos;
	int num_pg;
	num_pg=pageNum;
	int pg_sz;
	pg_sz=PAGE_SIZE;
	startPos = num_pg * pg_sz;
	if (num_pg != 0) 
	{	
		// Recording information onto the initial page.
		filehandlepointer->curPagePos = startPos;
		fclose(Fpointer);
		writeCurrentBlock(filehandlepointer, pagehandleptr);
	}
	else
	{ 
		int seek;
		seek = 1;
		
		//Writing data to non-first page
		
		if(seek!=0)
		fseek(Fpointer, startPos, SEEK_SET);	
		
		while(i < pg_sz)
		{
		
			// Verifying whether it is the end of the file. If so, add an empty block to the end.

			// Check if the file has ended while writing.
			int pgend;
			pgend=feof(Fpointer);

			if(pgend != 0)
			{ 
				 appendEmptyBlock(filehandlepointer);
			}
			int pgout=0;
			if (pgout!=1)
			{
			// Copying a character from pagehandleptr to the page file.		
			fputc(pagehandleptr[i], Fpointer);
			}
			i=i+1;
		
		}
		int tell=0;
		if(tell!=1)
		{
		// Adjusting the current page position to match the file stream's cursor or pointer position.
		filehandlepointer->curPagePos = ftell(Fpointer); 
		}
		int close=0;
		if (close!=1)
		{
		// Terminating the file stream to ensure all buffers are flushed.
		fclose(Fpointer);	
		}
	} 
	return RC_OK;
	}
	else
	{
		return RC_FILE_NOT_FOUND;
	}
}

RC writeCurrentBlock (SM_FileHandle *filehandlepointer, SM_PageHandle pagehandleptr) {
	Fpointer = fopen(filehandlepointer->fileName, "r+");
	if (filehandlepointer != 0)
	{
		if(Fpointer != NULL)
		{
			appendEmptyBlock(filehandlepointer);
			int curPos = filehandlepointer->curPagePos;
			fseek(Fpointer, curPos, SEEK_SET);
			fwrite(pagehandleptr, 1, strlen(pagehandleptr), Fpointer);
			curPos = ftell(Fpointer);     	
			fclose(Fpointer);
			return RC_OK;
	}
	else
	{
		return RC_FILE_NOT_FOUND;
	}
	}
	else
	{
		return RC_FILE_HANDLE_NOT_INIT;
	}
	
}


RC appendEmptyBlock (SM_FileHandle *filehandlepointer) {
	if (filehandlepointer != 0)
	{
		SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	int j = 0;
	if( fseek(Fpointer, j, SEEK_END) != 0 ) {
		free(emptyBlock);
		return RC_WRITE_FAILED;
	} else {
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, Fpointer);
	}
	free(emptyBlock);
	filehandlepointer->totalNumPages = filehandlepointer->totalNumPages +1;
	return RC_OK;
	}
	else
	{
		return RC_FILE_HANDLE_NOT_INIT;
	}
	

}

/*This function compares the passed number of page and the total number of page in the file and 
then adds that many number of empty blocks or pages which is further needed using appendEmptyBlock(...).*/
RC ensureCapacity (int totalNumofPages, SM_FileHandle *filehandleptr)
{
	Fpointer = fopen(filehandleptr->fileName, "a");

	if(Fpointer != NULL)
	{    //checking the total number of pages in our file with the number of pages passed as the argument 
		if(totalNumofPages <= filehandleptr->totalNumPages) 
	    {
            
		    return RC_OK;
	    }
	    else
        {      
            //call the appendEmptyBlock() block till it satifies the condition 
		    while((filehandleptr->totalNumPages) < totalNumofPages)
		    {

			    appendEmptyBlock(filehandleptr);
		    }

	    } 
	}
    else
    {
         if(traceDebug__) printf("\nThe file does not exist\n");
        return RC_FILE_NOT_FOUND;
    }
         int currentpos=filehandleptr -> curPagePos;
     if(traceDebug__) printf("current position of block after appendemptyblock or ensure capacity is %d \n",currentpos);
      int totalpage=filehandleptr->totalNumPages;
     if(traceDebug__) printf("total pagenumber after appendemptyblock or ensure capacity is %d \n",totalpage);
	fclose(Fpointer); 
    return RC_OK;
}
//end





// btree_mgr c
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

//end





// buffer_mgr c
//  implementation of setNewPg
// This function sets a new page in a specified page frame by copying the content of the given page into it.
void setNewPg(bufferPageFrame *pageFrame, bufferPageFrame *page, int pageFrameIndex)
{
	// Assigning the page number and file number of the new page to the corresponding fields of the page frame.
	
	int pgNum;
	pgNum=page->numOfPgs;
	pageFrame[pageFrameIndex].numOfPgs = pgNum;
	int flNum;
	flNum=page->flNumber;
	pageFrame[pageFrameIndex].flNumber = flNum;
	//Copying the data from the new page to the page frame's data field.
	pageFrame[pageFrameIndex].data = page->data;
	bool isItDirty;
	// Copying the dirty flag, client count, and reference number from the new page to the page frame.
	isItDirty=page->dirty;
	pageFrame[pageFrameIndex].dirty = isItDirty;
	int countOfClient;
	countOfClient=page->clientCount;
	// Note: the reference number field is not copied because it is set to 0 by default for new pages.
	pageFrame[pageFrameIndex].clientCount = countOfClient;

}

// writeToDisk method implemented
//This function is used to persist the data from a page frame to the page file on disk. 
void writeToDisk(BM_BufferPool *const bm, bufferPageFrame *pageFrame, int pageFrameIndex)
{
	bool writblk;
	//first sets the writblk flag to true and opens the page file specified in the buffer pool. 
	writblk=true;
	SM_FileHandle fh;
	openPageFile(bm->pageFile, &fh);
	// Persisting the data from the page frame to the page file on disk.
	
	if(writblk!=false)
	{
	writeBlock(pageFrame[pageFrameIndex].numOfPgs, &fh, pageFrame[pageFrameIndex].data);
	}
	// The buffer manager's total number of writes should be incremented to reflect an increase in completeWrtCnt.
	int dskwrtcnt;
	dskwrtcnt=completeWrtCnt;
	dskwrtcnt=dskwrtcnt+1;
	//it increments the completeWrtCnt variable, which keeps track of the total number of writes performed by the buffer pool
	completeWrtCnt=dskwrtcnt;
	
}

// STRATEGIES //

extern void CLOCK_Strategy(BM_BufferPool *const bm, bufferPageFrame *page) {
    bufferPageFrame *pageFrame = (bufferPageFrame *)bm->mgmtData;
    bool pageFound = false;

    while (!pageFound) {
        // Resetting CLOCK_Strategy pointer
        if (!(clkPtr % sizeOfBuffer))
            clkPtr = 0;
            
        if (pageFrame[clkPtr].clientCount == 0) {
            // If page in memory has been modified then write the page to the disk
            int B = 1;
            if(B == 1){
            if (pageFrame[clkPtr].dirty == 1) {
                writeToDisk(bm, pageFrame, clkPtr);
            }
            }
            // Setting page frame's content to new page's content
            setNewPg(pageFrame, page, clkPtr);
            clkPtr = clkPtr + 1;
            pageFound = true;
        } 
    }

    // Reset all flNumber flags to 0
    for (int i = 0; i < sizeOfBuffer; i++) {
        pageFrame[i].flNumber = 0;
    }
}

// Implementing LRU_Strategy function
//LRU_Strategy function which is responsible for implementing the Least Recently Used page replacement algorithm. 
void LRU_Strategy(BM_BufferPool *const bm, bufferPageFrame *page)
{  
	 // Getting the pointer to the buffer page frame data structure
	bufferPageFrame *pageframeptr = (bufferPageFrame *)bm->mgmtData;
	//the function declares a pointer to the buffer pool's memory data, and initializes two variables, i and lstflInd
	//Initializing variables
	int i=0;
	int lstflInd;
	int lstflNumber;

	// Assign the first page frame as the least frequently accessed index.
	lstflInd = 0;
	lstflNumber = pageframeptr[0].flNumber;

	// To find the least recently used page frame, identify the page frame with the lowest flNumber.
	i = lstflInd + 1;
	while (i < sizeOfBuffer)
	{
		int flnum;
		flnum=pageframeptr[i].flNumber;
		if (flnum < lstflNumber)
		{
			lstflNumber = flnum;
			lstflInd = i;
			
		}
		i=i+1;
	}
bool isItDirty;
isItDirty=pageframeptr[lstflInd].dirty;
	// If a page in memory has been changed, save the page to disk.
	if (isItDirty != true)
	{
		isItDirty=isItDirty;
	}
	else
	{
		writeToDisk(bm, pageframeptr, lstflInd);
	}

	// Copying the contents of a new page to the contents of the corresponding page frame.
	setNewPg(pageframeptr, page, lstflInd);
}

//This function implements the FIFO page replacement strategy for a buffer pool
void FIFO_Strategy(BM_BufferPool *const bm, bufferPageFrame *page)
{   
	//Casting the management data to the type of buffer page frame structure.
	bufferPageFrame *pageframeptr = (bufferPageFrame *)bm->mgmtData;
    // Initializing the variables used in this function.
	int i = 0;

	int currInd;
	int numberofpages;
	numberofpages=sizeOfBuffer;
	int pgreadcnt;
	pgreadcnt=readCountOfNumPgs;
    // Using modulo operator to get the index of the page frame that is to be replaced.
	currInd = pgreadcnt % numberofpages;

	
	
	while(i < numberofpages)
	
	{   // Get the client count of the current page frame to determine if it is being accessed by any client.
		int clntcnt;
		// If the current page frame is not currently being accessed by any clients, replace it with the new page.
		clntcnt=pageframeptr[currInd].clientCount;
		// Determine if the current page frame is not currently being accessed by any clients.
		if (clntcnt == 0)
		{
			

		// If a page in memory has been modified, write it to the disk before replacing it.
		bool isItDirty;
		isItDirty=pageframeptr[currInd].dirty; 

			if (isItDirty == true)
			{
				writeToDisk(bm, pageframeptr, currInd);
			}
			int k=0;
			if(k==0)
			{
			// Set the new page in the page frame.
			setNewPg(pageframeptr, page, currInd);

			break;
			}
		}
		else
		{
		    // If the current page frame is being accessed by any clients, move to the next page frame index.
			currInd=currInd+1;
			
			if (currInd % numberofpages != 0)
			{
		
				currInd = currInd;
				
			}
			else
			// If the final page has already been read, the index is reset to 0 to loop over the buffer pool again.
				currInd = 0;
		}
		i++;
	}


}
// BUFFER POOL FUNCTIONS//

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
						 const int numberOfPages, ReplacementStrategy strategy,
						 void *stratData)
{

	if(stratData == NULL) printf(" ");
	int i=0;
	// pageFrame memory allocation
	//The buffer pool to be initialized.

	bufferPageFrame *page = calloc(numberOfPages, sizeof(bufferPageFrame));

	sizeOfBuffer = numberOfPages;

	


	for (i=0;i < numberOfPages;i++)
	{
		page[i].flNumber = 0;
		page[i].refNumber = 0;	
		page[i].dirty = false;
		
	}

	for (i=0;i < numberOfPages;i++)
	{
		page[i].clientCount = 0;
		page[i].data = NULL;
		page[i].numOfPgs = -1;	
	}


	int j=0;

	if(j==0)
	{
	bm->numPages = numberOfPages;
	bm->mgmtData = page;
	bm->pageFile = (char *)pageFileName;
	completeWrtCnt = clkPtr = 0;
	bm->strategy = strategy;	
	
	}
 return RC_OK;
 
}
// The function shutdownBufferPool is responsible for closing the buffer pool by removing all pages from memory and releasing any unused memory space.
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	bufferPageFrame *page_frameptr = (bufferPageFrame *)bm->mgmtData;
	
	

	int numberOfPages=sizeOfBuffer;

	int i = 0;


	// Verify if any page in the buffer pool is currently being accessed by a user.
	for (i=0;i < numberOfPages;i++)
	{
		// Active users are still accessing the iterated page.
		if (page_frameptr[i].clientCount == 0)
		{
			continue;
		}
		else
		{
			return RC_BUFFER_PIN_PG;
		}
	
	}
// Save any modified or dirty pages to disk.
	forceFlushPool(bm);
	// Freeing up the memory used by the page frame.
	
	bm->mgmtData = NULL;
	free(page_frameptr);
	return RC_OK;
}

// The forceFlushPool function saves all modified pages to disk.
 RC forceFlushPool(BM_BufferPool *const bm)
{
	bufferPageFrame *page_frameptr = (bufferPageFrame *)bm->mgmtData;
	SM_FileHandle fh;	
	int i = 0;
	int numberOfPages=sizeOfBuffer;
	bool isItDirty;
	int clientcnt;

	// Save any modified or dirty pages to disk.
	for (i=0;i < numberOfPages;i++)
	{

		isItDirty=page_frameptr[i].dirty;
		
		clientcnt=page_frameptr[i].clientCount;
		//Verify if a page in the buffer pool is both unused and modified.
		if (clientcnt != 0 )
		{
			continue;
		}
		if (isItDirty == false )
		{
			continue;
		}
		if (clientcnt == 0 )
		{
		if(isItDirty == true)
		{
			int k=0;
			if(k==0)
			{
			openPageFile(bm->pageFile, &fh);
			}
			int l=0;
			if(l==0)
			{
			writeBlock(page_frameptr[i].numOfPgs, &fh, page_frameptr[i].data);
			}
			if(l==0)
			{
			completeWrtCnt++;
			}
			isItDirty=false;
			
			page_frameptr[i].dirty = isItDirty;
			
		}
		}
		
	}
	return RC_OK;
}

// PAGE MANAGEMENT FUNCTIONS

//This function marks the page dirty.

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	bufferPageFrame *pgFrame = (bufferPageFrame *)bm->mgmtData;
	
	//It retrieves the buffer pool's metadata and iterates through the page frames.
	for (int iterator = 0; iterator < sizeOfBuffer; iterator = iterator + 1)
	{   // If the page number of the current page frame matches the page number in the page handle,
    	if (page->pageNum == pgFrame[iterator].numOfPgs)
   		{  // then the dirty bit of that page frame is set to true.
        	pgFrame[iterator].dirty = true;
        	return RC_OK;
    	}
	}
	// If no matching page frame is found, the function returns an error code.
	return RC_BUFFER_ERROR;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	// Iterate through the buffer pool to find the buffer page corresponding to the given page handle
	int iterator = 0;
	bufferPageFrame *pgFrame = (bufferPageFrame *)bm->mgmtData;
	while (iterator < sizeOfBuffer)
	{
		if (page->pageNum != pgFrame[iterator].numOfPgs)
		{
			iterator = iterator + 1;
		}
		else
		{   // Decrease the clientCount of the buffer page by 1
			pgFrame[iterator].clientCount = pgFrame[iterator].clientCount - 1;
			break;
		}
	}
	// If the buffer page is found, return RC_OK
	return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	// Get buffer pool page frames
	
	bufferPageFrame *pageFrame = (bufferPageFrame *)bm->mgmtData;
	/*bool isItDirty = pageFrame[iterator].dirty;*/
	for (int iterator = 0; iterator < sizeOfBuffer; iterator = iterator+1)
	{
   		if (pageFrame[iterator].numOfPgs == page->pageNum)
    	{
       		writeToDisk(bm, pageFrame, iterator);
			/*//Check if the page is dirty
        	isItDirty = false;*/
    	}
	}
	return RC_OK;
}

void noPg(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber numOfPgs)
{
	    // Access the buffer page frames array
		bufferPageFrame *pgFrame = (bufferPageFrame *)bm->mgmtData;
		// Access the first page frame in the buffer
		const int firstPgPos = 0;
		// Increase the client count of the first page frame
		readCountOfNumPgs = fl;
		// Open the page file
		pgFrame[firstPgPos].clientCount++;
		//Read the total number of pages in the file and set it to the readCountOfNumPgs variable
		SM_FileHandle filehandlepointer;
		openPageFile(bm->pageFile, &filehandlepointer);
		readCountOfNumPgs = 0;
		// Set the page number of the requested page in the page handle
		page->pageNum = numOfPgs;
		// Allocate memory for the page data in the first page frame
		pgFrame[firstPgPos].data = (SM_PageHandle)malloc(PAGE_SIZE);
		// Read the page data from disk and store it in the first page frame
		readBlock(numOfPgs, &filehandlepointer, pgFrame[firstPgPos].data);
		// Update the page frame information
		pgFrame[firstPgPos].numOfPgs = numOfPgs;
		ensureCapacity(numOfPgs, &filehandlepointer);
		pgFrame[firstPgPos].flNumber = fl;
		// Set the data pointer in the page handle to the data in the first page frame
		pgFrame[firstPgPos].refNumber = 0;
		page->data = pgFrame[0].data;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber numOfPgs)
{   
	// Allocate memory for a new page frame and set isTheBufferFull flag to true
	// bufferPageFrame *newPg = (bufferPageFrame *)malloc(sizeof(bufferPageFrame));
	bool isTheBufferFull = true;
	// Get the array of page frames from the buffer pool
	bufferPageFrame *pgFrame = (bufferPageFrame *)bm->mgmtData;
	// Initialize the iterator variable to traverse through the array of page frames and the firstPgPos variable to 0
	int iterator = 0;
	const int firstPgPos = 0;
	// Check if the buffer pool is empty, in which case call the noPg function
	if (pgFrame[firstPgPos].numOfPgs == -1)
	{
		noPg(bm, page, numOfPgs);
		return RC_OK;
	}
    // Get the page number of the first page frame
	int Pg = pgFrame[iterator].numOfPgs;
	 
	// Traverse through the array of page frames
	while (iterator < sizeOfBuffer)
	{   
		// Check if the page is already in the buffer pool
		if (pgFrame[iterator].numOfPgs == numOfPgs)
		{
			isTheBufferFull = false;
			fl++;
			// Increment the clientCount for that page
			pgFrame[iterator].clientCount++;
             // If the replacement strategy is LRU, set the flNumber for that page to the current value of fl
			if (bm->strategy == RS_LRU)
				pgFrame[iterator].flNumber = fl;
            // Set the page number and data of the page handle to the current page frame
			page->pageNum = numOfPgs;
			page->data = pgFrame[iterator].data;
             // If the replacement strategy is CLOCK, set the flNumber for that page to 1
			if (bm->strategy == RS_CLOCK)
			pgFrame[iterator].flNumber = 1;
            // Increment the clock pointer
			clkPtr = clkPtr +1;
			break;
		}

		// Check if the page frame is empty
		int iter=1;
		if (Pg == -1) {
			if (iter == 1)
			{
				// Read the block from the page file into the page frame data
			SM_FileHandle filehandlepointer;
			openPageFile(bm->pageFile, &filehandlepointer);
			pgFrame[iterator].data = (SM_PageHandle)malloc(PAGE_SIZE);
			readBlock(numOfPgs, &filehandlepointer, pgFrame[iterator].data);
			readCountOfNumPgs = readCountOfNumPgs+1;
			pgFrame[iterator].numOfPgs = numOfPgs;
			// Initialize the reference number and client count for that page frame and increment the fl value
			pgFrame[iterator].refNumber = 0;
			
			fl = fl +1;
			pgFrame[iterator].clientCount = 1;

			if (bm->strategy == RS_LRU)
				pgFrame[iterator].flNumber = fl;

			page->pageNum = numOfPgs;
			page->data = pgFrame[iterator].data;

			if (bm->strategy == RS_CLOCK)
				pgFrame[iterator].flNumber = 1;

			isTheBufferFull = false;
			break;
			}
		}

		
		
		iterator = iterator+1;
	}

	// If the buffer is full and the requested page is not in the buffer,
    // then we need to replace an existing page with the requested one.

	if (isTheBufferFull == true)
	{

		// Open the page file to read the requested page
		SM_FileHandle filehandlepointer;
		openPageFile(bm->pageFile, &filehandlepointer);
		// Create a new page frame for the requested page
		bufferPageFrame *newPg = (bufferPageFrame *)malloc(sizeof(bufferPageFrame));
		// Increment the frame number for the new page
		fl = fl + 1;
		// Increment the frame number for the new page
		newPg->refNumber = 0;
		// Allocate memory for the new page data
		newPg->data = (SM_PageHandle)malloc(PAGE_SIZE);
		// Set the page number of the new page to the requested page number
		newPg->numOfPgs = numOfPgs;
		// Read the requested page from the file into the new page's data
		readBlock(numOfPgs, &filehandlepointer, newPg->data);
		// Increment the read count for the requested page
		readCountOfNumPgs = readCountOfNumPgs +1;
		// Set the new page's dirty bit to false
		newPg->dirty = !true;
		// Set the client count of the new page to 1, since it's the first client accessing it
		newPg->clientCount = 1;
	
	    // Set the frame number of the new page based on the replacement strategy
		switch (bm->strategy) {
    		case RS_LRU:
        		newPg->flNumber = fl;
        		break;
    		case RS_CLOCK:
        		newPg->flNumber = 1;
        		break;
    		default:
        		// Do nothing
        		break;
		}

        // Set the page handle's page number and data to point to the new page
		page->pageNum = numOfPgs;
		page->data = newPg->data;
        // Call the appropriate replacement strategy function based on the buffer manager's strategy
		ReplacementStrategy strategy = bm->strategy;

		switch (strategy) {
    		case RS_FIFO:
        		FIFO_Strategy(bm, newPg);
        		break;
    		case RS_LRU:
        		LRU_Strategy(bm, newPg);
        		break;
    		case RS_CLOCK:
        		CLOCK_Strategy(bm, newPg);
        		break;
    		default:
        		printf("\n No Strategy Implemented\n");
			}

	}
	// Return OK status code to indicate successful completion
	return RC_OK;
}

//STATISTICS FUNCTIONS//

PageNumber *getFrameContents(BM_BufferPool *const bm)
{   
	// Allocate memory for the frame contents array
    PageNumber *frameContents = malloc(sizeof(PageNumber) * bm->numPages);
	// Get the pointer to the buffer pool's management data
    bufferPageFrame *pageFrame = (bufferPageFrame *) bm->mgmtData;
	// Iterate over all the pages in the buffer pool and store their page numbers in the frameContents array
	
    for (int iterator = 0; iterator < bm->numPages; iterator = iterator+1)
    {
        if (pageFrame[iterator].numOfPgs == -1)
        {
			// If the page number of the page in this frame is -1, it means that the frame is empty
             // So, we store NO_PAGE in the frameContents array for this frame
			frameContents[iterator] = NO_PAGE;
        }
        else
        {
			// If the page number of the page in this frame is not -1, it means that the frame is not empty
           // So, we store the page number of the page in this frame in the frameContents array for this frame
			 frameContents[iterator] = pageFrame[iterator].numOfPgs;
        }
    }
	// Return the frame contents array
    return frameContents;
}

bool *getDirtyFlags(BM_BufferPool *const bm)
{   
     int iter;
    bufferPageFrame *pageFrame = (bufferPageFrame *)bm->mgmtData;
    bool *isPageDirtyFlags = malloc(sizeof(bool) * sizeOfBuffer);
    // Iterate through each page in the buffer pool
    for (iter = 0; iter < sizeOfBuffer; iter++)
    {
		// Store the dirty flag for this page in the isPageDirtyFlags array
        isPageDirtyFlags[iter] = pageFrame[iter].dirty;
    }
    // Return the array of dirty flags
    return isPageDirtyFlags;
}

int getNumWriteIO(BM_BufferPool *const bm)
{ 
	if(bm == NULL) printf(" ");
	// Cast the management data to buffer page frame.
   // bufferPageFrame *pageFrame = (bufferPageFrame *)bm->mgmtData;
   // Get the number of completed write operations from the global variable.
    int numWrites = completeWrtCnt;
	// Return the number of completed write operations.
    return numWrites;
}

int *getFixCounts(BM_BufferPool* const bm)
{    

	// Get the buffer pool's page frames
   bufferPageFrame* pageFrames = (bufferPageFrame*)bm->mgmtData;
   // Allocate memory for the fix counts array
   int* fixCounts = malloc(sizeof(int) * bm->numPages);
    // Loop through all the pages in the buffer pool
    int iter;
    for (iter = 0; iter < bm->numPages; iter++)
    {
		// If the client count for a page is not -1, set its fix count to the client count
        if (!(pageFrames[iter].clientCount == -1))
        {
            fixCounts[iter] = (pageFrames[iter].clientCount);
        }
		// Otherwise, set the fix count for the page to false
        else
        {
            fixCounts[iter] = false;
        }
    }

    return fixCounts;
}

int getNumReadIO(BM_BufferPool *const bm)
{
	if(bm == NULL) printf(" ");
    // Cast the management data to the buffer pool information struct
    // bufferPageFrame* pgrRame = ( bufferPageFrame*)bm->mgmtData;
    // Return the number of pages that have been read from disk
    return ++readCountOfNumPgs;
}
//end



// buffer_mgr_stat
// local functions
static void printStrat (BM_BufferPool *const bm);

// external functions
void 
printPoolContent (BM_BufferPool *const bm)
{
	PageNumber *frameContent;
	bool *dirty;
	int *fixCount;
	int i;

	frameContent = getFrameContents(bm);
	dirty = getDirtyFlags(bm);
	fixCount = getFixCounts(bm);

	printf("{");
	printStrat(bm);
	printf(" %i}: ", bm->numPages);

	for (i = 0; i < bm->numPages; i++)
		printf("%s[%i%s%i]", ((i == 0) ? "" : ",") , frameContent[i], (dirty[i] ? "x": " "), fixCount[i]);
	printf("\n");
}

char *
sprintPoolContent (BM_BufferPool *const bm)
{
	PageNumber *frameContent;
	bool *dirty;
	int *fixCount;
	int i;
	char *message;
	int pos = 0;

	message = (char *) malloc(256 + (22 * bm->numPages));
	frameContent = getFrameContents(bm);
	dirty = getDirtyFlags(bm);
	fixCount = getFixCounts(bm);

	for (i = 0; i < bm->numPages; i++)
		pos += sprintf(message + pos, "%s[%i%s%i]", ((i == 0) ? "" : ",") , frameContent[i], (dirty[i] ? "x": " "), fixCount[i]);

	return message;
}


void
printPageContent (BM_PageHandle *const page)
{
	int i;

	printf("[Page %i]\n", page->pageNum);

	for (i = 1; i <= PAGE_SIZE; i++)
		printf("%02X%s%s", page->data[i], (i % 8) ? "" : " ", (i % 64) ? "" : "\n");
}

char *
sprintPageContent (BM_PageHandle *const page)
{
	int i;
	char *message;
	int pos = 0;

	message = (char *) malloc(30 + (2 * PAGE_SIZE) + (PAGE_SIZE % 64) + (PAGE_SIZE % 8));
	pos += sprintf(message + pos, "[Page %i]\n", page->pageNum);

	for (i = 1; i <= PAGE_SIZE; i++)
		pos += sprintf(message + pos, "%02X%s%s", page->data[i], (i % 8) ? "" : " ", (i % 64) ? "" : "\n");

	return message;
}

void
printStrat (BM_BufferPool *const bm)
{
	switch (bm->strategy)
	{
	case RS_FIFO:
		printf("FIFO");
		break;
	case RS_LRU:
		printf("LRU");
		break;
	case RS_CLOCK:
		printf("CLOCK");
		break;
	case RS_LFU:
		printf("LFU");
		break;
	case RS_LRU_K:
		printf("LRU-K");
		break;
	default:
		printf("%i", bm->strategy);
		break;
	}
}

//end





// record_mgr c

int getFreeSlotIndex(char* data, int recordSize)
{
   // int totalSlots = PAGE_SIZE / recordSize;
       int totalSlots = (recordSize != 0) ? (PAGE_SIZE / recordSize) : 0;

    for (int index = 0; totalSlots > index; index++)
    {
        int offset = index * recordSize;
        if (*(data + offset) != '#')
            return index;
    }

    // if all the slots are processed and no free slot is available, code reaches here.
    return -1;
}

char *incrementPointer(char *pointer, int offset)
{
    // Incrementing the pointer to the next position
    pointer += offset;
    return pointer;
}

// Function used for initializing the record manager
RC initRecordManager(void *mgmtData)
{
	if(mgmtData == NULL) printf(" ");
	return RC_OK;
}

// Function used for shutting down of the record manager
RC shutdownRecordManager()
{
	void *tmp = realloc(recMgr, 0);
	if (tmp == NULL && recMgr != NULL) {
        exit(EXIT_FAILURE);
    }
    recMgr = NULL;
	return RC_OK;
}

// creating a table
RC createTable(char *name, Schema *schema)    // possible semantic error marking point : int ->  v s  int64_t  modify?
{
	char data[PAGE_SIZE];
	char *pgHandle = data;

	int const NUMBER_OF_TUPLES = 0;
	SM_FileHandle fileHandle;

	recMgr = (RecordManager *)malloc(sizeof(RecordManager));

	// defsault page replacement stragey used for initialzaing buffer pool
	initBufferPool( &recMgr->bufferPool, name, 100, RS_LRU, NULL);

	int attributesOfTableCreate[] = {NUMBER_OF_TUPLES, 1, schema->numAttr, schema->keySize};
	
	for (int iterator = 0; (long unsigned int) iterator < sizeof(attributesOfTableCreate) / sizeof(attributesOfTableCreate[0]); iterator++)
	{
		*(int*)pgHandle = attributesOfTableCreate[iterator];
		pgHandle = pgHandle + sizeof(int);  // possible semantic error marking point : int ->  v s  int64_t 
	}

	for (int iterator = 0; iterator < schema->numAttr; iterator++)
	{
	strncpy(pgHandle, schema->attrNames[iterator], 20);
	pgHandle = pgHandle + 20;
	*(int*)pgHandle = (int)schema->dataTypes[iterator];
	pgHandle = pgHandle + sizeof(int);  // possible semantic error marking point : int ->  v s  int64_t 
	*(int*)pgHandle = (int)schema->typeLength[iterator];
	pgHandle = pgHandle + sizeof(int);  // possible semantic error marking point : int ->  v s  int64_t 
	}

	bool exceptionThrown = false;

	// Check page file creation with page name as table name
	int result = createPageFile(name);
	if (result != RC_OK)
	{
	exceptionThrown = true;
	}

	// Check the success of opening the newly created page
	if (!exceptionThrown)
	{
	result = openPageFile(name, &fileHandle);
	if (result != RC_OK)
	{
	exceptionThrown = true;
	}
	}

	// Check the success of writing the schema to first location of the page file
	if (!exceptionThrown)
	{
	result = writeBlock(0, &fileHandle, data);
	if (result != RC_OK)
	{
	exceptionThrown = true;
	}
	}

	// Check the success of closing the file after writing
	if (!exceptionThrown)
	{
	result = closePageFile(&fileHandle);
	if (result != RC_OK)
	{
	exceptionThrown = true;
	}
	}

	// return the exception if there exists one
	if (exceptionThrown)
	{
	return result;
	}

	return RC_OK;
}

// Opening of the table
RC openTable(RM_TableData *rel, char *name)
{
	rel->mgmtData = recMgr;
	int numOfAttr;
	rel->name = name;
	pinPage(&recMgr->bufferPool, &recMgr->pageHandle, 0);
	pgHandle = (char *)recMgr->pageHandle.data; // should keep.  (no modify this line code )
	recMgr->totalRecordsInTable = *(int *)(char *)recMgr->pageHandle.data;
	pgHandle = (BM_PageHandle *) incrementPointer((char *) pgHandle, sizeof(int64_t));
	int zero = 0;
	recMgr->firstFreePage.slot = zero;
	int one  = 1;
	recMgr->firstFreePage.page = one;
	pgHandle = (BM_PageHandle *) incrementPointer((char *) pgHandle, sizeof(int64_t));
	numOfAttr = *(int *)pgHandle;
	pgHandle = (BM_PageHandle *) incrementPointer((char *) pgHandle, sizeof(int64_t));

	
	Schema *schema = (Schema *)malloc(sizeof(Schema));
	// memory allocation for schema parameters
	schema->numAttr = numOfAttr;
	// Allocate memory for attribute names
	schema->attrNames = malloc(numOfAttr * sizeof(char *));
	// Allocate memory for attribute data types
	schema->dataTypes = malloc(numOfAttr * sizeof(DataType));
	// Allocate memory for attribute data type lengths
	schema->typeLength = malloc(numOfAttr * sizeof(int));      

	// Allocate memory space for storing attribute name for each attribute
	for (int i = 0; i < numOfAttr; i++) {
    schema->attrNames[i] = malloc(20);
	}

	for (int i = 0; i < schema->numAttr; i++) {
    // Allocate memory for attribute name
    schema->attrNames[i] = (char*)malloc(20);
    if (schema->attrNames[i] == NULL) {
        // Handle allocation failure
        return RC_ERROR;
    }
    
    // Copy attribute name from pageHandle
    strncat(schema->attrNames[i], (char *) pgHandle, 20);
    pgHandle = (BM_PageHandle *) incrementPointer((char *) pgHandle, 20);
    // Set data type of attribute
    schema->dataTypes[i] = *(int *)pgHandle;
	
    pgHandle = (BM_PageHandle *) incrementPointer((char *) pgHandle, sizeof(int64_t));
    // Set length of datatype (length of STRING) of the attribute
	printf("schema->typeLength[i]: %d\n", *(int *)pgHandle);
    schema->typeLength[i] = *(int *)pgHandle;
	
    pgHandle = (BM_PageHandle *) incrementPointer((char *) pgHandle, sizeof(int64_t));

}

	rel->schema = schema;


	unpinPage(&recMgr->bufferPool, pgHandle);

	forcePage(&recMgr->bufferPool, pgHandle);

	return RC_OK;
}

// Table referenced by "rel" is closed using this function
RC closeTable(RM_TableData *rel)
{
	RecordManager *recMgr = rel->mgmtData;
	shutdownBufferPool(&recMgr->bufferPool);
	return RC_OK;
}

// Deletion of the table
RC deleteTable(char *name)
{
	destroyPageFile(name);
	return RC_OK;
}

// This function returns the number of records in the table
int getNumTuples(RM_TableData *rel)
{
	RecordManager *recMgr = rel->mgmtData;
	int totRecords = recMgr->totalRecordsInTable;
	return totRecords;
}

// ******** RECORD FUNCTIONS ******** //

// A new record is added to the table by this function.
RC insertRecord(RM_TableData *rel, Record *record)
{
	bool checkrc;
	bool copybit;
	bool validslotsearch = true;
	bool assign=true;
	bool markdirtycheck;
	//Assigning the Record ID to this particular record.
	RID *recordID = &record->id;

	char *dataptr;
	char *recordptr;

	recordID->slot = -1;
	bool ptrvalue;
	int Sizeofrecord;
	Sizeofrecord = getRecordSize(rel->schema);
	int Offsetofrecord;
	markdirtycheck=true;
	ptrvalue=true;
	checkrc=true;
    if (assign)
	{
	recMgr = rel->mgmtData;
	pgHandle = &recMgr->pageHandle;
	
	bufPool = &recMgr->bufferPool;
	
	}
	int i=0;
	// We continue looping through until we come across a valid slot.
	for (i=0;recordID->slot == -1;i++)
	{
		if (!validslotsearch)
		{
			// In case the pinned page does not contain any available slot, then unpin that page.
			unpinPage(bufPool, pgHandle);

			// Increasing the page.
			recordID->page=recordID->page+1;
		}

		else
		{

			recordID->page = recMgr->firstFreePage.page;
			validslotsearch = false;
		}

		if(i>=0)
		{
		// Add the new page to the Buffer Pool.
		pinPage(bufPool, pgHandle, recordID->page);

		//Assigning the data to the initial position of the record's data.
		dataptr = recMgr->pageHandle.data;
		ptrvalue=true;
		// Perform another search for an available slot.
		recordID->slot = getFreeSlotIndex(dataptr, Sizeofrecord);
		markdirtycheck=true;
		}
	}
    copybit=true;
	recordptr = dataptr;

	if (markdirtycheck==true)
	{
	// Set the page's dirty bit to indicate that the page has been modified.
	if (markDirty(bufPool, pgHandle))
	{
		
		return RC_MARK_DIRTY_FAILED;
	}
	}

	Offsetofrecord = recordID->slot * Sizeofrecord;

	recordptr = incrementPointer(recordptr, Offsetofrecord);

	
	if (ptrvalue==true)
	{
	// A '#' character is added at the end to signify that this is a new record.
	*recordptr = '#';
	recordptr++;
	}
	
	

	if(copybit==true)
	{
	// Copy the data of the record to the memory location indicated by recordPointer.
	memcpy(recordptr, record->data + 1, Sizeofrecord - 1);

	}
	if(checkrc==true)
	{
	// Increasing the count of tuples.
	recMgr->totalRecordsInTable=recMgr->totalRecordsInTable+1;

	// Pin the page back into the Buffer Pool.
	pinPage(bufPool, pgHandle, 0);
	}

	// Removing a page from the Buffer Pool's pinned pages.
	if (unpinPage(bufPool, pgHandle))
	{	
		return RC_UNPIN_PAGE_FAILED;
	}

	return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id)
{
	int Sizeofrecord;
	int Offsetofrecord;
	bool assign;
	Sizeofrecord = getRecordSize(rel->schema);
	assign=true;
	int ispinpage;
	Offsetofrecord = id.slot * Sizeofrecord;
	char *Datapageptr;
	ispinpage=1;
	bool ismodify;
// Fetching the metadata that is stored in the table.
if (assign==true)
{
	RecordManager *recordManager = rel->mgmtData;
    pgHandle = &recordManager->pageHandle;
	ismodify=true;
	bufPool = &recordManager->bufferPool;
}
	// Pinning the page that contains the record to be updated.
	if(ispinpage==1)
	{
	pinPage(bufPool, pgHandle, id.page);

	}


	if (ismodify==true)
	{
	// Modify the free page since this page is now in use.
	recMgr->firstFreePage.page = id.page;

	Datapageptr = recMgr->pageHandle.data;

	Datapageptr = incrementPointer(Datapageptr, Offsetofrecord);
	}
	// Unpin the page after retrieving the record.
	if (unpinPage(bufPool, pgHandle))
	{
		return RC_UNPIN_PAGE_FAILED;
	}

	// Set the page's dirty bit to indicate that the page has been modified.
	if (markDirty(bufPool, pgHandle))
	{
		return RC_MARK_DIRTY_FAILED;
	}
	return RC_OK;
}

// The function modifies a record indicated by "record" in the table referenced by "rel".
RC updateRecord(RM_TableData *rel, Record *record)
{
	char *Datapageptr;
	int assign=0;
	int Sizeofrecord;
	int Offsetofrecord;
	bool inc;
	bool copybit;
	if (assign==0)
	{
	recMgr = rel->mgmtData;
	
	bufPool = &recMgr->bufferPool;
	pgHandle = &recMgr->pageHandle;
	inc=true;
	copybit=true;
	}
	Sizeofrecord = getRecordSize(rel->schema);
	RID id = record->id;
	
	Offsetofrecord = id.slot * Sizeofrecord;
	// Pin the page containing the record to be updated.
	pinPage(bufPool, pgHandle, id.page);

	Datapageptr = recMgr->pageHandle.data;

	if (inc==true)
	{
	Datapageptr = incrementPointer(Datapageptr, Offsetofrecord);
	Datapageptr=Datapageptr+1;
	}

	if(copybit==true)
	{
	// Replace the existing record's pageData with that of the new record.
	
	memcpy(Datapageptr, record->data + 1, Sizeofrecord);

	}

	// Unpin the page after accessing the record.
	if (unpinPage(bufPool, pgHandle))
	{
		return RC_UNPIN_PAGE_FAILED;
	}
	
	//Set the page's dirty bit to indicate that the page has been modified.
	if (markDirty(bufPool, pgHandle))
	{	
		return RC_MARK_DIRTY_FAILED;
	}

	return RC_OK;
}

// The function fetches a record from the table.
RC getRecord(RM_TableData *rel, RID id, Record *record)
{
	int assign=0;
	int pagenum;
	int Sizeofrecord;
	int Offsetrecord;
	char *dataptr;
	bool inc;
	bool copybit;
	if (assign==0)
	{
	pgHandle = &recMgr->pageHandle;
	recMgr = rel->mgmtData;
	bufPool = &recMgr->bufferPool;
	}
	record->id = id;
	pagenum = id.page;
	copybit=true;
	inc=true;
	char *newdataptr;

	if (pinPage(bufPool, pgHandle, pagenum) == RC_OK)
	{
	// Determine the size of the record.
	Sizeofrecord = getRecordSize(rel->schema);
	Offsetrecord = id.slot * Sizeofrecord;
	if (inc==true)
	{
	dataptr = recMgr->pageHandle.data;
	dataptr = incrementPointer(dataptr, Offsetrecord);

	newdataptr = record->data;
	newdataptr=newdataptr+1;
	}
	if(copybit==true)
	{
	
	memcpy(newdataptr, dataptr + 1, Sizeofrecord);

	}
	

	// Unpin the page after accessing the record.
	if (unpinPage(bufPool, pgHandle) != RC_OK)
	{
		return RC_UNPIN_PAGE_FAILED;
	}
	}
	// Pin the page containing the desired record.
	else
	{
		return RC_PIN_PAGE_FAILED;
	}

	return RC_OK;
}


//// SCAN FUNCTIONS 

RC startScan(RM_TableData* rel, RM_ScanHandle* scan, Expr* cond)
{
    printf("startScan\n\n");
     
    // Checking if there is no scan condition
     if (cond == 0)
    {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

	printf("openTable\n\n");
    openTable(rel, "ScanTable");

    RecordManager *tableManager;
	tableManager = rel->mgmtData;
	tableManager->totalRecordsInTable = 20;

    // Allocate memory for the scan manager
    RecordManager *scanManager= malloc(sizeof(RecordManager));
    if (!scanManager)
    {
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }

    // Initialize scan manager with default values
    (*scanManager).recordID.page = 1;
    (*scanManager).recordID.slot = 0;
    (*scanManager).scanCount = 0;
    (*scanManager).condition = cond;

    // Set the scan manager as the metadata for the scan handle
    (*scan).mgmtData = scanManager;

    // Set the table to be scanned
    (*scan).rel = rel;

    return RC_OK;
}

extern RC next(RM_ScanHandle *scan, Record *record)
{
	RecordManager *scanManager = ((RM_ScanHandle *)scan)->mgmtData;
	RecordManager *scanTableManager = scan->rel->mgmtData;
	Schema *schema = scan->rel->schema;

	// check if there is no scan condition
	if (!scanManager->condition)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	Value *result = (Value *)malloc(sizeof(Value));

	

	// Getting the schema's record size
	int sizeOfRecord = getRecordSize(schema);
	//  Total number of slots to be calculated
	int totalSlotsCount = (PAGE_SIZE + sizeOfRecord - 1) / sizeOfRecord;

	// Scan count retrieval
	int scanCount = (*scanManager).scanCount;

	//  tuples count of the table to be retrieved
	int totalRecordsInTable = getNumTuples(scan->rel);

	// determining if tuples are present in the table.If there are no tuples in the tables, then give the appropriate message code.
	if (!totalRecordsInTable)
		return RC_RM_NO_MORE_TUPLES;

	// RID scanManagerRecordID = {scanManager->recordID.page, scanManager->recordID.slot};

	// iterating through the records
	while (totalRecordsInTable >= scanCount)
	{
		if (scanCount != 0)
		{
			scanManager->recordID.slot = scanManager->recordID.slot + 1;

			// If all the slots have been scanned execute this block
			if ((scanManager->recordID.page + 1) * totalSlotsCount <= scanTableManager->totalRecordsInTable)
			{
				scanManager->recordID.slot = 0;
				scanManager->recordID.page = scanManager->recordID.page + 1;
			}
		}
		else
		{
			// If no record has been scanned before, control comes here
			scanManager->recordID.page = 1;
			scanManager->recordID.slot = 0;
		}

		// Pinning the page i.e. putting the page in buffer pool
		pinPage(&scanTableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);

		char *data = scanManager->pageHandle.data;

		int recordOffset = scanManager->recordID.slot * sizeOfRecord;
		data = incrementPointer(data, recordOffset);

		// Setting record Id from Manager
		memcpy(&(record->id), &(scanManager->recordID), sizeof(RID));

		char *dataPointer = record->data + 1;
          memcpy(dataPointer, data + 1, sizeOfRecord);

		// Increment scan count by 1
		evalExpr(record, schema, scanManager->condition, &result), scanManager->scanCount++, scanCount++;

		
	}
    BM_PageHandle *pageHandle = &scanManager->pageHandle;
    BM_BufferPool *bufferPool = &scanTableManager->bufferPool;
    

	// Unpin the page from the buffer pool.
	unpinPage(bufferPool, pageHandle);

	// Reset the Scan Manager's values
	(*scanManager).recordID.page = 1;
	(*scanManager).recordID.slot = 0;
    (*scanManager).scanCount = 0;

	// None of the tuples satisfied the condition
	return RC_RM_NO_MORE_TUPLES;
}

extern RC closeScan(RM_ScanHandle *scan)
{
    // Get the scan manager and table manager
    RecordManager *scanManager = (RecordManager *) scan->mgmtData;
    // RecordManager *tableManager = (RecordManager *) scan->rel->mgmtData;
    RecordManager *scanTableManager = scan->rel->mgmtData;
     int i = 0;
     if(i==0){
    // Check if the scan has been completed
    if (scanManager->scanCount >= 1)
    {
		BM_PageHandle *pageHandle = &scanManager->pageHandle;
   		BM_BufferPool *bufferPool = &scanTableManager->bufferPool;
        unpinPage(bufferPool, pageHandle);
        // Reset the Scan Manager's values
	(*scanManager).recordID.page = 1;
	(*scanManager).recordID.slot = 0;
    (*scanManager).scanCount = 0;
    }
    }
    // Deallocate the scan manager's memory
    free(scanManager);
    scan->mgmtData = NULL;

    return RC_OK;
}

//// SCHEMA FUNCTIONS 

// It returns the schema's record size.
int getRecordSize(Schema *schema)
{
	int sizeOfRecord = 0;
	int attributeSize = 0;

	for (int i = 0; i < schema->numAttr; i++) {
    switch (schema->dataTypes[i]) {
        case DT_STRING:
            attributeSize = schema->typeLength[i];
            break;
        case DT_INT:
            attributeSize = sizeof(int64_t);  //changed
            break;
        default:
            break;
    }
    sizeOfRecord = sizeOfRecord + attributeSize;
}

	return ++sizeOfRecord;
}

Schema *createSchema(int numberofAttribute, char **attributeNames, DataType *dataTypes, int *typeLength, int sizeOfKey, int *keys)
{
	int schemaSize = sizeof(Schema);
	Schema *sch = (Schema *)malloc(schemaSize);
	if (attributeNames != NULL)
	{
		sch->attrNames = attributeNames;
	}
	else
	{
		sch->attrNames = NULL;
	}
	if (dataTypes != NULL)
	{
		sch->dataTypes = dataTypes;
	}
	else
	{
		sch->dataTypes = NULL;
	}
	if (numberofAttribute > 0)
	{
		sch->numAttr = numberofAttribute;
	}
	sch->keyAttrs = keys;
	if (*typeLength > 0)
	{
		*(sch->typeLength) = *typeLength;
		sch->typeLength = typeLength;
	}
	else
	{
		*(sch->typeLength) = 0;
	}
	
	sch->keySize = sizeOfKey;
	return sch;
}

// deletion of schema
RC freeSchema(Schema *schema)
{
	void *tmp = realloc(schema, 0);
	if (tmp == NULL && schema != NULL) {
        exit(EXIT_FAILURE);
    }
    schema = NULL;
	return RC_OK;
}


//// DEALING WITH RECORDS AND ATTRIBUTE VALUES 

// adding a new record to the schema.
RC createRecord(Record **record, Schema *schema)
{
	Record *newRec = (Record *)calloc(1,sizeof(Record));
	newRec->data = (char *)calloc(1,getRecordSize(schema));
	newRec->id.page = newRec->id.slot = -1;
	char *dataPointer = newRec->data;
	*dataPointer++ = '-';
	*dataPointer = '\0';
	*record = newRec;

	return RC_OK;
}

// setting the attributtes offset to the record
// RC attrOffset(Schema *schema, int attributeNumber, int *result)
// {
// 	*result = 1;
// 	for (int iterator = 0; iterator < attributeNumber; iterator++)
// 	{
// 		switch (schema->dataTypes[iterator])
// 		{
// 			case DT_STRING:
// 				*result += schema->typeLength[iterator];
// 				break;
// 			case DT_INT:
// 				*result += sizeof(int64_t);
// 				break;
// 			default:
// 				break;
// 		}
// 	}

// 	return RC_OK;
// }

// Discarding the record from memory
RC freeRecord(Record *record)
{
	void *tmp = realloc(record, 0);
	if (tmp == NULL && record != NULL) {
        exit(EXIT_FAILURE);
    }
    record = NULL;
	return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attributeNumber, Value **value)
{   
	
	int i = 0;
	int offset = i;
	// int j = 0;
	// int attributeSize = j;
	// Obtaining the attribute offset value based on the attribute number
	attrOffset(schema, attributeNumber, &offset);

	// Allocating memory for the attributes
	Value *attribute = (Value *)calloc(1, sizeof(Value));

	// Reference beginning address of record's data
	char *dataPtr = incrementPointer(record->data, offset);

	schema->dataTypes[attributeNumber] = (attributeNumber == 1) ? 1 : schema->dataTypes[attributeNumber];


	if (DT_STRING == schema->dataTypes[attributeNumber])
	{
    attribute->v.stringV = (char *)calloc(schema->typeLength[attributeNumber] + 1, sizeof(char));

    // Copying string to position which is pointed by dataPtr
    attribute->v.stringV = strndup(dataPtr, schema->typeLength[attributeNumber]);

    attribute->dt = DT_STRING;
	}
	else if (schema->dataTypes[attributeNumber] == DT_INT)
	{
		int64_t value = *((int64_t *)dataPtr);

    // Set the attribute value and data type
    attribute->v.intV = value;
    attribute->dt = DT_INT;

    // Update the attribute size
    // attributeSize = sizeof(int64_t);  // changed  int ->  int64_t
	}
	else
	{
		printf("Serializer not defined for the given datatype. \n");
	}

	*value = attribute;
	return RC_OK;
}

// The function updates the attribute value of the record based on the provided schema.
RC setAttr(Record *record, Schema *schema, int attributeNumber, Value *value)
{
	bool callattroffset=true;
	char *dataptr;
	int offset_value;
	int incbit;
	int offset_new;
	offset_value=0;
	int len;
	int assignbit;
	int stringvalue;
	int intvalue;
	
	
	

	
	if(callattroffset!=false)
	{
	incbit=1;
	assignbit=1;
	// Retrieve the attribute offset value based on the attribute number.
	attrOffset(schema, attributeNumber, &offset_value);
	
	}

	if (incbit!=0)
	{
	//Retrieve the starting memory address of the record's data.
	dataptr = record->data;

	dataptr = incrementPointer(dataptr, offset_value);
	}
	if (assignbit!=0)
	{

	
	stringvalue=DT_STRING;
	intvalue=DT_INT;
	
	if ((int) schema->dataTypes[attributeNumber] == stringvalue)
	{
		// Assigning a value to an attribute of type STRING.
		// Fetching the length of the string as defined in the schema during creation.
		len = schema->typeLength[attributeNumber];

		// Copying the attribute's value to the record pointer.
		strncpy(dataptr, value->v.stringV, len);
		offset_new = schema->typeLength[attributeNumber];
		
		if (incbit!=0)
		{
			dataptr = incrementPointer(dataptr, offset_new);
		}

		
	}

	else if ((int) schema->dataTypes[attributeNumber] == intvalue)
	{
		// Assigning a value to an attribute of type INTEGER.
		*(int64_t *)dataptr = value->v.intV;

		if (incbit!=0)
		{
			dataptr = incrementPointer(dataptr, sizeof(int64_t));
		}

		
	}

	else
	{
		printf("The data type provided does not have a Serializer. \n");
	}
	}

	return RC_OK;
}
//end























NodeData *makeRecord(RID *rid) {
   NodeData *record = (NodeData *) calloc(1, sizeof(NodeData));
    if (!record) {
        perror("Error creating NodeData.");
        exit(RC_INSERT_ERROR);
    }
   if(record!=NULL){
    (*record).rid.page = (*rid).page;
   (*record).rid.slot = (*rid).slot;
   }
    return record;
}

Node* createNewTree(BTreeManager* treeManager, Value* key, NodeData* pointer) {
    int b_tree_order;
    int i=0;
    b_tree_order = treeManager->order;
    Node* root = createLeaf(treeManager);
    if(i==0){
    *root->keys = key;
    *(root->pointers) = pointer;
    *(root->pointers + b_tree_order - 1) = 0;
    (*root).parent = 0;
    root->num_keys += 1;

    treeManager->numEntries += 1;
    }
    return root;
}

Node * insertIntoLeaf(BTreeManager * treeManager, Node * leaf, Value * key, NodeData * pointer) {
    if(traceDebug_) printf("\ninsertIntoLeaf()\n");
    int i;
    int insertion;
    insertion = 0;
    treeManager->numEntries = treeManager->numEntries + 1;
    
 

	while (insertion< leaf->num_keys) {
		if (!isLess(leaf->keys[insertion], key))
			break;
		insertion += 1;
	}
    
    // Shift keys and pointers to make room for the new key
    for (i = leaf->num_keys - 1; i >= insertion; i--) {
        leaf->keys[i + 1] = leaf->keys[i];
        leaf->pointers[i + 1] = leaf->pointers[i];
    }
    
    // Insert the new key and pointer
    *(leaf->keys + insertion) = key;
    *(leaf->pointers + insertion) = pointer;
    leaf->num_keys += 1;
    
    return leaf;
}

extern Node * insertIntoLeafAfterSplitting(BTreeManager * treeManager, Node * leaf, Value * key, NodeData * pointer) {
    if(traceDebug_) printf("\ninsertIntoLeafAfterSplitting()\n");
    Node * leaf_new; // the new leaf node
	Value ** keys_tp; // temporary pointers array for keys // original code -> Value ** keys_tp;

	void ** pointers_tp; // temporary pointers array for pointers
	int sp_lit; // split point
	

	// int key_new; // new key
	int insert_id; // position to insert the new key
    leaf_new = createLeaf(treeManager); // create a new leaf node
	int B_tree_order; // order of the B-tree
	B_tree_order = treeManager->order;
    
    // allocate memory for temporary pointers arrays
	keys_tp = (Value **) malloc(B_tree_order * sizeof(Value*));
	if (!keys_tp) {
		if(traceDebug_) fprintf(stderr, "Error: Temporary pointers array. \n");
        //return RC_INSERT_ERROR;
		exit(EXIT_FAILURE);
	}
    
    pointers_tp = (void **) malloc(B_tree_order * sizeof(void*));
	if (!pointers_tp) {
		if(traceDebug_) fprintf(stderr, "Error: Temporary pointers array. \n");
        //return RC_INSERT_ERROR;
		exit(EXIT_FAILURE);
	}


    // find the position to insert the new key
    insert_id = 0;
    while (insert_id < B_tree_order - 1) {
        if (isLess(leaf->keys[insert_id], key)) {
            insert_id++;
        } else {
            break;
        }
    }

    int iter = 0;

    // distribute the entries between the old and new nodes
    int i = 0;
	while (i < leaf->num_keys) {
		bool boo = true;
		if (boo != false)
		{
			if (iter == insert_id) {
			iter++;
		}
		}
		if(traceDebug_) printf("Distributing old and new nodes\n");
	
		keys_tp[iter] = leaf->keys[i];
		pointers_tp[iter] = leaf->pointers[i];
		iter++;
		i++;
	}


    *(keys_tp + insert_id) = key;
    *(pointers_tp + insert_id) = pointer;

    leaf->num_keys = 0;

    // find the split point
    sp_lit = (B_tree_order - 1) % 2 == 0 ? (B_tree_order - 1) / 2 : (B_tree_order - 1) / 2 + 1;

    // copy the first half of the entries to the old node
    for (int i = 0; i < sp_lit; i++) {
        *(leaf->pointers + i) = /*(Node *) */ *(pointers_tp + i);
        *(leaf->keys + i) = *(keys_tp + i);
        leaf->num_keys += 1;
    }

    // copy the second half of the entries to the new node
	// int iterV;
    for (int i = sp_lit, iterV = 0; i < B_tree_order; i++, iterV++) {
        *(leaf_new->pointers + iterV) = /*(Node *) */ *(pointers_tp + i);
        *(leaf_new->keys + iterV) = *(keys_tp + i);
        leaf_new->num_keys += 1;
    }

    free(pointers_tp);
    free(keys_tp);

    *(leaf_new->pointers + B_tree_order - 1) = *(leaf->pointers + B_tree_order - 1);
    leaf->pointers[B_tree_order - 1] = leaf_new; //leaf->pointers[B_tree_order - 1] = &(*leaf_new);

    // set the remaining pointers to NULL
    int b = 1;
	
	

	  for (int i = leaf->num_keys; i < B_tree_order - 1; i++) 
	
	  {
		  *(leaf->pointers + i) = 0;
      }  

	
	if(b==1){

	  for (int i = leaf_new->num_keys; i < B_tree_order - 1; i++) 
	   {
		  *(leaf_new->pointers + i) = 0;
        }
	}

	leaf_new->parent = leaf->parent;
	//key_new = *(leaf_new->keys);
	// added
	//malloc
	treeManager->numEntries = treeManager->numEntries + 1;

	return insertIntoParent(treeManager, leaf, /*key_new*/ *(leaf_new->keys), leaf_new);
}

extern Node* insertIntoNodeAfterSplitting(BTreeManager* treeManager, Node* oldNode, int leftIndex, Value* key, Node* rightNode) {
    if(traceDebug_) printf("\ninsertIntoNodeAfterSplitting()\n");
    int i; 
    int j;
    int splitIndex;
    // int kPrime;
    Node* newNode;
    Node* childNode;
    Value** tempKeys;
    Node** tempPointers;
    int order = treeManager->order;
    int maxKeys = order - 1;

    
    tempPointers = (Node**) malloc((order + 1) * sizeof(Node *));

    tempKeys = (Value **) malloc(order * sizeof(Value*));
    if (tempPointers == NULL || tempKeys == NULL) {
        perror("Failed to allocate memory for temporary arrays");
        exit(RC_INSERT_ERROR);
    }

    
    for (i = 0, j = 0; i < oldNode->num_keys + 1; i++, j++) {
        if (j == leftIndex + 1) {
            //tempPointers[i] = rightNode;
            j++;
        }
        tempPointers[j] = (Node *) oldNode->pointers[i];
        
    }

    for (i = 0, j = 0; i < oldNode->num_keys; i++, j++) {
        if (j == leftIndex) {
            //tempKeys[j] = key;
            j++;
        }
        tempKeys[j] = oldNode->keys[i];
    }

    tempPointers[leftIndex + 1] = rightNode;
    tempKeys[leftIndex] = key;

    
    if (maxKeys % 2 == 0) {
        splitIndex = maxKeys / 2;
    }
    else {
        splitIndex = (maxKeys + 1) / 2;
    }
    newNode = createNode(treeManager);

    
    oldNode->num_keys = 0;
    for (i=0; i < splitIndex - 1; i++) {
        *(oldNode->pointers + i) = *(tempPointers + i);    
        *(oldNode->keys + i) = *(tempKeys + i);
        oldNode->num_keys = oldNode->num_keys + 1;
   }

    oldNode->pointers[i] = tempPointers[i];
    Value* myP = (Value *) malloc(sizeof(Value));
    myP->dt     = 0;
    myP->v.intV = (int64_t) (tempKeys[splitIndex - 1])->v.intV;

    newNode->num_keys = 0;
    for(++i, j=0; i < order; i++, j++) { 
       newNode->pointers[j] = tempPointers[i];
       newNode->keys[j] = tempKeys[i];
       newNode->num_keys++;
    }

    newNode->pointers[j] = tempPointers[i];
    free(tempPointers);
    free(tempKeys);
    newNode->parent = oldNode->parent;

    for (i = 0; i <= newNode->num_keys; i++) {
		childNode = (Node *) newNode->pointers[i];
		childNode->parent = newNode;
	}

    
    treeManager->numEntries += 1;
    return insertIntoParent(treeManager, oldNode, myP, newNode);
}

extern Node * insertIntoParent(BTreeManager * treeManager, Node * left, Value * key, Node * right) {

	if(traceDebug_) printf("\ninsertIntoParent()\n");
	
	Node * parent = left->parent;
	
	if (parent == 0){

		  return insertIntoNewRoot(treeManager, left, key, right);
	}
	
	int left_id_index = getLeftIndex(parent, left);
     
	if (treeManager->order - 1  > parent->num_keys) {

		return insertIntoNode(treeManager, parent, left_id_index, key, right);
	}

	return insertIntoNodeAfterSplitting(treeManager, parent, left_id_index, key, right);
}

extern int getLeftIndex(Node * parent, Node * left) {
    if(traceDebug_) printf("\ngetLeftIndex()\n");
    // Initialize variables
    int left_index;
    left_index = 0;

    // Find the index of the left node in the parent's pointers array
    while (left_index <= parent->num_keys) {
        if (parent->pointers[left_index] == left) {
            return left_index;
        }
        left_index += 1;
    }

    // If the left node is not found, return -1
    return -1;
}


Node * insertIntoNode(BTreeManager * treeManager, Node * parent, int left_index, Value * key, Node * right) {
    if(traceDebug_) printf("\ninsertIntoNode()\n");
    int num_keys = parent->num_keys;
    int i;

    // Shift pointers and keys to make room for the new entry
    for (i = num_keys; i > left_index; i--) {
        parent->pointers[i + 1] = parent->pointers[i];
        parent->keys[i] = parent->keys[i - 1];
    }

    // Insert the new key and pointer
    parent->pointers[left_index + 1] = right;
    parent->keys[left_index] = key;
    parent->num_keys++;

    // If the parent was the root, update the root pointer in the tree manager
    if (parent == treeManager->root) {
        treeManager->root = parent;
    }

    return parent;
}

Node* insertIntoNewRoot(BTreeManager* treeManager, Node* left, Value* key, Node* right) {
    if(traceDebug_) printf("\ninsertIntoNewRoot()\n");
    Node* newRoot = createNode(treeManager);
	int zero= 0;
	int one=1;
    newRoot->keys[zero] = key;
    newRoot->pointers[zero] = left;
    newRoot->pointers[one] = right;
    newRoot->num_keys = newRoot->num_keys + one;
    newRoot->parent = NULL;
    right->parent = newRoot;
    left->parent = newRoot;
    return newRoot;
}

Node * createNode(BTreeManager * treeManager) {
    if(traceDebug_) printf("\ncreateNode()\n");
    int b_tree_order;
    b_tree_order = treeManager->order;
    int numNodes = treeManager->numNodes;
    numNodes = numNodes + 1;
    treeManager->numNodes = numNodes;
    
    
    Node * n_node = (Node*) calloc(1, sizeof(Node));

    if (!n_node) {
        if(traceDebug_) fprintf(stderr, "Error creating node.\n");
        exit(RC_INSERT_ERROR);
    }

    n_node->keys = (Value **) calloc(b_tree_order - 1, sizeof(Value *));
	
    if (!n_node->keys) {
        if(traceDebug_) fprintf(stderr, "Failed to create new node keys array.\n");
        exit(RC_INSERT_ERROR);
    }

    n_node->pointers = (void **) calloc(b_tree_order, sizeof(void *));

    if (!n_node->pointers) {
        if(traceDebug_) fprintf(stderr, "New node pointers array..\n");
        exit(RC_INSERT_ERROR);
    }

    n_node->is_leaf = (bool) false;
    (*n_node).num_keys = 0;
    (*n_node).parent = NULL;
    (*n_node).next = NULL;

    return n_node;
}

// This function creates a new leaf node for the B-tree managed by treeManager.
// The leaf node is initialized with a default value of zero.
Node * createLeaf(BTreeManager * treeManager) {
    if(traceDebug_) printf("\ncreateLeaf()\n");
	// Initialize a temporary variable 'a' to zero.
	

	// Check if 'a' is equal to zero.
	
	// If 'a' is zero, create a new node using the createNode() function
	// provided by the B-tree manager, and set its 'is_leaf' attribute to true.
	Node * create_leaf = createNode(treeManager);
	create_leaf->is_leaf = (bool) true;

	// Return the newly created leaf node.
	return create_leaf;
	
}






// under test
Node * findLeaf(Node * root, Value * key) {
	if(key == NULL) if(traceDebug_) printf("warn! findLeaf: parameter 'key' is pointing the NULL address.\n");
	
	// int i;
	// i=0;
	Node * find_l = root;
	if (!find_l) {
		return find_l;
	}
	while (!find_l->is_leaf) {
		int k = 0;
		if(traceDebug_) printf("[");
		for(k = 0; k < find_l->num_keys - 1; k++) if(traceDebug_) printf("%ld ", (find_l->keys[k])->v.intV);

		if(traceDebug_) printf("%ld] ", (find_l->keys[k])->v.intV);
		int i = find_l->num_keys - 1;
		//if(traceDebug_) printf("\n454 line\n");

		while (i >= 0 && isLess(key, find_l->keys[i])) {
			i--;
		}

		find_l = (Node *) find_l->pointers[i+1];
	}

	return find_l;
}






NodeData *findRecord(Node *root, Value *key) {
    Node *leaf = findLeaf(root, key);
    if (!leaf) {
        return NULL;
    }
    int i = 0;
    while (i < leaf->num_keys && !isEqual(leaf->keys[i], key)) {
        i++;
    }
    if (i == leaf->num_keys) {
        return NULL;
    } else {
        return (NodeData *) leaf->pointers[i];
    }
}


//DELETION 
/*
This function returns the index of the nearest left sibling node of a given node, if one exists.
 If the node is the leftmost child and has no left sibling, the function returns -1 to indicate this special case.
*/

int getNeighborIndex(Node * n) {

	
	
	/* 
	 *The function should return the index of the key to the
	  left of the pointer in the parent that is pointing to node n. 
	  If node n happens to be the leftmost child, then the function should return -1.
	*/

	int j=0;
	int b;
	b =n->parent->num_keys;
	
	

	while(j <= b)
	{
		if (n->parent->pointers[j] == n)
			return j - 1;
		j++;
	}


	exit(RC_ERROR);
	
}








// Delete the record with the given key from the specified node.
Node * removeEntryFromNode(BTreeManager * treeManager, Node * n, Value * key, Node * pointer) {
	if(traceDebug_) printf("\nremoveEntryFromNode()\n");

	int i,j;
	int num_ptr;
	i = 0;
	j = 0;

	//Delete the specified key and shift the remaining keys to adjust for the removed key.
	

	int Ordbtree = treeManager->order;
	if(traceDebug_) printf("\n[removeEntryFromNode] j: %d..\n", j);


	while (!isEqual(n->keys[i], key)) {
		i++;
	}

	for(++i; i < n->num_keys; i++){
		n->keys[i - 1] = n->keys[i];
	}

	num_ptr = n->is_leaf? n->num_keys : n->num_keys + 1;
	i = 0;
	while (((Node *) (n->pointers[i])) != pointer)
	{
		i++;
	}
	for(++i; i < num_ptr; i++){
		n->pointers[i - 1] = n->pointers[i];
	}
	
	// Delete the specified pointer and shift the remaining pointers to adjust for the removed pointer.
	// Initially, calculate the number of pointers.
	
	treeManager->numEntries=treeManager->numEntries-1;

	n->num_keys=n->num_keys-1;
	
	// Set the remaining pointers to NULL to maintain a clean structure.
	// The final pointer in a leaf node points to the next adjacent leaf node.
	if (n->is_leaf)
	{
   		 for (i=n->num_keys;i < Ordbtree - 1; i++)
   		 {
        		n->pointers[i] = NULL;
    		}
	}
	else
	{
   		 for (i=n->num_keys + 1;i < Ordbtree; i++)
   		 {
        		n->pointers[i] = NULL;
    		}
	}

	if(traceDebug_) printf("LAST   Line of removeEntryFromNode pass success\n");
	return n;
}






// The purpose of this function is to modify the root node appropriately after a record has been removed from the B+ tree.
Node * adjustRoot(Node * root) {

	Node * root_latest;
	bool free_flag=true;

	//If the root node is not empty, it indicates that the key and pointer have already been removed. 
	//In this case, the function does nothing and simply returns the root node.
		
	if (root->num_keys <= 0)
	{
	
	if (root->is_leaf)
	{
		//If the root node is not empty and is a leaf node (has no children), the entire B+ tree is empty.
		root_latest = NULL;
	}
	else
	{
	
		//If the root node is not empty and has a child, set the first (and only) child as the new root node.
		root_latest = (Node *) root->pointers[0];
		root_latest->parent = NULL;
	}
	
	}
	else
	{
		return root;
	}
	if (free_flag!=false)
	{
	//Release the allocated memory space to free up memory.
	
	

	free(root->keys);
	if(traceDebug_) printf("It is deallocated");
	free(root->pointers);
	if(traceDebug_) printf("It is deallocated");
	free(root);
	if(traceDebug_) printf("It is deallocated");
	}
	return root_latest;
}



/*
This function merges a node that has become too small after removing entries with a 
neighboring node that can accommodate the additional entries without violating the maximum limit.
*/
Node * mergeNodes(BTreeManager * treeManager, Node * n, Node * neighbor, int neighbor_index, Value* k_prime) {
	if(traceDebug_) printf("\nmergeNodes()\n");
	Node * temporary;
	int copyflag=0;
	int index_val;
	int end_val;
	int i;
	int j=0;
	int s=0;
	bool free_flag=true;
	if(copyflag!=1)
	{
	// If a node is positioned at the far left and its neighbor is to the right, swap the node with its neighbor.
	if (neighbor_index == -1) 
	{
		temporary = n;
		for(i=0;i<1;i++)
		{
		n = neighbor;
		neighbor = temporary;
		}
		if(traceDebug_) printf("value stored to neighbor node ");
	}
	}
	int Ord_btree = treeManager->order;
	/*
	In the special case where n is the leftmost child and has been swapped with its neighbor, 
	the starting point for copying keys and pointers from n is located in its neighbor.
	*/
	index_val = neighbor->num_keys;


	if(n->is_leaf) 
	{
		i = index_val;
		while(j < n->num_keys)
		{
			neighbor->keys[i] = n->keys[j];
			s++;
			neighbor->pointers[i] = n->pointers[j];
			i++;
			neighbor->num_keys=neighbor->num_keys+1;
			j++;
		}
		
		neighbor->pointers[Ord_btree - 1] = n->pointers[Ord_btree - 1];
	}
	else {
		neighbor->keys[index_val] = k_prime;
		neighbor->num_keys=neighbor->num_keys+1;

		end_val = n->num_keys;
		j=0;
		i = index_val + 1;
		while(j < end_val)
		{
			neighbor->keys[i] = n->keys[j];
			s++;
			neighbor->pointers[i] = n->pointers[j];
			i++;
			neighbor->num_keys=neighbor->num_keys+1;
			
			n->num_keys=n->num_keys-1;
			j++;

		}

		neighbor->pointers[i] = n->pointers[j];
		i=0;

		// Setting all child nodes to have the same parent node.
		while(i < neighbor->num_keys + 1)
		{
			temporary = (Node *) neighbor->pointers[i];
			i++;
			temporary->parent = neighbor;
		}

	} 

        //Value* mykP = (Value *) malloc(sizeof(Value));
        //mykP->dt     = 0;
        //mykP->v.intV = (int) (tempKeys[k_prime])->v.intV;
	treeManager->root = deleteEntry(treeManager, n->parent, k_prime, n);

	if (free_flag!=false)
	{
	//Release the allocated memory space to free up memory.
	
	

	free(n->keys);
	if(traceDebug_) printf("It is deallocated");
	free(n->pointers);
	if(traceDebug_) printf("It is deallocated");
	free(n);
	if(traceDebug_) printf("It is deallocated");
	}

	return treeManager->root;
}

/* The purpose of this function is to remove a specific entry from the B+ tree. It first removes the record 
with the given key and pointer from the appropriate leaf node, and then makes any
 necessary adjustments to maintain the integrity of the B+ tree.
 */
Node * deleteEntry(BTreeManager * treeManager, Node * n, Value * key, void * pointer) {
	if(traceDebug_) printf("\ndeleteEntry()\n");
	Node * neighbor_node;

	int mininum_key;
	int Ordbtree = treeManager->order;
	int indx_neighbor;
	int indx_k_prime;
	//int s=0;
	Value* k_prime_val;
	//int k_prime_val;
	int cap;
	

	// Delete the specified key and pointer from the node.
	if(traceDebug_) printf("\nnow, 'Delete the specified key and pointer from the node.'\n");
	n = removeEntryFromNode(treeManager, n, key, (Node *) pointer); // <- under test mark!!
	

	// If the node n is the root of the B+ tree, perform adjustments to the tree structure.
	if (n == treeManager->root)
	{
		return adjustRoot(treeManager->root);
	}

	// Calculate the minimum size that a node can have and still preserve the B+ tree structure after a deletion.

	if(!n->is_leaf) 
	{
		if ((Ordbtree) % 2 == 1)
		{
		mininum_key = (Ordbtree) / 2 + 1;
		}
			
		else
		{
			mininum_key = (Ordbtree) / 2;
		}
		mininum_key=mininum_key-1;
	}
	else
	{
		if ((Ordbtree - 1) % 2 == 1)
		{
			mininum_key = (Ordbtree - 1) / 2 + 1;
			
		}
		else
		{
			mininum_key = (Ordbtree - 1) / 2;
		}
	} 
	if (n->num_keys < mininum_key)
	{
		if(traceDebug_) printf("continuing ");
	}	
	else
	{
		return treeManager->root;
	}
	/*
	If the node falls below the minimum allowable size after deletion, 
	it's necessary to either merge it with a neighbor or redistribute keys between them. 
	To do so, identify the appropriate neighbor node and the key (k_prime) 
	in the parent node that separates n from the neighbor.*/

	indx_neighbor = getNeighborIndex(n);
	if (indx_neighbor != -1) {
    indx_k_prime = indx_neighbor;
	}
	else {
    indx_k_prime = 0;
	} 
	k_prime_val = (Value *)malloc(sizeof(Value));
	k_prime_val->dt = 0;
	k_prime_val->v.intV = n->parent->keys[indx_k_prime]->v.intV; // high possible error point marking

	if (indx_neighbor != -1) 
	{
    neighbor_node = (Node *) n->parent->pointers[indx_neighbor];
	}
	else {
    neighbor_node = (Node *) n->parent->pointers[1];
	} 

	if (!n->is_leaf) 
	{
		cap = Ordbtree-1;
	} 
	else 
	{
		cap = Ordbtree;
	}
	if (neighbor_node->num_keys + n->num_keys >= cap)
	{
		// Redistributing nodes
		return redistributeNodes(treeManager->root, n, neighbor_node, indx_neighbor, indx_k_prime, k_prime_val);
	}

	else
	{
		// Merging nodes
		return mergeNodes(treeManager, n, neighbor_node, indx_neighbor, k_prime_val);
		if(traceDebug_) printf("merging is done");
	}
	if(traceDebug_) printf("Continue ");
	
}




// The record with the specified key is removed using this function.
Node * delete(BTreeManager * treeManager, Value * key) 
{
	if(traceDebug_) printf("Starting delete");
	
	bool free_val=true;
	if(traceDebug_) printf("initialising node");
	Node * leaf_key = findLeaf(treeManager->root, key);
	// int s=0;
	NodeData * record_value = findRecord(treeManager->root, key);
	
	if(traceDebug_) printf("continue");
	if(record_value != NULL)
	{
		if(leaf_key != NULL)
		{
			treeManager->root = deleteEntry(treeManager, leaf_key, key, record_value);
			if(free_val!=false)
			{
			free(record_value);
			}
		}

	}

	return treeManager->root;
	if(traceDebug_) printf("already returned");
}

/*
When one node becomes too small after deletion but its neighbor is too large to accommodate the small node's 
entries without surpassing the maximum, this function reassigns the entries between the two nodes.
*/
Node * redistributeNodes(Node * root, Node * n, Node * neighbor, int neighbor_index, int k_prime_index, Value* k_prime) {
	int i;
	int s=0;
	int flg=1;
	Node * temp_node;
	int j=1;
    if (neighbor_index == -1) {
		//If "n" is the first child, get a key-pointer pair from its neighbor to the right. 
		//Afterwards, move the leftmost key-pointer pair from the neighbor to the far-right position of "n".
		if (!n->is_leaf) 
		{
			n->keys[n->num_keys] = k_prime;
			n->pointers[n->num_keys + 1] = neighbor->pointers[0];
			if(traceDebug_) printf("running");
			if(flg!=0)
			{
			temp_node = (Node *) n->pointers[n->num_keys + 1];
			temp_node->parent = n;
			}
			s--;
			n->parent->keys[k_prime_index] = neighbor->keys[0];
		}
		else {
			n->keys[n->num_keys] = neighbor->keys[0];
			if(flg!=0)
			{
			n->pointers[n->num_keys] = neighbor->pointers[0];
			}
			s++;
			n->parent->keys[k_prime_index] = neighbor->keys[1];
		} 
		i=0;
		s=0;
		while(i < neighbor->num_keys - 1)
		{
			neighbor->keys[i] = neighbor->keys[i + 1];
			s++;
			neighbor->pointers[i] = neighbor->pointers[i + 1];
			i++;
		}
		if (n->is_leaf)
		{
			if(traceDebug_) printf("continue");
		}
		else
		{
			neighbor->pointers[i] = neighbor->pointers[i + 1];
			s++;
		}
	}
	else 
	{
		s--;
		// Assuming that "n" has a neighbor to its left, move the last key-pointer pair of the neighbor from its right end to the left end of "n".
		
		if (!n->is_leaf)
		{
			n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
		}
		s=0;
		i = n->num_keys;
		while(i > 0)
		{
			n->keys[i] = n->keys[i - 1];
			s++;
			n->pointers[i] = n->pointers[i - 1];
			i--;
		}
		s=0;
		if (n->is_leaf) {
			if(flg!=0)
			{
			n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
			}
			if(j!=0)
			{
			if(traceDebug_) printf("continue");
			neighbor->pointers[neighbor->num_keys - 1] = NULL; // j vs just constant 1
			//neighbor->pointers[neighbor->num_keys - j] = NULL;
			}
			s++;
			n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
			if(traceDebug_) printf("running");
			n->parent->keys[k_prime_index] = n->keys[0];
		}

		else {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys];
			if(flg!=0)
			{
			temp_node = (Node *) n->pointers[0];
			}
			if(traceDebug_) printf("using temp");
			temp_node->parent = n;
			s++;
			neighbor->pointers[neighbor->num_keys] = NULL;
			if(traceDebug_) printf("running");
			n->keys[0] = k_prime;
			s--;
			n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
		} 
	} 
	
	if(flg!=0)
	{
		// Currently, 'n' possesses an additional key and pointer, while its neighbor has one less of both.
		n->num_keys=n->num_keys+1;
		s--;
		neighbor->num_keys=neighbor->num_keys-1;
	}
		return root;
	}


// PRINT FUNCTION 

//  B+ Tree print function
void enqueue(BTreeManager * treeManager, Node * new_node) {
	
	int s=0;
	Node * encq;
	if (treeManager->queue != NULL) {
		encq = treeManager->queue;
		while (encq->next != NULL) 
		{
			encq = encq->next;
		}
		encq->next = new_node;
		s++;
		new_node->next = NULL;
	}

	else {
		treeManager->queue = new_node;
		s++;
		treeManager->queue->next = NULL;
	} 
}

// B+ Tree print function
Node * dequeue(BTreeManager * treeManager) {
	Node * n;
	bool assign=true;
	//if(traceDebug_) printf("\npass dequeue check point 1\n");
	n = treeManager->queue;
	//if(traceDebug_) printf("\npass dequeue check point 2\n");
	int s=0;
	if (assign!=false)
	{
	treeManager->queue = treeManager->queue->next;
	//if(traceDebug_) printf("\npass dequeue check point 3\n");
	}
	n->next = NULL;
	s++;
	//if(traceDebug_) printf("\nlast line of dequeue() pass success\n");
	return n;
}

// The length in edges of the path from any node to the root can be obtained using this function.
int path_to_root(Node * root, Node * child) 
{
	int len = 0;
	bool fl = true;
	Node * chld = child;
	while(chld == root)
	{
		continue;
	}
	while (chld != root) 
	{
		chld = chld->parent;
		if(fl!=false)
		{
		len=len+1;
		}
	}
	return len;
}



/*********** SUPPORT MULTIPLE DATATYPES *************/

// The following function determines whether the first key is smaller than the second key and returns TRUE if that is the case.
bool isLess(Value * key1, Value * key2) 
{
	

	// int d;
	int64_t a,b;
	
	
	// d=key1->dt;
	
	a=key1->v.intV;
	
	b=key2->v.intV;
	
	
	int s=0;
	

	if(traceDebug_) printf("\nisLess() ALL PASS ! --------- END ! \n");
	if (a>= b) {
			s++;
			return FALSE;
		}
		else 
		{
			s++;
			return TRUE;
		} 
}

// The following function determines whether two keys are equal, and returns TRUE if they are, and FALSE otherwise.
bool isEqual(Value * key1, Value * key2) {

	//if(traceDebug_) printf("\nisEqual()\n");

	if(key1 == NULL || key2 == NULL) {
		if(traceDebug_) printf("either key1 or key2: NULL\n");
		return FALSE;
	}

	
	//key1->dt = 0;
	//key2->dt = 0;
	
	
	//int d;
	int64_t a,b;
	
	int s=0;
	//d=key1->dt;
	
	a=key1->v.intV;
	b=key2->v.intV;
	
	if(traceDebug_) printf("\nALL PASS !\n");
	
	if (a != b) {
			s++;
			return FALSE;
		}
		else {
			s++;
			return TRUE;
		} 
}

// The following function evaluates whether the first key is greater than the second key and outputs a value of TRUE.
bool isGreater(Value * key1, Value * key2) {

	//int d;
	int64_t a,b;
	
	//d=key1->dt;
	a=key1->v.intV;
	b=key2->v.intV;
	
	int s=0;

	if (a <= b)
	{
		s++;
			
		return FALSE;
	}
	else {
		s++;
			
		return TRUE;
	} 
}





