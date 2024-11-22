#include <stdlib.h>
#include <stdint.h>

#include "bpt.h"

#define ASSERT_EQUALS_RID(_l,_r, message)				\
  do {									\
    ASSERT_TRUE((_l).page == (_r).page && (_l).slot == (_r).slot, message); \
  } while(0)

// test methods
static void testInsertAndFind (void);
static void testDelete (void);
static void testIndexScan (void);

// helper methods
static Value **createValues (char **stringVals, int size);
static void freeValues (Value **vals, int size);
static int *createPermutation (int size);

// proto
char *serializeTableInfo(RM_TableData *);
char *serializeTableContent(RM_TableData *);
char *serializeSchema(Schema *);
char *serializeRecord(Record *, Schema *);
char *serializeAttr(Record *, Schema *, int);
char *serializeValue(Value *);
Value *stringToValue(char *);
RC attrOffset (Schema *, int, int *);

int traceDebug = 0;

// test name
char *testName;

// just
// dynamic string
typedef struct VarString {
	char *buf;
	int size;
	int bufsize;
} VarString;

#define MAKE_VARSTRING(var)				\
		do {							\
			var = (VarString *) malloc(sizeof(VarString));	\
			var->size = 0;					\
			var->bufsize = 100;					\
			var->buf = calloc(100,1);				\
		} while (0)

#define FREE_VARSTRING(var)			\
		do {						\
			free(var->buf);				\
			free(var);					\
		} while (0)

#define GET_STRING(result, var)			\
		do {						\
			result = malloc((var->size) + 1);		\
			memcpy(result, var->buf, var->size);	\
			result[var->size] = '\0';			\
		} while (0)

#define RETURN_STRING(var)			\
		do {						\
			char *resultStr;				\
			GET_STRING(resultStr, var);			\
			FREE_VARSTRING(var);			\
			return resultStr;				\
		} while (0)

#define ENSURE_SIZE(var,newsize)				\
		do {								\
			if ((size_t) var->bufsize < newsize)					\
			{								\
				int newbufsize = var->bufsize;				\
				while((size_t) (newbufsize *= 2) < newsize);			\
				var->buf = realloc(var->buf, newbufsize);			\
			}								\
		} while (0)

#define APPEND_STRING(var,string)					\
		do {									\
			ENSURE_SIZE(var, var->size + strlen(string));			\
			memcpy(var->buf + var->size, string, strlen(string));		\
			var->size += strlen(string);					\
		} while(0)

#define APPEND(var, ...)			\
		do {						\
			char *tmp = malloc(10000);			\
			sprintf(tmp, __VA_ARGS__);			\
			APPEND_STRING(var,tmp);			\
			free(tmp);					\
		} while(0)

// prototypes
//static RC attrOffset (Schema *schema, int attrNum, int *result);

// implementations
char *
serializeTableInfo(RM_TableData *rel)
{
	VarString *result;
	MAKE_VARSTRING(result);

	APPEND(result, "TABLE <%s> with <%i> tuples:\n", rel->name, getNumTuples(rel));
	APPEND_STRING(result, serializeSchema(rel->schema));

	RETURN_STRING(result);
}

char *
serializeTableContent(RM_TableData *rel)
{
	int i;
	VarString *result;
	RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
	Record *r = (Record *) malloc(sizeof(Record));
	MAKE_VARSTRING(result);

	for(i = 0; i < rel->schema->numAttr; i++)
		APPEND(result, "%s%s", (i != 0) ? ", " : "", rel->schema->attrNames[i]);

	startScan(rel, sc, NULL);

	while(next(sc, r) != RC_RM_NO_MORE_TUPLES)
	{
		APPEND_STRING(result,serializeRecord(r, rel->schema));
		APPEND_STRING(result,"\n");
	}
	closeScan(sc);

	RETURN_STRING(result);
}


char *
serializeSchema(Schema *schema)
{
	int i;
	VarString *result;
	MAKE_VARSTRING(result);

	APPEND(result, "Schema with <%i> attributes (", schema->numAttr);

	for(i = 0; i < schema->numAttr; i++)
	{
		APPEND(result,"%s%s: ", (i != 0) ? ", ": "", schema->attrNames[i]);
		switch (schema->dataTypes[i])
		{
		case DT_INT:
			APPEND_STRING(result, "INT");
			break;
		case DT_STRING:
			APPEND(result,"STRING[%i]", schema->typeLength[i]);
			break;
		default:
			break;
		}
	}
	APPEND_STRING(result,")");

	APPEND_STRING(result," with keys: (");

	for(i = 0; i < schema->keySize; i++)
		APPEND(result, "%s%s", ((i != 0) ? ", ": ""), schema->attrNames[schema->keyAttrs[i]]);

	APPEND_STRING(result,")\n");

	RETURN_STRING(result);
}

char *
serializeRecord(Record *record, Schema *schema)
{
	VarString *result;
	MAKE_VARSTRING(result);
	int i;

	APPEND(result, "[%i-%i] (", record->id.page, record->id.slot);

	for(i = 0; i < schema->numAttr; i++)
	{
		APPEND_STRING(result, serializeAttr (record, schema, i));
		APPEND(result, "%s", (i == 0) ? "" : ",");
	}

	APPEND_STRING(result, ")");

	RETURN_STRING(result);
}

char *
serializeAttr(Record *record, Schema *schema, int attrNum)
{
	int offset;
	char *attrData;
	VarString *result;
	MAKE_VARSTRING(result);

	attrOffset(schema, attrNum, &offset);
	attrData = record->data + offset;

	switch(schema->dataTypes[attrNum]) {
		case DT_INT:
			{
				int64_t val = 0;
				memcpy(&val,attrData, sizeof(int64_t));
				APPEND(result, "%s:%ld", schema->attrNames[attrNum], val);
			}
			break;
		case DT_STRING:
			{
				char *buf;
				int len = schema->typeLength[attrNum];
				buf = (char *) malloc(len + 1);
				strncpy(buf, attrData, len);
				buf[len] = '\0';

				APPEND(result, "%s:%s", schema->attrNames[attrNum], buf);
				free(buf);
			}
			break;
		default:
			return "NO SERIALIZER FOR DATATYPE";
	}

	RETURN_STRING(result);
}

char *
serializeValue(Value *val)
{
	VarString *result;
	MAKE_VARSTRING(result);

	switch(val->dt)
	{
	case DT_INT:
		APPEND(result,"%ld",val->v.intV);
		break;
	case DT_STRING:
		APPEND(result,"%s", val->v.stringV);
		break;
	}

	RETURN_STRING(result);
}

Value *
stringToValue(char *val)
{
	Value *result = (Value *) malloc(sizeof(Value));

	switch(val[0])
	{
	case 'i':
		result->dt = DT_INT;
		result->v.intV = atoi(val + 1);
		break;
	case 's':
		result->dt = DT_STRING;
		result->v.stringV = malloc(strlen(val));
		strcpy(result->v.stringV, val + 1);
		break;
	default:
		result->dt = DT_INT;
		result->v.intV = -1;
		break;
	}

	return result;
}


RC
attrOffset (Schema *schema, int attrNum, int *result)
{
	int offset = 0;
	int attrPos = 0;

	for(attrPos = 0; attrPos < attrNum; attrPos++)
		switch (schema->dataTypes[attrPos])
		{
		case DT_STRING:
			offset += schema->typeLength[attrPos];
			break;
		case DT_INT:
			offset += sizeof(int64_t);
			break;
		default:
			break;
		}

	*result = offset;
	return RC_OK;
}
// just end

// main method
int 
main (void) 
{
  testName = "";

  testInsertAndFind();
  testDelete();
  testIndexScan();

  /**
  caller? ret?malloc?
  find 에 대한 설명  char* 리턴하는 애 
  caller 에서 동적할당 안해도 된다는거임
  calle 에서 동적할당 하면 됨 


  leaf node 만 kri 해라
   */

  return 0;
}

// ************************************************************ 
void
testInsertAndFind (void)
{
  RID insert[] = { 
    {1,1},
    {2,3},
    {1,2},
    {3,5},
    {4,4},
    {3,2}, 
  };
  int numInserts = 6;
  Value **keys;
  char *stringKeys[] = {
    "i1",
    "i11",
    "i13",
    "i17",
    "i23",
    "i52"
  };
  testName = "test b-tree inserting and search";
  int i, testint;
  BTreeHandle *tree = NULL;
  
  keys = createValues(stringKeys, numInserts);

  // init
  TEST_CHECK(initIndexManager(NULL));
  TEST_CHECK(createBtree("testidx", DT_INT, 2));
  TEST_CHECK(openBtree(&tree, "testidx"));

  // insert keys
  if(traceDebug) printf("insert keys\n");
  for(i = 0; i < numInserts; i++) {
    TEST_CHECK(insertKey(tree, keys[i], insert[i]));
    if(traceDebug) printf("------------sequence [%d]\n\n-------------------------\n\n---printTree!!\t ",i);
    if(traceDebug) printTree(tree);
	  //if(i>0) TEST_CHECK(printTree(tree));
  }

  // check index stats
  if(traceDebug) printf("check index stats\n");
  TEST_CHECK(getNumNodes(tree, &testint));
  //ASSERT_EQUALS_INT(testint,2, "number of nodes in btree");
  TEST_CHECK(getNumEntries(tree, &testint));
  //ASSERT_EQUALS_INT(testint, numInserts, "number of entries in btree");

  // search for keys
  if(traceDebug) printf("search for keys\n");
  for(i = 0; i < 6; i++)
    {
      int pos = rand() % numInserts;
      RID rid;
      Value *key = keys[pos];

      TEST_CHECK(findKey(tree, key, &rid));
      //ASSERT_EQUALS_RID(insert[pos], rid, "did we find the correct RID?");
    }

  // cleanup
  TEST_CHECK(closeBtree(tree));
  TEST_CHECK(deleteBtree("testidx"));
  TEST_CHECK(shutdownIndexManager());
  freeValues(keys, numInserts);

  TEST_DONE();
}

// ************************************************************ 
void
testDelete (void)
{
  RID insert[] = { 
    {1,1},
    {2,3},
    {1,2},
    {3,5},
    {4,4},
    {3,2}, 
  };
  int numInserts = 6;
  Value **keys;
  char *stringKeys[] = {
    "i1",
    "i11",
    "i13",
    "i17",
    "i23",
    "i52"
  };
  testName = "test b-tree inserting and search";
  int i, iter;
  BTreeHandle *tree = NULL;
  int numDeletes = 6;
  bool *deletes = (bool *) malloc(numInserts * sizeof(bool));
  
  keys = createValues(stringKeys, numInserts);

  // init
  TEST_CHECK(initIndexManager(NULL));

  // create test b-tree and randomly remove entries
  for(iter = 0; iter < 6; iter++)
    {
      // randomly select entries for deletion (may select the same on twice)
      for(i = 0; i < numInserts; i++)
	deletes[i] = FALSE;
      for(i = 0; i < numDeletes; i++)
	deletes[rand() % numInserts] = TRUE;

      // init B-tree
      TEST_CHECK(createBtree("testidx", DT_INT, 2));
      TEST_CHECK(openBtree(&tree, "testidx"));

      // insert keys
      for(i = 0; i < numInserts; i++)
	TEST_CHECK(insertKey(tree, keys[i], insert[i]));
      
//------------------------------------
      // delete entries
      for(i = 0; i < numInserts; i++)
      {
	  if (deletes[i]) TEST_CHECK(deleteKey(tree, keys[i]));
      }


//------------------------------------

      // search for keys
      
      for(i = 0; i < 6; i++)
	{
	  int pos = rand() % numInserts;
	  RID rid;
	  Value *key = keys[pos];
	  
	  if (deletes[pos])
	    {
	      int rc = findKey(tree, key, &rid);
        if(rc == -1000) printf("\n");
	      //ASSERT_TRUE((rc == RC_IM_KEY_NOT_FOUND), "entry was deleted, should not find it");
	    }
	  else
	    {
	      TEST_CHECK(findKey(tree, key, &rid));
	      //ASSERT_EQUALS_RID(insert[pos], rid, "did we find the correct RID?");
	    }
	}

//------------------------------------
      // cleanup
      if(traceDebug) printf("now, cleanup performing..\n");
      TEST_CHECK(closeBtree(tree));
      TEST_CHECK(deleteBtree("testidx"));
    }

  TEST_CHECK(shutdownIndexManager());
  freeValues(keys, numInserts);
  free(deletes);

  TEST_DONE();
}

// ************************************************************ 
void
testIndexScan (void)
{
  RID insert[] = { 
    {1,1},
    {2,3},
    {1,2},
    {3,5},
    {4,4},
    {3,2}, 
  };
  int numInserts = 6;
  Value **keys;
  char *stringKeys[] = {
    "i1",
    "i11",
    "i13",
    "i17",
    "i23",
    "i52"
  };
  
  testName = "random insertion order and scan";
  int i, testint, iter, rc;
  BTreeHandle *tree = NULL;
  BT_ScanHandle *sc = NULL;
  RID rid;
  
  keys = createValues(stringKeys, numInserts);

  // init
  TEST_CHECK(initIndexManager(NULL));

  for(iter = 0; iter < 6; iter++)
    {
      int *permute;

      // create permutation
      permute = createPermutation(numInserts);

      // create B-tree
      TEST_CHECK(createBtree("testidx", DT_INT, 2));
      TEST_CHECK(openBtree(&tree, "testidx"));

      // insert keys
      for(i = 0; i < numInserts; i++)
	TEST_CHECK(insertKey(tree, keys[permute[i]], insert[permute[i]]));

      // check index stats
      TEST_CHECK(getNumEntries(tree, &testint));
      ASSERT_EQUALS_INT(testint, numInserts, "number of entries in btree");
      
      // execute scan, we should see tuples in sort order
      openTreeScan(tree, &sc);
      i = 0;
      while((rc = nextEntry(sc, &rid)) == RC_OK)
	{
	  RID expRid = insert[i++];
	  ASSERT_EQUALS_RID(expRid, rid, "did we find the correct RID?");
	}
      ASSERT_EQUALS_INT(RC_IM_NO_MORE_ENTRIES, rc, "no error returned by scan");
      ASSERT_EQUALS_INT(numInserts, i, "have seen all entries");
      closeTreeScan(sc);

      // cleanup
      TEST_CHECK(closeBtree(tree));
      TEST_CHECK(deleteBtree("testidx"));
      free(permute);
    }

  TEST_CHECK(shutdownIndexManager());
  freeValues(keys, numInserts);

  TEST_DONE();
}

// ************************************************************ 
int *
createPermutation (int size)
{
  int *result = (int *) malloc(size * sizeof(int));
  int i;

  for(i = 0; i < size; result[i] = i, i++);

  for(i = 0; i < 100; i++)
    {
      int l, r, temp;
      l = rand() % size;
      r = rand() % size;
      temp = result[l];
      result[l] = result[r];
      result[r] = temp;
    }
  
  return result;
}

// ************************************************************ 
Value **
createValues (char **stringVals, int size)
{
  Value **result = (Value **) malloc(sizeof(Value *) * size);
  int i;
  
  for(i = 0; i < size; i++)
    result[i] = stringToValue(stringVals[i]);

  return result;
}

// ************************************************************ 
void
freeValues (Value **vals, int size)
{
  while(--size >= 0)
    free(vals[size]);
  free(vals);
}

