#ifndef TABLES_H
#define TABLES_H

#include "dt.h"
#include "stdint.h"
#include "stdlib.h"

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
extern Value *stringToValue (char *value);
extern char *serializeTableInfo(RM_TableData *rel);
extern char *serializeTableContent(RM_TableData *rel);
extern char *serializeSchema(Schema *schema);
extern char *serializeRecord(Record *record, Schema *schema);
extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
extern char *serializeValue(Value *val);

#endif
