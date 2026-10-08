
CREATE TABLE "AllDatatypeColumns" (
  "varchar_column" VARCHAR2(20) NOT NULL, 
  "tinyint_column" NUMBER,              
  "text_column" CLOB,                    
  "date_column" DATE,                    
  "smallint_column" NUMBER,            
  "mediumint_column" NUMBER,          
  "int_column" NUMBER,                      
  "bigint_column" NUMBER,                
  "float_column" FLOAT,            
  "double_column" DOUBLE PRECISION,                
  "decimal_column" NUMBER(10,2),        
  "datetime_column" TIMESTAMP WITH TIME ZONE,            
  "timestamp_column" TIMESTAMP WITH TIME ZONE,          
  "time_column" VARCHAR2(50),                    
  "year_column" NUMBER,                    
  "char_column" CHAR(10),                
  "tinyblob_column" RAW(255),            
  "tinytext_column" VARCHAR2(255),            
  "blob_column" BLOB,                    
  "mediumblob_column" BLOB,        
  "mediumtext_column" CLOB,        
  "longblob_column" BLOB,            
  "longtext_column" CLOB,            
  "enum_column" VARCHAR2(255),       
  "bool_column" NUMBER(1),              
  "other_bool_column" NUMBER(1),        
  "binary_column" RAW(20),            
  "varbinary_column" RAW(20),      
  "bit_column" RAW(8),                   
  PRIMARY KEY ("varchar_column")
);

CREATE TABLE "AllDatatypeColumns2" (
 "varchar_column" VARCHAR2(20) NOT NULL, 
 "tinyint_column" NUMBER,              
 "text_column" CLOB,                    
 "date_column" DATE,                    
 "smallint_column" NUMBER,            
 "mediumint_column" NUMBER,          
 "int_column" NUMBER,                      
 "bigint_column" NUMBER,                
 "float_column" FLOAT,            
 "double_column" DOUBLE PRECISION,                
 "decimal_column" NUMBER(10,2),        
 "datetime_column" TIMESTAMP WITH TIME ZONE,            
 "timestamp_column" TIMESTAMP WITH TIME ZONE,          
 "time_column" VARCHAR2(50),                    
 "year_column" NUMBER,                    
 "char_column" CHAR(10),                
 "tinyblob_column" RAW(255),            
 "tinytext_column" VARCHAR2(255),            
 "blob_column" BLOB,                    
 "mediumblob_column" BLOB,        
 "mediumtext_column" CLOB,        
 "longblob_column" BLOB,            
 "longtext_column" CLOB,            
 "enum_column" VARCHAR2(255),       
 "bool_column" NUMBER(1),              
 "binary_column" RAW(20),            
 "varbinary_column" RAW(20),      
 "bit_column" RAW(8),                   
 PRIMARY KEY ("varchar_column")
);

CREATE TABLE "DatatypeColumnsWithSizes" (
   "varchar_column" VARCHAR2(20) NOT NULL, 
   "float_column" FLOAT,            
   "decimal_column" NUMBER(10,2),        
   "char_column" CHAR(10),                
   "bool_column" NUMBER(1),              
   "binary_column" RAW(20),            
   "varbinary_column" RAW(20),      
   "bit_column" RAW(8),                   
   PRIMARY KEY ("varchar_column")
);

CREATE TABLE "DatatypeColumnsReducedSizes" (
    "varchar_column" VARCHAR2(20) NOT NULL, 
    "float_column" FLOAT,            
    "decimal_column" NUMBER(10,2),        
    "char_column" CHAR(10),                
    "bool_column" NUMBER(1),              
    "binary_column" RAW(20),            
    "varbinary_column" RAW(20),      
    "bit_column" RAW(8),                   
    PRIMARY KEY ("varchar_column")
);

CREATE TABLE "Users" (
    "user_id" NUMBER NOT NULL,
    "first_name" VARCHAR2(50),
    "last_name" VARCHAR2(50),
    "age" NUMBER,
    PRIMARY KEY ("user_id")
);

CREATE TABLE "Authors" (
    "id" NUMBER NOT NULL,
    "name" VARCHAR2(200),
    PRIMARY KEY ("id")
) ;

CREATE TABLE "AllDatatypeTransformation" (
 "varchar_column" VARCHAR2(20) NOT NULL, 
 "tinyint_column" NUMBER,              
 "text_column" CLOB,                    
 "date_column" DATE,                    
 "int_column" NUMBER,                      
 "bigint_column" NUMBER,                
 "float_column" FLOAT,            
 "double_column" DOUBLE PRECISION,                
 "decimal_column" NUMBER(10,2),        
 "datetime_column" TIMESTAMP WITH TIME ZONE,            
 "timestamp_column" TIMESTAMP WITH TIME ZONE,          
 "time_column" VARCHAR2(50),                    
 "year_column" NUMBER,                    
 "blob_column" BLOB,                    
 "enum_column" VARCHAR2(255),       
 "bool_column" NUMBER(1),              
 "binary_column" RAW(20),            
 "bit_column" RAW(8),                   
 PRIMARY KEY ("varchar_column")
);

CREATE TABLE "Singers" (
    "singer_id" NUMBER NOT NULL ,
    "first_name" VARCHAR2(1024),
    "last_name" VARCHAR2(1024),
    PRIMARY KEY ("singer_id")
);

CREATE TABLE "Books" (
    "id" NUMBER NOT NULL ,
    "title" VARCHAR2(200),
    PRIMARY KEY ("id")
) ;

ALTER TABLE "AllDatatypeColumns" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

ALTER TABLE "AllDatatypeColumns2" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

ALTER TABLE "DatatypeColumnsWithSizes" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

ALTER TABLE "DatatypeColumnsReducedSizes" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

ALTER TABLE "Users" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

ALTER TABLE "Authors" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

ALTER TABLE "AllDatatypeTransformation" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

ALTER TABLE "Singers" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

ALTER TABLE "Books" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
