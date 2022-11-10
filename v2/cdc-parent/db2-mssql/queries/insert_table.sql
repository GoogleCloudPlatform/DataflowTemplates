INSERT INTO TESTDB.TESTS.OBJ_BASE (
                ID,
                APP_ID ,
                CID ,
                UID ,
                TYPE ,
                STATUS ,
                CREATED ,
                STATUS_DATE ,
                MASTER_FLAG ,
                EVENT_ID ,
                EVENT_NAME ,
                OWNER_ID ,
                TIMEOUT ,
                OBJ_CLASS ,
                SUBTYPE ,
                TECH_PRIORITY ,
                SENDER ,
                RECEIVER ,
                AUX_STATUS ,
                AUDIT_SEQ
        )
VALUES
        (UTIL.ID_VALUE,1,'dummyCID','dummyUID','dummyTYPE','dummySTATUS',TIMESTAMP('2007-09-24-15.53.37.2162474',9),TIMESTAMP('2007-09-24-15.53.37.2162474',9),'N',1,'dummyEVENT_NAME',1,TIMESTAMP('2007-09-24-15.53.37.2162474',9),'dummyOBJ_CLASS','dummySUBTYPE',1,'dummySENDER','dummyRECEIVER','dummyAUX_STATUS',1);

INSERT INTO TESTDB.TESTS.ERROR (
                ID,
                ACTIVITY_ID,
                OBJ_ID,
                TYPE,
                SCHEME,
                CODE,
                COMPONENT_TYPE,
                COMPONENT_NAME,
                DESCRIPTION,
                EVENT_ID,
                SEVERITY,
                CREATED
        )
        VALUES
        (UTIL.ID_VALUE, 1,1,'dummyTYPE','dummySCHEME','dummyCODE','dummyCOMPONENT_TYPE','dummyCOMPONENT_NAME','dummyDESCRIPTION',1,'dummySEVERITY',TIMESTAMP('2007-09-24-15.53.37.2162474',9));
INSERT INTO TESTDB.TESTS.OBJ_OBJ_REL (
                OBJ1_ID ,
                OBJ2_ID ,
                TYPE ,
                EFF_DATE ,
                END_DATE
        )
        VALUES
        (UTIL.ID_VALUE,1,'dummyTYPE',TIMESTAMP('2007-09-24-15.53.37.2162474',9),TIMESTAMP('2007-09-24-15.53.37.2162474',9));
INSERT INTO TESTDB.TESTS.TRANSACTION_BASE (
                OBJ_ID,
                BATCH_ID ,
                TRANSMISSION_ID ,
                ISF_FMT_ID ,
                TXN_SEQUENCE ,
                ALT_ID ,
                ISF_DATA ,
                ISF_DATA_S ,
                RAW_DATA ,
                USER ,
                USER_COMMENT ,
                TXN_CLASS ,
                TXN_DATA1 ,
                TXN_DATA2 ,
                AUDIT_SEQ
        )
        VALUES
        (UTIL.ID_VALUE,1,1,1,1,'dummyALT_ID',CLOB('dummy'),'dummyISF_DATA_S',BLOB('dummy'),'dummyUSER','dummyUSER_COMMENT','dummyTXN_CLASS','dummyTXN_DATA1','dummyTXN_DATA2',1);
INSERT INTO TESTDB.TESTS.TRANSMISSION_BASE (
                OBJ_ID ,
                PARTY_ID ,
                CHANNEL_ID ,
                DATA ,
                DATA_S ,
                ISF_MAPPER ,
                FILENAME ,
                FILEADDRESS ,
                CCSID ,
                ENCODING ,
                BAT_COUNT ,
                BAT_SUCCESS_CNTR ,
                BAT_FAILED_CNTR ,
                FRAG_COUNT ,
                FRAG_COUNTER ,
                FRAG_MAPPED_CNTR ,
                FRAG_SUCCESS_CNTR ,
                FRAG_FAILED_CNTR ,
                AUDIT_SEQ
        )
        VALUES
        (UTIL.ID_VALUE,1,1,BLOB('dummy'),'dummyDATA_S','dummyISF_MAPPER','dummyFILENAME','dummyFILEADDRESS',1,1,1,1,1,1,1,1,1,1,1);
INSERT INTO TESTDB.TESTS.TXN_PAYMENT_BASE (
                OBJ_ID ,
                PAYMENT_TYPE ,
                PMC ,
                BANK_CODE ,
                ACCOUNT ,
                DEST_BANK_CODE ,
                DEST_ACCOUNT ,
                AMOUNT ,
                CURRENCY ,
                FX_RATE ,
                DEBIT_CREDIT_FLAG ,
                TXN_TIMESTAMP ,
                BOOK_DATE ,
                VALUE_DATE ,
                SETTLE_DATE ,
                SETTLEMENT_SYSTEM ,
                AUDIT_SEQ
        )
        VALUES
        (UTIL.ID_VALUE,'dummyPAYMENT_TYPE','dummyPMC','dummyBANK_CODE','dummyACCOUNT','dummyDEST_BANK_CODE','dummyDEST_ACCOUNT',1.0,'dummyCURRENCY',1.0,'C',TIMESTAMP('2007-09-24-15.53.37.2162474',9),DATE('1989-03-02'),DATE('1989-03-02'),DATE('1989-03-02'),'dummySETTLEMENT_SYSTEM',1);