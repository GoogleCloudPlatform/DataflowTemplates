DB2 instance for testing purpose

1. In itself
   A gce instance (e2-medium), named db2-test on which db2 is running
   SSH is necessary to connect to the VM:
- click on the SSH button
- follow instructions to create a key
- connect

2. Once connected to the VM
   The instance runs on a docker container, we must connect to it:
- ```sudo docker ps```
- get the container ID for ibmcom/db2 image
- connect to the container : ```sudo docker exec -it 7d26f7951b30 bash```

3. Tables we need
   Using an existing production s schema FTC replaced by TESTS schema
   Schema is here : gs://db2mssqlbackup/db2/FTMCoreTables.sql

4. DB2 commands
   Some commands have been written within a bash script for simplicity and are stored in /scripts/
- each script must be launched with a db2 user db2inst1 : ```su db2inst1 -s <script_name>.sh```
- each scripts reference a sql file in /home/db2inst1/queries/
- create_table.sh create all the tables then display the tables
- insert_table.sh takes an argument which is the ID you want as an integer input in the tables,
  ie ```su db2inst1 -s insert_table.sh.sh 12```
- lock_table.sh locks all the tables
- select_table.sh quickly display the tables
- create_triggers.sh create the triggers


5. triggers
- In the triggers FTM_TIMESTAMP() has been replaced by CURRENT_TIMESTAMP
- TESTS.T_OB_HIS : insert the old values from OBJ_BASE to H_OBJ_BASE after status changes
- TESTS.T_ERR_INS and TESTS.T_OB_CRE: update the timestamp FTM_TIMESTAMP() originally for an insert
- TESTS.T_ST_DAT : adds old update rows AUDIT_SEQ +1
- TESTS.T_OR_EFF : insert FTM_TIMESTAMP() is TESTS.OBJ_OBJ_REL.EFF_DATE is null