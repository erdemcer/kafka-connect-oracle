# Kafka Connect Oracle

kafka-connect-oracle is a Kafka source connector for capturing all row based DML changes from Oracle database and streaming these changes to Kafka. Change data capture logic is based on Oracle LogMiner solution.

Only committed changes are pulled from Oracle which are Insert, Update, Delete operations. All streamed messages have related full "sql_redo" statement and parsed fields with values of sql statements. Parsed fields and values are kept in proper field type in schemas.

Messages have old (before change) and new (after change) values of row fields for DML operations. Insert operation has only new values of row tagged as "data". Update operation has new data tagged as "data" and also contains old values of row before change tagged as "before". Delete operation only contains old data tagged as "before".

# News
*   Ability to only capture specified operation like INSERT / UPDATE / DELETE .
*   DDL Capture Support.DDL statements can be captured via this connector.All captured DDL statements are published into <db.name.alias>.<SEGMENT_OWNER>."_GENERIC_DDL" name formatted topic if no topic configuration property is set in property file.If a topic is set for configuration propery , all captured DDL statements are published into this topic with other statements.
*   Partial rollback detection has been implemented
*   With new relases of Oracle database like 19c, CONTINUOUS_MINE option is desupported and Logminer has lost ability mining of redo and archive logs continuously. First release of this connector was based on this property. This Connector now has the ability to capture all changed data(DML changes) without CONTINUOUS_MINE option for new relases of Oracle database. For this change all test have been done on single instances. Working on RAC support
*   Table blacklist configuration property can be used to not capture specified tables or schemas

# Sample Data

**Insert :**


    {    
        "SCN": 768889966828,
        "SEG_OWNER": "TEST",
        "TABLE_NAME": "TEST4",
        "TIMESTAMP": 1537958606000,
        "SQL_REDO": "insert into \"TEST\".\"TEST4\"(\"ID\",\"NAME\",\"PROCESS_DATE\",\"CDC_TIMESTAMP\") values (78238,NULL,NULL,TIMESTAMP ' 2018-09-26 10:43:26.643')",
        "OPERATION": "INSERT",
        "data": {
            "ID": 78238.0,
            "NAME": null,
            "PROCESS_DATE": null,
            "CDC_TIMESTAMP": 1537947806643
        },
        "before": null 
    }

**Update :**

    {
        "SCN": 768889969452,
        "SEG_OWNER": "TEST",
        "TABLE_NAME": "TEST4",
        "TIMESTAMP": 1537964106000,
        "SQL_REDO": "update \"TEST\".\"TEST4\" set \"NAME\" = 'XaQCZKDINhTQBMevBZGGDjfPAsGqTUlCTyLThpmZ' where \"ID\" = 78238 and \"NAME\" IS NULL and \"PROCESS_DATE\" IS NULL and \"CDC_TIMESTAMP\" = TIMESTAMP ' 2018-09-26 10:43:26.643'",
        "OPERATION": "UPDATE",
        "data": {
            "ID": 78238.0,
            "NAME": "XaQCZKDINhTQBMevBZGGDjfPAsGqTUlCTyLThpmZ",
            "PROCESS_DATE": null,
            "CDC_TIMESTAMP": 1537947806643
        },
        "before": {
            "ID": 78238.0,
            "NAME": null,
            "PROCESS_DATE": null,
            "CDC_TIMESTAMP": 1537947806643
        }
    }

**Delete :**

    {
        "SCN": 768889969632,
        "SEG_OWNER": "TEST",
        "TABLE_NAME": "TEST4",
        "TIMESTAMP": 1537964142000,
        "SQL_REDO": "delete from \"TEST\".\"TEST4\" where \"ID\" = 78238 and \"NAME\" = 'XaQCZKDINhTQBMevBZGGDjfPAsGqTUlCTyLThpmZ' and \"PROCESS_DATE\" IS NULL and \"CDC_TIMESTAMP\" = TIMESTAMP ' 2018-09-26 10:43:26.643'",
        "OPERATION": "DELETE",
        "data": null,
        "before": {
            "ID": 78238.0,
            "NAME": "XaQCZKDINhTQBMevBZGGDjfPAsGqTUlCTyLThpmZ",
            "PROCESS_DATE": null,
            "CDC_TIMESTAMP": 1537947806643
        }
    }

# Setting Up

The database must be in archivelog mode and supplemental logging must be enabled.

On database server

    sqlplus / as sysdba    
    SQL>shutdown immediate
    SQL>startup mount
    SQL>alter database archivelog;
    SQL>alter database open;

Enable supplemental logging

    sqlplus / as sysdba    
    SQL>alter database add supplemental log data (all) columns;

In order to execute connector successfully, connector must be started with privileged Oracle user. If given user has DBA role, this step can be skipped. Otherwise, the following scripts need to be executed to create a privileged user:

    create role logmnr_role;
    grant create session to logmnr_role;
    grant  execute_catalog_role,select any transaction ,select any dictionary to logmnr_role;
    create user kminer identified by kminerpass;
    grant  logmnr_role to kminer;
    alter user kminer quota unlimited on users;

In a multitenant configuration, the privileged Oracle user must be a "common user" and some multitenant-specific grants need to be made:

    create role c##logmnr_role;
    grant create session to c##logmnr_role;
    grant  execute_catalog_role,select any transaction ,select any dictionary,logmining to c##logmnr_role;
    create user c##kminer identified by kminerpass;
    grant  c##logmnr_role to c##kminer;
    alter user c##kminer quota unlimited on users set container_data = all container = current;

# Configuration

## Configuration Properties

|Name|Type|Description|
|---|---|---|
|name|String|Connector name|
|connector.class|String|The name of the java class for this connector.|
|db.name.alias|String|Identifier name for database like Test,Dev,Prod or specific name to identify the database.This name will be used as header of topics and schema names.|
|tasks.max|Integer|Maximum number of tasks to create.This connector uses a single task.|
|topic|String|Name of the topic that the messages will be written to.If it is set a value all messages will be written into this declared topic , if it is not set,  for each database table a topic will be created dynamically.|
|db.name|String|Service name  or sid of the database to connect.Mostly database service name is used.|
|db.hostname|String|Ip address or hostname of Oracle database server.|
|db.port|Integer|Port number of Oracle database server.|
|db.user|String |Name of database user which is used to connect to database to start and execute logminer. This           user must provide necessary privileges mentioned above.|
|db.user.password|String|Password of database user.|
|db.fetch.size|Integer|This config property sets Oracle row fetch size value.|
|table.whitelist|String|A comma separated list of database schema or table names which will be captured.<br />For all schema capture **<SCHEMA_NAME>.*** <br /> For table capture **<SCHEMA_NAME>.<TABLE_NAME>** must be specified.|
|parse.dml.data|Boolean|If it is true , captured sql DML statement will be parsed into fields and values.If it is false only sql DML statement is published.
|reset.offset|Boolean|If it is true , offset value will be set to current SCN of database when connector started.If it is false connector will start from last offset value.
|start.scn|Long|If it is set , offset value will be set this specified value and logminer will start at this SCN.If connector would like to be started from desired SCN , this property can be used.
|multitenant|Boolean|If true, multitenant support is enabled.  If false, single instance configuration will be used.
|table.blacklist|String|A comma separated list of database schema or table names which will not be captured.<br />For all schema capture **<SCHEMA_NAME>.*** <br /> For table capture **<SCHEMA_NAME>.<TABLE_NAME>** must be specified.|
|dml.types|String|A comma separated list of DML operations (INSERT,UPDATE,DELETE). If not specified the default behavior of replicating all DML operations occurs,if specified only specified operations are captured.|
|||



## Example Config

    name=oracle-logminer-connector
    connector.class=com.ecer.kafka.connect.oracle.OracleSourceConnector
    db.name.alias=test
    tasks.max=1
    topic=cdctest
    db.name=testdb
    db.hostname=10.1.X.X
    db.port=1521
    db.user=kminer
    db.user.password=kminerpass
    db.fetch.size=1
    table.whitelist=TEST.*,TEST2.TABLE2
    table.blacklist=TEST2.TABLE3
    parse.dml.data=true
    reset.offset=false
    multitenant=false

## Building and Running

    mvn clean package

    Copy kafka-connect-oracle-1.0.jar and lib/ojdbc7.jar to KAFKA_HOME/lib folder. If CONFLUENT platform is using for kafka cluster , copy ojdbc7.jar and  kafka-connect-oracle-1.0.jar to $CONFLUENT_HOME/share/java/kafka-connect-jdbc folder.

    Copy config/OracleSourceConnector.properties file to $KAFKA_HOME/config . For CONFLUENT copy properties file to $CONFLUENT_HOME/etc/kafka-connect-jdbc

    To start connector

    cd $KAFKA_HOME
    ./bin/connect-standalone.sh ./config/connect-standalone.properties ./config/OracleSourceConnector.properties

    In order to start connector with Avro serialization 
    
    cd $CONFLUENT_HOME

    ./bin/connect-standalone ./etc/schema-registry/connect-avro-standalone.properties ./etc/kafka-connect-jdbc/OracleSourceConnector.properties 

    Do not forget to populate connect-standalone.properties  or connect-avro-standalone.properties with the appropriate hostnames and ports.

# Todo:

    1. Implementation for DDL operations
    2. Support for other Oracle specific data types
    3. New implementations
    4. Performance Tuning
    5. Initial data load
    6. Bug fix    