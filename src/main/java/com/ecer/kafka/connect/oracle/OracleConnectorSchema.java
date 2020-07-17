package com.ecer.kafka.connect.oracle;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class OracleConnectorSchema{

    public static final String POSITION_FIELD = "scnposition";    
    public static final String COMMITSCN_POSITION_FIELD = "commitscnposition";    
    public static final String ROWID_POSITION_FIELD = "rowid";
    public static final String LOG_MINER_OFFSET_FIELD="logminer";  
    public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = org.apache.kafka.connect.data.Timestamp.builder().optional().build();
    public static final Schema TIMESTAMP_SCHEMA = org.apache.kafka.connect.data.Timestamp.builder().build();
    public static final Schema EMPTY_SCHEMA = SchemaBuilder.struct().optional().build();

    public static final String DML_ROW_SCHEMA_NAME ="DML_ROW";
    public static final String SCN_FIELD ="SCN";
    public static final String COMMIT_SCN_FIELD ="COMMIT_SCN";
    public static final String OWNER_FIELD ="OWNER";
    public static final String SEG_OWNER_FIELD ="SEG_OWNER";
    public static final String TABLE_NAME_FIELD ="TABLE_NAME";
    public static final String TIMESTAMP_FIELD ="TIMESTAMP";
    public static final String COMMIT_TIMESTAMP_FIELD ="COMMIT_TIMESTAMP";
    public static final String SQL_REDO_FIELD ="SQL_REDO";
    public static final String OPERATION_FIELD ="OPERATION";
    public static final String DATA_ROW_FIELD ="data";
    public static final String BEFORE_DATA_ROW_FIELD ="before";
    public static final String DATA_SCALE_FIELD ="DATA_SCALE";
    public static final String DATA_PRECISION_FIELD ="DATA_PRECISION";
    public static final String DATA_LENGTH_FIELD ="DATA_LENGTH";
    public static final String PK_COLUMN_FIELD ="PK_COLUMN";
    public static final String UQ_COLUMN_FIELD ="UQ_COLUMN";
    public static final String NULLABLE_FIELD ="NULLABLE";
    public static final String COLUMN_NAME_FIELD ="COLUMN_NAME";
    public static final String CSF_FIELD ="CSF";
    public static final String NULL_FIELD ="NULL";
    public static final String DATA_TYPE_FIELD ="DATA_TYPE";
    public static final String DOT =".";
    public static final String COMMA = ",";
    public static final String ROW_ID_FIELD = "ROW_ID";
    public static final String SRC_CON_ID_FIELD = "SRC_CON_ID";
    public static final String XID_FIELD = "XID";
    public static final String THREAD_FIELD = "THREAD#";
    public static final String ROLLBACK_FIELD = "ROLLBACK";

    public static final String NUMBER_TYPE = "NUMBER";
    public static final String LONG_TYPE = "LONG";
    public static final String STRING_TYPE = "STRING";
    public static final String TIMESTAMP_TYPE = "TIMESTAMP";
    public static final String DATE_TYPE = "DATE";
    public static final String CHAR_TYPE = "CHAR";
    public static final String FLOAT_TYPE = "FLOAT";
    public static final String TEMPORARY_TABLE = "temporary tables";

    public static final String OPERATION_START = "START";
    public static final String OPERATION_COMMIT = "COMMIT";
    public static final String OPERATION_ROLLBACK = "ROLLBACK";
    public static final String OPERATION_INSERT = "INSERT";
    public static final String OPERATION_UPDATE = "UPDATE";
    public static final String OPERATION_DELETE = "DELETE";
    public static final String OPERATION_DDL = "DDL";

    public static final int ORA_DESUPPORT_CM_VERSION=190000;
    public static final String DDL_TOPIC_POSTFIX = "_GENERIC_DDL";
}
