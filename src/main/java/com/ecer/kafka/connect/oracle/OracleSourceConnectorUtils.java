package com.ecer.kafka.connect.oracle;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.BEFORE_DATA_ROW_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.COLUMN_NAME_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.DATA_LENGTH_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.DATA_PRECISION_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.DATA_ROW_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.DATA_SCALE_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.DATA_TYPE_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.DATE_TYPE;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.DOT;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.EMPTY_SCHEMA;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.NULLABLE_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.NULL_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.NUMBER_TYPE;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.OPERATION_DELETE;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.OPERATION_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.OPERATION_INSERT;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.OPERATION_UPDATE;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.OPTIONAL_TIMESTAMP_SCHEMA;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.PK_COLUMN_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.SCN_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.SEG_OWNER_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.SQL_REDO_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.TABLE_NAME_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.TIMESTAMP_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.TIMESTAMP_SCHEMA;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.TIMESTAMP_TYPE;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.UQ_COLUMN_FIELD;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.OPERATION_DDL;

import java.net.ConnectException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ecer.kafka.connect.oracle.models.DataSchemaStruct;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

/**
 * contains common utils for connector
 *  
 * @author Erdem Cer (erdemcer@gmail.com)
 */

public class OracleSourceConnectorUtils{
    static final Logger log = LoggerFactory.getLogger(OracleSourceConnectorUtils.class);
    private String logMinerSelectWhereStmt;
    private String tableWhiteList;
    private String tableBlackList;
    private String logMinerSelectSql = OracleConnectorSQL.LOGMINER_SELECT_WITHSCHEMA;
    private String logMinerSelectSqlDeSupportCM = OracleConnectorSQL.LOGMINER_SELECT_WITHSCHEMA_DESUPPORT_CM;
    private final Map<String,String> tableColType = new HashMap<>();   
    private final Map<String,Schema> tableSchema = new HashMap<>();
    private final Map<String,Schema> tableRecordSchema = new HashMap<>();
    private final Map<String,com.ecer.kafka.connect.oracle.models.Column> tabColsMap = new HashMap<>();    

    OracleSourceConnectorConfig config;
    Connection dbConn;
    CallableStatement mineTables;
    CallableStatement mineTableCols;
    ResultSet mineTableColsResultSet;
    ResultSet mineTablesResultSet;

    public OracleSourceConnectorUtils(Connection Conn,OracleSourceConnectorConfig Config)throws SQLException {    	
        this.dbConn=Conn;
        this.config=Config;
        parseTableWhiteList();
    }

    protected String getLogMinerSelectSql(){
        return this.logMinerSelectSql;
    }

    protected String getLogMinerSelectSqlDeSupportCM(){
      return this.logMinerSelectSqlDeSupportCM;
    }

    protected Map<String,String> getTableColType(){
        return this.tableColType;
    }

    protected Struct getRowDataStruct(String tableName){
        return new Struct(tableSchema.get(tableName));
    }

    protected Schema getTableRecordSchema(String tableName){
        return tableRecordSchema.get(tableName);
    }

    protected int getDbVersion() throws SQLException{
      int dbVersion = 0;
      PreparedStatement dbVersionPs = dbConn.prepareCall(OracleConnectorSQL.DB_VERSION);
      ResultSet dbVersionRs = dbVersionPs.executeQuery();
      while (dbVersionRs.next()){
        dbVersion = dbVersionRs.getInt("VERSION");
      }
      dbVersionRs.close();
      dbVersionPs.close();
      return dbVersion;
    }

    protected void parseTableWhiteList(){
        tableWhiteList=config.getTableWhiteList();
        tableBlackList=config.getTableBlackList();
        logMinerSelectWhereStmt="(";
        List<String> tabWithSchemas = Arrays.asList(tableWhiteList.split(","));
        for (String tables:tabWithSchemas){
          List<String> tabs = Arrays.asList(tables.split("\\."));
          logMinerSelectWhereStmt+="("+SEG_OWNER_FIELD+"='"+tabs.get(0)+ "'" + (tabs.get(1).equals("*") ? "":" and "+TABLE_NAME_FIELD+"='"+tabs.get(1)+ "'")+") or ";
        }        
        logMinerSelectWhereStmt=logMinerSelectWhereStmt.substring(0,logMinerSelectWhereStmt.length()-4)+")";

        if (!tableBlackList.equals("")){          
          logMinerSelectWhereStmt+=" and not (";
          tabWithSchemas = Arrays.asList(tableBlackList.split(","));          
          for (String tables:tabWithSchemas){            
            List<String> tabs = Arrays.asList(tables.split("\\."));
            logMinerSelectWhereStmt+="("+SEG_OWNER_FIELD+"='"+tabs.get(0)+ "'" + (tabs.get(1).equals("*") ? "":" and "+TABLE_NAME_FIELD+"='"+tabs.get(1)+ "'")+") or ";
          }
          logMinerSelectWhereStmt=logMinerSelectWhereStmt.substring(0,logMinerSelectWhereStmt.length()-4)+")";
        }
        logMinerSelectSql+=logMinerSelectWhereStmt;
        logMinerSelectSqlDeSupportCM+=logMinerSelectWhereStmt+"))";
    }

    protected void loadTable(String owner,String tableName,String operation) throws SQLException{
      log.info("Getting dictionary details for table : {}",tableName);
      //SchemaBuilder dataSchemaBuiler = SchemaBuilder.struct().name((config.getDbNameAlias()+DOT+owner+DOT+tableName+DOT+"Value").toLowerCase());
      SchemaBuilder dataSchemaBuiler = SchemaBuilder.struct().name("value");
      String mineTableColsSql=OracleConnectorSQL.TABLE_WITH_COLS;
      if (config.getMultitenant()){
        mineTableColsSql=OracleConnectorSQL.TABLE_WITH_COLS_CDB;
      }
      mineTableColsSql=mineTableColsSql.replace("$TABLE_OWNER$", owner).replace("$TABLE_NAME$", tableName);
      
      /*if (config.getMultitenant()) {
    	  mineTableCols=dbConn.prepareCall(sql.getContainerDictionarySQL());
      } else {
          mineTableCols=dbConn.prepareCall(sql.getDictionarySQL());
      }
      mineTableCols.setString(ConnectorSQL.PARAMETER_OWNER, owner);
      mineTableCols.setString(ConnectorSQL.PARAMETER_TABLE_NAME, tableName);*/
      mineTableCols = dbConn.prepareCall(mineTableColsSql);
      mineTableColsResultSet=mineTableCols.executeQuery();
      if (!mineTableColsResultSet.isBeforeFirst()) {
    	  // TODO: consider throwing up here, or an NPE will be thrown in OracleSourceTask.poll()
          log.warn("mineTableCols has no results for {}.{}", owner, tableName);
      }
      while(mineTableColsResultSet.next()){
        String columnName = mineTableColsResultSet.getString(COLUMN_NAME_FIELD);
        Boolean nullable = mineTableColsResultSet.getString(NULLABLE_FIELD).equals("Y") ? true:false;
        String dataType = mineTableColsResultSet.getString(DATA_TYPE_FIELD);
        if (dataType.contains(TIMESTAMP_TYPE)) dataType=TIMESTAMP_TYPE;
        int dataLength = mineTableColsResultSet.getInt(DATA_LENGTH_FIELD);
        int dataScale = mineTableColsResultSet.getInt(DATA_SCALE_FIELD);
        int dataPrecision = mineTableColsResultSet.getInt(DATA_PRECISION_FIELD);
        Boolean pkColumn = mineTableColsResultSet.getInt(PK_COLUMN_FIELD)==1 ? true:false;
        Boolean uqColumn = mineTableColsResultSet.getInt(UQ_COLUMN_FIELD)==1 ? true:false;
        Schema columnSchema = null;       
                 
        switch (dataType){
          case NUMBER_TYPE:
          {                        
            if (dataScale>0 || dataPrecision == 0){              
              columnSchema = nullable ? Schema.OPTIONAL_FLOAT64_SCHEMA  : Schema.FLOAT64_SCHEMA;                            
            }else{
              switch (dataPrecision){
                case 1:
                case 2:              
                  columnSchema = nullable ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
                  break;                
                case 3:
                case 4:
                  columnSchema = nullable ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
                  break;
                case 5:
                case 6:
                case 7:
                case 8:
                case 9:
                  columnSchema = nullable ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
                  break;
                default:
                  columnSchema = nullable ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
                  break;
              }
            }
            break;
          }
          case "CHAR":
          case "VARCHAR":
          case "VARCHAR2":
          case "NCHAR":
          case "NVARCHAR":
          case "NVARCHAR2":          
          case "LONG":
          case "CLOB":
          {            
            columnSchema = nullable ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;            
            break;
          }
          case DATE_TYPE:
          case TIMESTAMP_TYPE:
          {            
            columnSchema = nullable ? OPTIONAL_TIMESTAMP_SCHEMA : TIMESTAMP_SCHEMA;           
            break;
          }          
          default:                        
            columnSchema = nullable ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;            
            break;
        }
        dataSchemaBuiler.field(columnName,columnSchema);
        com.ecer.kafka.connect.oracle.models.Column column = new com.ecer.kafka.connect.oracle.models.Column(owner, tableName, columnName, nullable, dataType, dataLength, dataScale, pkColumn, uqColumn,columnSchema);
        String keyTabCols = owner+DOT+tableName+DOT+columnName;
        tabColsMap.put(keyTabCols, column); 
        log.debug("tabColsMap entry added: {} = {}", keyTabCols, column.toString());
      }
      Schema tSchema = dataSchemaBuiler.optional().build();
      tableSchema.put(owner+DOT+tableName, tSchema);
      mineTableColsResultSet.close();
      mineTableCols.close();      
    }

    

    protected Map<String,LinkedHashMap<String,String>> parseSql(String owner,String tableName,String sqlRedo) throws JSQLParserException , SQLException{
    	/**
    	 * Issue #71 updates.
    	 * 
    	 * JSQLParser defaults to returning the escaped value of strings. This causing
    	 * the mapping here to return text already escaped. If a consumer that is intending
    	 * on replicating the data from Oracle source to Oracle destination attempts to
    	 * use or inspect data from the mapped values it will double escape single quotes
    	 * causing data to be different from source and potentially over-running the size
    	 * of the column causing errors.
    	 */    

      String sqlRedo2=sqlRedo.replace("IS NULL", "= NULL");
      Statement stmt = CCJSqlParserUtil.parse(sqlRedo2);
      final LinkedHashMap<String,String> dataMap = new LinkedHashMap<>();    
      final LinkedHashMap<String,String> beforeDataMap = new LinkedHashMap<>();
      final Map<String,LinkedHashMap<String,String>> allDataMap = new HashMap<>();
      
      if (stmt instanceof Insert){
        Insert insert = (Insert) stmt;              
        
        for (Column c : insert.getColumns()){
          dataMap.put(cleanString(c.getColumnName()), null);
        }
        
        ExpressionList eList = (ExpressionList) insert.getItemsList();
        List<Expression> valueList = eList.getExpressions();
        int i =0;
        for (String key : dataMap.keySet()){
          String value = cleanString(valueList.get(i).toString());
          if (config.getMapUnescapedStrings()) {
        	  // Issue #71
        	  if (valueList.get(i) instanceof StringValue) {
        		  // Override with non escaped string
        		  value = cleanString(((StringValue)valueList.get(i)).getNotExcapedValue());
        	  }
          }
          dataMap.put(key, value);          
          i++;
        }  
  
      }else if (stmt instanceof Update){
        Update update = (Update) stmt;        
        for (Column c : update.getColumns()){
          dataMap.put(cleanString(c.getColumnName()), null);
        }
  
        Iterator<Expression> iterator = update.getExpressions().iterator();
        
        for (String key : dataMap.keySet()){
            Object o = iterator.next();

            String value =   cleanString(o.toString());
            if (config.getMapUnescapedStrings()) {
            	// Issue #71
            	if (o instanceof StringValue) {
            		// Override value with non escaped value
            		value = ((StringValue) o).getNotExcapedValue();
            	}
            }            
            dataMap.put(key, value);            
        }
  
        update.getWhere().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(final EqualsTo expr){                    
                String col = cleanString(expr.getLeftExpression().toString());
                String value = cleanString(expr.getRightExpression().toString());
                if (config.getMapUnescapedStrings()) {
                	// Issue #71
                	if (expr.getRightExpression() instanceof StringValue) {
                		value = cleanString(((StringValue)expr.getRightExpression()).getNotExcapedValue());
                	}
                }                
                beforeDataMap.put(col, value);
                
            }
        });
  
      }else if (stmt instanceof Delete){
        Delete delete = (Delete) stmt;
        delete.getWhere().accept(new ExpressionVisitorAdapter(){
          @Override
          public void visit(final EqualsTo expr){
            String col = cleanString(expr.getLeftExpression().toString());
            String value = cleanString(expr.getRightExpression().toString());
            if (config.getMapUnescapedStrings()) {
            	// Issue #71
            	if (expr.getRightExpression() instanceof StringValue) {
            		value = cleanString(((StringValue)expr.getRightExpression()).getNotExcapedValue());
            	}
            }            
            beforeDataMap.put(col, value);
                       
          }          
        });
      }
  
      allDataMap.put(DATA_ROW_FIELD, dataMap);
      allDataMap.put(BEFORE_DATA_ROW_FIELD, beforeDataMap);
  
      return allDataMap;
    }


    protected DataSchemaStruct createDataSchema(String owner,String tableName,String sqlRedo,String operation) throws Exception{

      Schema dataSchema=EMPTY_SCHEMA;
      Struct dataStruct = null;
      Struct beforeDataStruct = null;      
      String tableKey = owner+DOT+tableName;
      String preSchemaName = (config.getDbNameAlias()+DOT+owner+DOT+tableName+DOT+"row").toLowerCase();      
      if (tableName!=null && operation.equals(OPERATION_DDL)&&tableSchema.containsKey(tableKey)){
        tableSchema.remove(tableKey);
        log.info("Dictionary details of Segment {} has been changed , removed from internal dictionary table",tableKey);  
      }       
      if (config.getParseDmlData() && (!operation.equals(OPERATION_DDL))){
        if (!tableSchema.containsKey(tableKey)){
          if (!tableName.matches("^[\\w.-]+$")){
            throw new ConnectException("Invalid table name "+tableName+" for kafka topic.Check table name which must consist only a-z, A-Z, '0-9', ., - and _");
          }
          loadTable(owner, tableName,operation);
        }

        Map<String,LinkedHashMap<String,String>> allDataMap = parseSql(owner, tableName, sqlRedo);

        LinkedHashMap<String,String> dataMap = allDataMap.get(DATA_ROW_FIELD);
        LinkedHashMap<String,String> beforeDataMap = allDataMap.get(BEFORE_DATA_ROW_FIELD);

        dataSchema = tableSchema.get(tableKey);
        dataStruct = new Struct(dataSchema);
        beforeDataStruct = new Struct(dataSchema);
        
        for (String col : beforeDataMap.keySet()){
          String value = beforeDataMap.get(col);
          String keyTabCol=owner+"."+tableName+"."+col;
          beforeDataStruct.put(col, value.equals(NULL_FIELD) ? null:reSetValue(value, tabColsMap.get(keyTabCol).getColumnSchema()));
          if (operation.equals(OPERATION_UPDATE)){
            if (dataMap.containsKey(col)){
              value = dataMap.get(col);               
            }
            dataStruct.put(col, value.equals(NULL_FIELD) ? null:reSetValue(value, tabColsMap.get(keyTabCol).getColumnSchema()));
          }
        }

        if (operation.equals(OPERATION_INSERT)){
          for (String col : dataMap.keySet()){
            String value = dataMap.get(col);
            String keyTabCol=owner+"."+tableName+"."+col;            
            dataStruct.put(col, value.equals(NULL_FIELD) ? null:reSetValue(value, tabColsMap.get(keyTabCol).getColumnSchema()));
          }          
        }     

        if (operation.equals(OPERATION_INSERT)){
          beforeDataStruct=null;
        }

        if (operation.equals(OPERATION_DELETE)){
          dataStruct=null;
        } 
      }
      Schema newSchema = SchemaBuilder.struct()
                  .name(preSchemaName)
                  .field(SCN_FIELD, Schema.INT64_SCHEMA)
                  .field(SEG_OWNER_FIELD, Schema.STRING_SCHEMA)
                  .field(TABLE_NAME_FIELD,Schema.STRING_SCHEMA)
                  .field(TIMESTAMP_FIELD,org.apache.kafka.connect.data.Timestamp.SCHEMA)
                  .field(SQL_REDO_FIELD, Schema.STRING_SCHEMA)
                  .field(OPERATION_FIELD, Schema.STRING_SCHEMA)
                  .field(DATA_ROW_FIELD, dataSchema)
                  .field(BEFORE_DATA_ROW_FIELD,dataSchema)
                  .build();

      return new DataSchemaStruct(newSchema, dataStruct, beforeDataStruct);
      
    }        

    private Object reSetValue(String value,Schema colSchema){
      
      Object o;
      switch(colSchema.toString()){
        case "Schema{INT8}":
          o = Byte.parseByte(value);
          break;
        case "Schema{INT16}":
          o = Short.parseShort(value);
          break;
        case "Schema{INT32}":
          o = Integer.parseInt(value);
          break;
        case "Schema{INT64}":
          o = Long.parseLong(value);
          break;
        case "Schema{FLOAT64}":
          o = Double.parseDouble(value);
          break;
        case "Schema{org.apache.kafka.connect.data.Timestamp:INT64}":
          if (value.contains("+")) value = value.substring(0, value.indexOf("+"));
          o = Timestamp.valueOf(value);
          break;
        case "Schema{STRING}":
        default:
          o = value;
          break;
      }
      return o;
    }    

    private static String cleanString(String str) {                
      if (str.startsWith("TIMESTAMP"))str=str.replace("TIMESTAMP ", "");        
      if (str.startsWith("'") && str.endsWith("'"))str=str.substring(1,str.length()-1);        
      if (str.startsWith("\"") && str.endsWith("\"") && str.length()>1)str=str.substring(1,str.length()-1);
      return str.replace("IS NULL","= NULL").trim();
    }       

}