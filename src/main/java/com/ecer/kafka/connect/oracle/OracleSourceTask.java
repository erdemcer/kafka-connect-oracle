package com.ecer.kafka.connect.oracle;

import java.sql.CallableStatement;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.ecer.kafka.connect.oracle.models.Data;
import com.ecer.kafka.connect.oracle.models.DataSchemaStruct;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;
import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.*;

/**
 *  
 * @author Erdem Cer (erdemcer@gmail.com)
 */

public class OracleSourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(OracleSourceTask.class);
  private String dbName;  
  private Long streamOffset;
  private String topic=null;  
  public OracleSourceConnectorConfig config;
  private OracleSourceConnectorUtils utils;
  private static Connection dbConn;  
  String logMinerOptions=OracleConnectorSQL.LOGMINER_START_OPTIONS;
  String logMinerStartScr=OracleConnectorSQL.START_LOGMINER_CMD;
  CallableStatement logMinerStartStmt=null;
  CallableStatement logMinerStopStmt = null;
  String logMinerSelectSql;
  static PreparedStatement logMinerSelect;
  PreparedStatement currentSCNStmt;
  ResultSet logMinerData;
  ResultSet currentScnResultSet;  
  private boolean closed=false;
  Boolean parseDmlData;
  static int ix=0;
  private DataSchemaStruct dataSchemaStruct;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  public static Connection getThreadConnection(){
    return dbConn;
  }

  public static void closeDbConn() throws SQLException{
    logMinerSelect.cancel();
    dbConn.close();
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
    config=new OracleSourceConnectorConfig(map);    
    topic=config.getTopic();
    dbName=config.getDbNameAlias();
    parseDmlData=config.getParseDmlData();
    String startSCN = config.getStartScn();
    log.info("Oracle Kafka Connector is starting on {}",config.getDbNameAlias());
    try {
      log.info("Connecting to database");
      dbConn = new OracleConnection().connect(config);
      utils = new OracleSourceConnectorUtils(dbConn, config);
      logMinerSelectSql = utils.getLogMinerSelectSql();      

      log.info("Starting LogMiner Session");
      logMinerStartScr=logMinerStartScr+logMinerOptions+") \n; end;";
      logMinerStartStmt=dbConn.prepareCall(logMinerStartScr);
      Map<String,Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(LOG_MINER_OFFSET_FIELD, dbName));
      streamOffset=0L;
      if (offset!=null){
        Object lastRecordedOffset = offset.get(POSITION_FIELD);
        streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
      }
            
      if (!startSCN.equals("")){
        log.info("Resetting offset with specified SCN:{}",startSCN);
        streamOffset=Long.parseLong(startSCN);
        streamOffset-=1;        
      }
      
      if (config.getResetOffset()){
        log.info("Resetting offset with new SCN");
        streamOffset=0L;
      }

      if (streamOffset==0L){
        currentSCNStmt=dbConn.prepareCall(OracleConnectorSQL.CURRENT_DB_SCN_SQL);
        currentScnResultSet=currentSCNStmt.executeQuery();
        while(currentScnResultSet.next()){
          streamOffset=currentScnResultSet.getLong("CURRENT_SCN");
        }
        currentScnResultSet.close();
        currentSCNStmt.close();
        log.info("Getting current scn from database {}",streamOffset);
      }
      streamOffset+=1;
      log.info(String.format("Log Miner will start at new position SCN : %s with fetch size : %s", streamOffset,config.getDbFetchSize()));
      logMinerStartStmt.setLong(1, streamOffset);
      logMinerStartStmt.execute();      
      logMinerSelect=dbConn.prepareCall(logMinerSelectSql);
      logMinerSelect.setFetchSize(config.getDbFetchSize());
      logMinerData=logMinerSelect.executeQuery();            
      log.info("Logminer started successfully");
    }catch(SQLException e){
      throw new ConnectException("Error at database tier, Please check : "+e.toString());
    }
  }    

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    //TODO: Create SourceRecord objects that will be sent the kafka cluster.       
    
    try {
      ArrayList<SourceRecord> records = new ArrayList<>();                
      while(!this.closed && logMinerData.next()){
        ix++;         
        String segOwner = logMinerData.getString(SEG_OWNER_FIELD); 
        String segName=logMinerData.getString(TABLE_NAME_FIELD);
        streamOffset=logMinerData.getLong(SCN_FIELD);          
        String sqlRedo=logMinerData.getString(SQL_REDO_FIELD);
        if (sqlRedo.contains(TEMPORARY_TABLE)) continue;
        boolean contSF = logMinerData.getBoolean(CSF_FIELD);          
        while(contSF){
          logMinerData.next();
          sqlRedo +=  logMinerData.getString(SQL_REDO_FIELD);
          contSF = logMinerData.getBoolean(CSF_FIELD);
        }
        Long scn=logMinerData.getLong(SCN_FIELD);          
        Timestamp timeStamp=logMinerData.getTimestamp(TIMESTAMP_FIELD);
        String operation = logMinerData.getString(OPERATION_FIELD);
        Data row = new Data(scn, segOwner, segName, sqlRedo,timeStamp,operation);          
        topic = config.getTopic().equals("") ? (config.getDbNameAlias()+DOT+row.getSegOwner()+DOT+row.getSegName()).toUpperCase() : topic;        
        if (ix % 100 == 0) log.info(String.format("Fetched %s rows from database %s ",ix,config.getDbNameAlias())+" "+row.getTimeStamp());

        dataSchemaStruct = utils.createDataSchema(segOwner, segName, sqlRedo,operation);
        records.add(new SourceRecord(sourcePartition(), sourceOffset(streamOffset), topic,  dataSchemaStruct.getDmlRowSchema(), setValueV2(row,dataSchemaStruct)));                          
        return records;
      }
      
      log.info("Logminer stoppped successfully");       
      
    } catch (SQLException e){
      log.error("Error at poll "+e);
    }catch(JSQLParserException e){
      log.error("SQL Parser exception "+e);
    }
    catch(Exception e){
      log.error("Error at poll : "+e+" topic:"+topic);
    }
    return null;
    
  }

  @Override
  public void stop() {
    log.info("Stop called for logminer");
    this.closed=true;
    try {            
      log.info("Logminer session cancel");
      logMinerSelect.cancel();
      if (dbConn!=null){
        log.info("Closing database connection.Last SCN : {}",streamOffset);        
        logMinerSelect.close();
        logMinerStartStmt.close();        
        dbConn.close();
      }
    } catch (SQLException e) {}

  }

  private Struct setValueV2(Data row,DataSchemaStruct dataSchemaStruct) {    
    Struct valueStruct = new Struct(dataSchemaStruct.getDmlRowSchema())
              .put(SCN_FIELD, row.getScn())
              .put(SEG_OWNER_FIELD, row.getSegOwner())
              .put(TABLE_NAME_FIELD, row.getSegName())
              .put(TIMESTAMP_FIELD, row.getTimeStamp())
              .put(SQL_REDO_FIELD, row.getSqlRedo())
              .put(OPERATION_FIELD, row.getOperation())
              .put(DATA_ROW_FIELD, dataSchemaStruct.getDataStruct())
              .put(BEFORE_DATA_ROW_FIELD, dataSchemaStruct.getBeforeDataStruct());
    return valueStruct;
    
  }  

  private Map<String,String> sourcePartition(){
    return Collections.singletonMap(LOG_MINER_OFFSET_FIELD, dbName);
  }

  private Map<String,Long> sourceOffset(Long scnPosition){
    return Collections.singletonMap(POSITION_FIELD, scnPosition);
  }

}