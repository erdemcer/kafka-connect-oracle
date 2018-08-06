package com.ecer.kafka.connect.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleSourceConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(OracleSourceConnector.class);
  private OracleSourceConnectorConfig config;      
  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new OracleSourceConnectorConfig(map);    
    
    
    String dbName = config.getDbName();    
    if (dbName.equals("")){
      throw new ConnectException("Missing Db Name property");
    }
    String tableWhiteList = config.getTableWhiteList();
    if ((tableWhiteList == null)){
      throw new ConnectException("Could not find schema or table entry for connector to capture");
    }    
    //TODO: Add things you need to do to setup your connector.
  }

  @Override
  public Class<? extends Task> taskClass() {
    //TODO: Return your task implementation.
    return OracleSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    //TODO: Define the individual task configurations that will be executed.
    ArrayList<Map<String,String>> configs = new ArrayList<>(1);
    configs.add(config.originalsStrings());
    return configs;
    //throw new UnsupportedOperationException("This has not been implemented.");
  }

  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.    
    if (OracleSourceTask.getThreadConnection()!=null){
      try {OracleSourceTask.closeDbConn();} catch (Exception e) {} 
    }
  }

  @Override
  public ConfigDef config() {
    return OracleSourceConnectorConfig.conf();
  }
}