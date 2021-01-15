package com.ecer.kafka.connect.oracle;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class OracleSourceConnectorConfig extends AbstractConfig {

  public static final String DB_NAME_ALIAS = "db.name.alias";
  public static final String TOPIC_CONFIG = "topic";
  public static final String DB_NAME_CONFIG = "db.name";
  public static final String DB_HOST_NAME_CONFIG = "db.hostname";
  public static final String DB_PORT_CONFIG = "db.port";
  public static final String DB_USER_CONFIG = "db.user";
  public static final String DB_USER_PASSWORD_CONFIG = "db.user.password";
  public static final String TABLE_WHITELIST = "table.whitelist";
  public static final String PARSE_DML_DATA = "parse.dml.data";
  public static final String DB_FETCH_SIZE = "db.fetch.size";
  public static final String RESET_OFFSET = "reset.offset";
  public static final String START_SCN = "start.scn";
  public static final String MULTITENANT = "multitenant";
  public static final String TABLE_BLACKLIST = "table.blacklist";
  public static final String DML_TYPES = "dml.types";
  public static final String MAP_UNESCAPED_STRINGS = "map.unescaped.strings";

  
  public OracleSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public OracleSourceConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(DB_NAME_ALIAS, Type.STRING, Importance.HIGH, "Db Name Alias")
        .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "Topic")
        .define(DB_NAME_CONFIG, Type.STRING, Importance.HIGH, "Db Name")
        .define(DB_HOST_NAME_CONFIG,Type.STRING,Importance.HIGH,"Db HostName")
        .define(DB_PORT_CONFIG,Type.INT,Importance.HIGH,"Db Port")
        .define(DB_USER_CONFIG,Type.STRING,Importance.HIGH,"Db User")
        .define(DB_USER_PASSWORD_CONFIG,Type.STRING,Importance.HIGH,"Db User Password")
        .define(TABLE_WHITELIST,Type.STRING,Importance.HIGH,"TAbles will be mined")
        .define(PARSE_DML_DATA,Type.BOOLEAN,Importance.HIGH,"Parse DML Data")
        .define(DB_FETCH_SIZE,Type.INT,Importance.HIGH,"Database Record Fetch Size")
        .define(RESET_OFFSET,Type.BOOLEAN,Importance.HIGH,"Reset Offset")
        .define(START_SCN,Type.STRING,"",Importance.LOW,"Start SCN")
        .define(MULTITENANT, Type.BOOLEAN, Importance.HIGH, "Database is multitenant (container)")
        .define(TABLE_BLACKLIST, Type.STRING, Importance.LOW, "Table will not be mined")
        .define(DML_TYPES, Type.STRING, "", Importance.LOW, "Types of DML to capture, CSV value of INSERT/UPDATE/DELETE")
        .define(MAP_UNESCAPED_STRINGS, Type.BOOLEAN, false, Importance.LOW, "Mapped values for data/before will have unescaped strings");
  }

  public String getDbNameAlias(){ return this.getString(DB_NAME_ALIAS);}
  public String getTopic(){ return this.getString(TOPIC_CONFIG);}
  public String getDbName(){ return this.getString(DB_NAME_CONFIG);}
  public String getDbHostName(){return this.getString(DB_HOST_NAME_CONFIG);}
  public int getDbPort(){return this.getInt(DB_PORT_CONFIG);}
  public String getDbUser(){return this.getString(DB_USER_CONFIG);}
  public String getDbUserPassword(){return this.getString(DB_USER_PASSWORD_CONFIG);}
  public String getTableWhiteList(){return this.getString(TABLE_WHITELIST);}
  public Boolean getParseDmlData(){return this.getBoolean(PARSE_DML_DATA);}
  public int getDbFetchSize(){return this.getInt(DB_FETCH_SIZE);}
  public Boolean getResetOffset(){return this.getBoolean(RESET_OFFSET);}
  public String getStartScn(){return this.getString(START_SCN);}
  public Boolean getMultitenant() {return this.getBoolean(MULTITENANT);}
  public String getTableBlackList(){return this.getString(TABLE_BLACKLIST);}
  public String getDMLTypes(){return this.getString(DML_TYPES);}
  public Boolean getMapUnescapedStrings(){return this.getBoolean(MAP_UNESCAPED_STRINGS);}
}
