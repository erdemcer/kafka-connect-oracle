package com.ecer.kafka.connect.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *  
 * @author Erdem Cer (erdemcer@gmail.com)
 */

public class OracleConnection{    
    
    public Connection connect(OracleSourceConnectorConfig config) throws SQLException{
        return DriverManager.getConnection(
            "jdbc:oracle:thin:@"+config.getDbHostName()+":"+config.getDbPort()+"/"+config.getDbName(),
            config.getDbUser(),
            config.getDbUserPassword());
    }
}