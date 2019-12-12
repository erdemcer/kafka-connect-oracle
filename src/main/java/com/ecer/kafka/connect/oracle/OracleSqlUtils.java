package com.ecer.kafka.connect.oracle;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleSqlUtils {

    static final Logger log = LoggerFactory.getLogger(OracleSqlUtils.class);    

    public OracleSqlUtils(){
        
    }


    public static Boolean getLogFiles(Connection conn,String sql,Long currScn) throws SQLException{        
        int i = 0;
        String option;
        List<String> logFiles=null;
        String pSql = sql.replace(":vcurrscn",currScn.toString());
        PreparedStatement ps = conn.prepareCall(pSql);
        log.info("#################################");
        ResultSet rs = ps.executeQuery();
        while (rs.next()){
            logFiles = Arrays.asList(rs.getString("NAME").split(" "));
        }                        
        if (logFiles != null){
            ListIterator<String> iterator = logFiles.listIterator();
            while (iterator.hasNext()){
                String logFile = iterator.next();
                log.info("Log file will be mined {}",logFile);
                if (i==0){
                    option = "DBMS_LOGMNR.NEW";
                    i++;
                }else {
                    option = "DBMS_LOGMNR.ADDFILE";
                }            
                executeCallableStmt(conn, OracleConnectorSQL.LOGMINER_ADD_LOGFILE.replace(":logfilename",logFile).replace(":option", option));
            }
        }
        log.info("#################################");
        rs.close();
        ps.close();
        /*
        while (rs.next()){
            log.info("Log file will be mined {} with min SCN {}",rs.getString("NAME"),rs.getLong("FIRST_CHANGE#"));
            if (i==0){
                option = "DBMS_LOGMNR.NEW";
                i++;
            }else {
                option = "DBMS_LOGMNR.ADDFILE";
            }            
            executeCallableStmt(conn, OracleConnectorSQL.LOGMINER_ADD_LOGFILE.replace(":logfilename",rs.getString("NAME")).replace(":option", option));
        }
        
        log.info("#################################");
        rs.close();
        ps.close();*/
        return i>0 ? true : false;
    }
    
    public static void executeCallableStmt(Connection conn,String sql) throws SQLException{        
        CallableStatement s = conn.prepareCall(sql);
        s.execute();
        s.close();
    }

    public static Long getCurrentScn(Connection conn) throws SQLException{
        Long currentScn=0L;
        PreparedStatement ps = conn.prepareCall(OracleConnectorSQL.CURRENT_DB_SCN_SQL);
        ResultSet rs = ps.executeQuery();
        while (rs.next()){
            currentScn = rs.getLong("CURRENT_SCN");
        }
        rs.close();
        ps.close();
        return currentScn;
    }    
    
}