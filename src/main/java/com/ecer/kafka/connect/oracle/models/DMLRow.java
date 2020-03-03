package com.ecer.kafka.connect.oracle.models;

import java.sql.Timestamp;

public class DMLRow{

    private String xid = null;
    private Long scn = 0L;
    private Long commitScn = 0L;
    private Timestamp timestamp = null;
    private Timestamp commitTimestamp = null;
    private String operation = null;
    private String segOwner = null;
    private String segName = null;
    private String rowId = null;
    private String sqlRedo = null;
    private String topic = null;

    public DMLRow(String xid,Long scn,Long commitScn,Timestamp timestamp,String operation,String segOwner,String segName,String rowId,String sqlRedo,String topic,Timestamp commitTimeStamp){
        this.xid = xid;
        this.scn = scn;
        this.commitScn = commitScn;
        this.timestamp = timestamp;
        this.operation = operation;
        this.segOwner = segOwner;
        this.segName = segName;
        this.rowId = rowId;
        this.sqlRedo = sqlRedo;
        this.topic = topic;
        this.commitTimestamp = commitTimeStamp;
    }

    public String getXid(){
        return xid;
    }

    public void setXid(String xid){
        this.xid = xid;
    }

    public Long getScn(){
        return scn;
    }

    public void setScn(Long scn){
        this.scn = scn;
    }

    public Long getCommitScn(){
        return commitScn;
    }

    public void setCommitScn(Long commitScn){
        this.commitScn = commitScn;
    }    

    public Timestamp getTimestamp(){
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp){
        this.timestamp = timestamp;
    }    

    public Timestamp getCommitTimestamp(){
        return commitTimestamp;
    }

    public void setCommitTimestamp(Timestamp commitTimeStamp){
        this.commitTimestamp = commitTimeStamp;
    }        

    public String getOperation(){
        return operation;
    }

    public void setOperation(String operation){
        this.operation = operation;
    }    

    public String getSegOwner(){
        return segOwner;
    }

    public void setSegOwner(String segOwner){
        this.segOwner = segOwner;
    }        

    public String getSegName(){
        return segName;
    }

    public void setSegName(String segName){
        this.segName = segName;
    }         

    public String getRowId(){
        return rowId;
    }

    public void setRowId(String rowId){
        this.rowId = rowId;
    }    

    public String getSqlRedo(){
        return sqlRedo;
    }

    public void setSqlRedo(String sqlRedo){
        this.sqlRedo = sqlRedo;
    }

    public String getTopic(){
        return topic;
    }

    public void setTopic(String topic){
        this.topic = topic;
    }    
    
    @Override
    public String toString(){
		return "LogMinerRow [xid=" + xid + ", scn=" + scn + ", timestamp=" + timestamp + ", operation="
				+ operation + ", segOwner=" + segOwner + ", segName=" + segName + ", rowId=" + rowId
				+ ", sqlRedo=" + sqlRedo +"]";
    }

}

