package com.ecer.kafka.connect.oracle.models;

import java.sql.Timestamp;
import java.util.List;

public class Transaction{

    private String xid = null;
    private Long scn = 0L;
    private Timestamp timestamp;
    private String commitOrRollback;
    private Boolean isCompleted = false;    
    //private HashMap<Integer,DMLRow> dmlRowCollection = new HashMap<>();    
    private List<DMLRow> dmlRowCollection ;
    private Boolean containsRollback = false;

    public Transaction(String xid,Long scn,Timestamp timestamp,List<DMLRow> dmlRowCollection,Boolean containsRollback){
        this.xid = xid;
        this.scn = scn;
        this.timestamp = timestamp;
        this.commitOrRollback = null;
        this.isCompleted = false;
        this.dmlRowCollection = dmlRowCollection;
        this.containsRollback = containsRollback;
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

    public Timestamp getTimestamp(){
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp){
        this.timestamp = timestamp;
    }    

    public String getCommitOrRollback(){
        return commitOrRollback;
    }

    public void setCommitOrRollback(String commitOrRollback){
        this.commitOrRollback = commitOrRollback;
    }    

    public Boolean getIsCompleted(){
        return isCompleted;
    }

    public void setIsCompleted(Boolean isCompleted){
        this.isCompleted = isCompleted;
    }

    public List<DMLRow>  getDmlRowCollection(){
        return dmlRowCollection;
    }
 
    public void setDmlRowCollection(List<DMLRow> dmlRowCollection){
        this.dmlRowCollection = dmlRowCollection;
    }

    public Boolean getContainsRollback(){
        return containsRollback;   
    }

    public void setContainsRollback(Boolean containsRollback){
        this.containsRollback = containsRollback;
    }

    @Override   
    public String toString(){
        return "Transaction [xid="+xid+", scn="+scn+", Dml Count="+dmlRowCollection.size()+"]";
    }    

}