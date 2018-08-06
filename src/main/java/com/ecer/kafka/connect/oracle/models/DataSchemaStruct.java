package com.ecer.kafka.connect.oracle.models;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 *  
 * @author Erdem Cer (erdemcer@gmail.com)
 */

public class DataSchemaStruct{

    private Schema dmlRowSchema;
    private Struct dataStruct;
    private Struct beforeDataStruct;    

    public DataSchemaStruct(Schema dmlRowSchema,Struct dataStruct,Struct beforeDataStruct){
        super();
        this.dmlRowSchema=dmlRowSchema;
        this.dataStruct=dataStruct;
        this.beforeDataStruct=beforeDataStruct;
    }

    public Schema getDmlRowSchema(){
        return dmlRowSchema;
    }

    public Struct getDataStruct(){
        return dataStruct;
    }

    public Struct getBeforeDataStruct(){
        return beforeDataStruct;
    }

    public void setDmlRowSchema(Schema dmlRowSchema){
        this.dmlRowSchema=dmlRowSchema;
    }

    public void setDataStruct(Struct dataStruct){
        this.dataStruct=dataStruct;
    }

    public void setBeforeDataStruct(Struct beforeDataStruct){
        this.beforeDataStruct=beforeDataStruct;
    }    
}