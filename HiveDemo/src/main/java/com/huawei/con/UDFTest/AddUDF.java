package com.huawei.con.UDFTest;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="myadd",
        value="myadd(int a,int b)====>return a+b"+
        "",
        extended = "Example:\n"
                +" myadd(1,2)===>3 \n")
public class AddUDF extends UDF{
    public int evaluate(int a,int b){
        return a+b;
    }
    public int evalue(int a,int b,int c){
        return a+b+c;
    }
}
