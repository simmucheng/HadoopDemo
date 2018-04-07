package com.huawei.JoinBoth;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinBean implements WritableComparable {
    private int type;
    private int cid;
    private int pid;
    private String customerInfo="";
    private String productorInfo="";

    /*
    type 0 customer 1 productor
     */
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public String getCustomerInfo() {
        return customerInfo;
    }

    public void setCustomerInfo(String customerInfo) {
        this.customerInfo = customerInfo;
    }

    public String getProductorInfo() {
        return productorInfo;
    }

    public void setProductorInfo(String productorInfo) {
        this.productorInfo = productorInfo;
    }

    public int compareTo(Object o) {
        JoinBean jb=(JoinBean)o;

        if(cid==jb.getCid()){
            if(type!=jb.getType()){
                return type-jb.getType();
            }
            else return -(pid-jb.pid);
        }
        else {
            return (cid-jb.getCid());
        }
    }

    public void write(DataOutput out) throws IOException {

        out.writeInt(type);
        out.writeInt(cid);
        out.writeInt(pid);
        out.writeUTF(customerInfo);
        out.writeUTF(productorInfo);
    }

    public void readFields(DataInput in) throws IOException {

        this.type=in.readInt();
        this.cid=in.readInt();
        this.pid=in.readInt();
        this.customerInfo=in.readUTF();
        this.productorInfo=in.readUTF();
    }
}
