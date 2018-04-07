package com.huawei.hdfs.com.huawei.mysql;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MyDBWritable implements DBWritable, Writable {

    private String name;
    private int age;

    private String p_name;
    private int p_age;


    public MyDBWritable(){

    }
    public MyDBWritable(String name, int age) {
        this.name = name;
        this.age = age;
        this.p_name=p_name;
        this.p_age=p_age;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(age);
        out.writeUTF(p_name);
        out.writeInt(p_age);
    }

    public void readFields(DataInput in) throws IOException {
        name=in.readUTF();
        age=in.readInt();
        p_name=in.readUTF();
        p_age=in.readInt();
    }

    //DB写数据的过程
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,p_name);
        statement.setInt(2,p_age);
    }

    //DBdu读数据的过程
    public void readFields(ResultSet resultSet) throws SQLException {
        name=resultSet.getString(1);
        age=resultSet.getInt(2);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getP_name() {
        return p_name;
    }

    public void setP_name(String p_name) {
        this.p_name = p_name;
    }

    public int getP_age() {
        return p_age;
    }

    public void setP_age(int p_age) {
        this.p_age = p_age;
    }
}
