package com.huawei.con;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveDemo {
    public static void main(String[] args) throws Exception {
        String classDriver="org.apache.hive.jdbc.HiveDriver";
        String url="jdbc:hive2://192.168.56.101:10000/mydb2";
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn= DriverManager.getConnection(url,"root","2011550321");
        Statement st=conn.createStatement();
        ResultSet set=st.executeQuery("select * from ");

        st.close();
        conn.close();

    }
}
