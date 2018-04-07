package com.huawei.jdbcdemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class APP {

    public static void main(String[] args) throws Exception {
        String driverclass="com.mysql.jdbc.Driver";
        String url="jdbc:mysql:/localhost:3306/bigtable";
        String username="root";
        String password="root";
        Class.forName(driverclass);
        Connection conn= DriverManager.getConnection(url,username,password);
        Statement st=conn.createStatement();
        st.execute("update users set age=100 where id = 1");

        conn.commit();
        st.close();
        conn.close();
    }

}
