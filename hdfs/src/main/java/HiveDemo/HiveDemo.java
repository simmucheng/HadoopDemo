package HiveDemo;


import java.sql.*;

public class HiveDemo {
    public static void main(String[] args) throws Exception {
        String url="jdbc:hive2://192.168.56.101:10000/bigtable/";
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn= DriverManager.getConnection(url,"root","2011550321");
        Statement st=conn.createStatement();
        ResultSet set=st.executeQuery("select * from persion");
        while(set.next()){
            System.out.println(set.getInt(1)+","+set.getInt(2));
        }

        st.close();
        conn.close();

    }
}
