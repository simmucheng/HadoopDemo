
import org.junit.Test;


import java.sql.*;

public class JdbcTest {
    @Test
    public void testPreparedStatement() throws Exception {
        long start=System.currentTimeMillis();
        String driveclass="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/bigtable";
        String username="root";
        String password="root";
        Class.forName(driveclass);
        java.sql.Connection conn= DriverManager.getConnection(url,username,password);

        conn.setAutoCommit(false);

        //创建语句
        String sql="insert into persion(name,age) values(?,?)";
        PreparedStatement ppst= (PreparedStatement) conn.prepareStatement(sql);
        for(int i=0;i<10000;i++){
            ppst.setString(1,"tom"+i);
            ppst.setInt(2,i%100);
            ppst.addBatch();
            //构建一个缓冲区，批处理
            //批量保存，统一提交
            if(i%2000==0){
                //统一执行批量提交
                ppst.executeBatch();
            }
        }
        ppst.executeBatch();
        conn.commit();
        ppst.close();
        conn.close();
        System.out.println(System.currentTimeMillis()-start);

    }
    @Test
    public void testStatement() throws Exception {
        long start=System.currentTimeMillis();
        String driverClass="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/bigtable";
        String username="root";
        String password="root";
        //取得连接
        Class.forName(driverClass);
        java.sql.Connection conn= DriverManager.getConnection(url,username,password);

        conn.setAutoCommit(false);
        //进行表的操作
        Statement st=conn.createStatement();
        for(int i=0;i<10000;i++){
            String sql="insert into persion(name,age) values(tom"+i+","+i+")";
            st.execute(sql);
        }
        conn.commit();
        st.close();
        conn.close();
        System.out.println(System.currentTimeMillis()-start);

    }

    /**
     * 该存储过程需要在服务器端设置sp_add存储过程，才能调用
     *
     * @throws Exception
     */
    @Test
    public void testSingalCallableStatement() throws Exception {
        String driverclass="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/bigtable";
        String username="root";
        String password="root";
        Class.forName(driverclass);
        Connection conn=DriverManager.getConnection(url,username,password);

        conn.setAutoCommit(false);

        CallableStatement cst=conn.prepareCall("{call sp_add(?,?,?)}");
        //绑定输入参数
        cst.setInt(1,2);
        cst.setInt(2,3);
        //绑定输出参数
        cst.registerOutParameter(3,Types.INTEGER);
        cst.execute();
        int sum=cst.getInt(3);
        System.out.println(sum);
        conn.commit();
        conn.close();
    }

    /**
     * 存储过程
     * @throws Exception
     */
    @Test
    public void testCallableStatement() throws Exception {
        long start=System.currentTimeMillis();
        String driverclass="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/bigtable";
        String username="root";
        String password="root";
        Class.forName(driverclass);
        Connection conn=DriverManager.getConnection(url,username,password);
        conn.setAutoCommit(false);
        CallableStatement cst=conn.prepareCall("{call sp_batchinsert(?)}");

        cst.setInt(1,1000000);
        cst.execute();
        conn.commit();
        conn.close();
        System.out.println(System.currentTimeMillis()-start);
    }

    /**
     * 通过callableStatement调用mysql的函数
     * @throws Exception
     */
    @Test
    public void testFunction() throws Exception {
        String classdrive="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/bigtable";
        String username="root";
        String password="root";
        Class.forName(classdrive);
        Connection conn=DriverManager.getConnection(url,username,password);
        conn.setAutoCommit(false);

        CallableStatement cst=conn.prepareCall("{?  = call pa_add(?,?)}");
        //设置输入参数，因为有输出参数，输出参数的位置为1
        cst.setInt(2,100);
        cst.setInt(3,200);
        cst.registerOutParameter(1,Types.INTEGER);

        cst.execute();

        System.out.println(cst.getInt(1));

        conn.commit();
        conn.close();

    }

    /**
     * java
     *编程实现程序b对程序a实行脏读
     * b设置了隔离级别为读未提交
     * @throws Exception
     */
    @Test
    public void TestA() throws Exception {
        String driverclass="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/bigtable";
        String username="root";
        String password="root";
        Class.forName(driverclass);
        Connection conn= DriverManager.getConnection(url,username,password);
        conn.setAutoCommit(false);
        Statement st=conn.createStatement();
        st.execute("update persion set age=100 where id = 1");

        conn.commit();
        st.close();
        conn.close();
    }
    @Test
    public void TestB() throws Exception {
        String driverclass="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/bigtable";
        String username="root";
        String password="root";
        Class.forName(driverclass);
        Connection conn= DriverManager.getConnection(url,username,password);
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(1);
        Statement st=conn.createStatement();
        ResultSet set=st.executeQuery("select age from persion where id = 1");
        set.next();
        int aa=set.getInt(1);
        System.out.println(aa);
        conn.commit();
        st.close();
        conn.close();
    }
}
