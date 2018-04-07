package com.huawei.hdfs.com.huawei.mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SQL_Test {

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job=Job.getInstance(conf);

        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(new Path(args[0]))){
            fs.delete(new Path(args[0]),true);
        }
        job.setJobName("SQL_TEST");
        job.setJarByClass(SQL_Test.class);
        job.setMapperClass(SQLMapper.class);
        job.setReducerClass(SQLReducer.class);

        //配置数据库信息
        String driveclass="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/bigtable";
        String username="root";
        String password="root";
        DBConfiguration.configureDB(job.getConfiguration(),driveclass,url,username,password);

        //设置数据库输入
        //需要通过总的记录数来计算切片
        DBInputFormat.setInput(job,MyDBWritable.class,"select name,age from persion","select count(*) from persion");

       //设置数据库输出
        DBOutputFormat.setOutput(job,"state","p_name","p_age");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
