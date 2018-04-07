package com.huawei.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class HDFSDemo {

    /*
    hadoop读文件
     */

    @Test
    public void readfile() throws Exception {
        Configuration conf=new Configuration();

        FileSystem fs=FileSystem.get(conf);
        Path ps=new Path("hdfs://192.168.56.101:9000/test");
        FSDataInputStream fis=fs.open(ps);
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        IOUtils.copyBytes(fis,baos,1024);
        fis.close();
        System.out.println(new String(baos.toByteArray()));

    }
    /*
    hadoop 写文件
     */
    @Test
    public void writefile() throws Exception {

        Configuration conf =new Configuration();
        FileSystem fs= FileSystem.get(conf);
        Path ps=new Path("hdfs://192.168.56.101:9000/test");
        FSDataOutputStream fout=fs.create(new Path("hdfs://192.168.56.101:9000/hadoop/1.txt"));
        fout.write("how are you?".getBytes());
        fout.close();

    }

    @Test
    public void writefileWithConf() throws Exception {

        Configuration conf =new Configuration();
        FileSystem fs= FileSystem.get(conf);
        Path ps=new Path("hdfs://192.168.56.101:9000/test");
        FSDataOutputStream fout=fs.create(new Path("hdfs://192.168.56.101:9000/hadoop/1.txt"),
                                true,1024,(short) 2,2*1024*1024);
        fout.write("how are you?".getBytes());
        fout.close();

    }
    
}
