package com.huawei.hdfs.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class TestCompress {
    @Test
    public void deflateCompress() throws Exception {
        Class codeclass=DeflateCodec.class;
        //实例化对象
        CompressionCodec codec= (CompressionCodec) ReflectionUtils.newInstance(codeclass,new Configuration());
        //创建文件输出流
        FileOutputStream fos=new FileOutputStream("/Users/simmucheng/tmp/words.gz");
        //得到压缩流
        CompressionOutputStream zipout=codec.createOutputStream(fos);

        IOUtils.copyBytes(new FileInputStream("/Users/simmucheng/tmp/words"),zipout,1024);
        zipout.finish();

    }
    public static void main(String[] args) throws Exception {
        Class[] zipClasses={
                DeflateCodec.class,
                GzipCodec.class,
                BZip2Codec.class
        };
        for(Class c:zipClasses){
            ManyCompress(c,args[0],args[1]);
        }
        for(Class c:zipClasses){
            ManyDecompress(c,args[0],args[1]);
        }
    }
    public static void ManyCompress(Class compressmethos,String strs0,String strs1) throws Exception {
        CompressionCodec codec=(CompressionCodec) ReflectionUtils.newInstance(compressmethos,new Configuration());
        FileOutputStream fos=new FileOutputStream(strs1+codec.getDefaultExtension());
        CompressionOutputStream zipout=codec.createOutputStream(fos);
        IOUtils.copyBytes(new FileInputStream(strs0),zipout,1024);
        zipout.close();
        fos.close();

    }
    public static void ManyDecompress(Class compressmethos, String arg, String s) throws Exception {
        //实例化对象
        CompressionCodec codec=(CompressionCodec) ReflectionUtils.newInstance(compressmethos,new Configuration());

        FileInputStream fis=new FileInputStream(arg);
        CompressionInputStream zipIn=codec.createInputStream(fis);
        IOUtils.copyBytes(zipIn,new FileOutputStream(s+".txt"),1024);
        zipIn.close();
        fis.close();

    }
}
