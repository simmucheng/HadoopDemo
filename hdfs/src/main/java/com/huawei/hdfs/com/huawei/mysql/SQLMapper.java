package com.huawei.hdfs.com.huawei.mysql;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SQLMapper extends Mapper<LongWritable,MyDBWritable,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, MyDBWritable value, Context context) throws IOException, InterruptedException {

        context.write(new Text(value.getName()),new IntWritable(value.getAge()));
    }
}
