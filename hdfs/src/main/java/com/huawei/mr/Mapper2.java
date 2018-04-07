package com.huawei.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 过滤敏感词汇
 */

public class Mapper2 extends Mapper<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if(!key.toString().equals("hello")){
            context.write(key,value);
        }
    }
}
