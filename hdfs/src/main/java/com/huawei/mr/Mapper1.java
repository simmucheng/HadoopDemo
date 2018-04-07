package com.huawei.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计单词
 */

public class Mapper1 extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyout=new Text();
        IntWritable keyvalue=new IntWritable();
        String str=value.toString();
        String[] strs=str.split(" ");
        for(String t:strs){
            keyout.set(t);
            keyvalue.set(1);
            context.write(keyout,keyvalue);
        }
    }
}
