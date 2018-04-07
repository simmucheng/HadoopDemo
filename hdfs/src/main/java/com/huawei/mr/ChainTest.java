package com.huawei.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ChainTest {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job=Job.getInstance(conf);

        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        job.setJarByClass(ChainTest.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        ChainMapper.addMapper(job,Mapper1.class,LongWritable.class,Text.class,Text.class,IntWritable.class,conf);
        ChainMapper.addMapper(job,Mapper2.class,Text.class,IntWritable.class,Text.class,IntWritable.class,conf);
        ChainReducer.setReducer(job,Reduce.class,Text.class,IntWritable.class,Text.class,IntWritable.class,conf);
        ChainReducer.addMapper(job,ReduceMapper1.class,Text.class,IntWritable.class,Text.class,IntWritable.class,conf);
        job.waitForCompletion(true);

    }
}
