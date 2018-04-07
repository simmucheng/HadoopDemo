package com.huawei.join;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 顾客表为小表，所以
 * 将顾客表在mapper的setup阶段将小表加载到内存中，然后直接在mapper端进行连接，而不需要
 * 多余的reducer
 */

public class JoinApp {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]));
        }
        Job job =Job.getInstance(conf);

        job.setJarByClass(JoinApp.class);
        job.setMapperClass(JoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setJobName("JoinTest");
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);

    }
}
