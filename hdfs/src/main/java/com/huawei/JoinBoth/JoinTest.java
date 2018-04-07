package com.huawei.JoinBoth;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JoinTest {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job=Job.getInstance(conf);
        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]));
        }
        job.setJarByClass(JoinTest.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(JoinBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        job.setGroupingComparatorClass(JoinCompareGroup.class);
        //job.setSortComparatorClass(JoinComparator.class);
        job.setPartitionerClass(JoinPartitioner.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));



        job.waitForCompletion(true);

    }
}
