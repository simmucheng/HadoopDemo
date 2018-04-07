package com.huawei.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.File;
import java.io.IOException;

public class HWAPP {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");//设置系统文件存储为本地文件格式
        Job job =Job.getInstance(conf);

        job.setJobName("HWAPP");                        //设置job名称
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }

        job.setInputFormatClass(TextInputFormat.class);//设置输入格式
        FileInputFormat.addInputPath(job,new Path(args[0])); //设置输入路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));//设置输出路径

        job.setJarByClass(HWAPP.class);                //设置执行的class文件
        job.setMapperClass(HWMapper.class);
        job.setReducerClass(HWReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);                       //设置reduce的个数
        job.setOutputKeyClass(Text.class);              //设置输出的key格式
        job.setOutputValueClass(IntWritable.class);     //设置输出的value格式
        job.waitForCompletion(true);//提交作业，等待它完成
    }
}
