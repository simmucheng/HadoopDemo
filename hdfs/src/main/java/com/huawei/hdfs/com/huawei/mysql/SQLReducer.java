package com.huawei.hdfs.com.huawei.mysql;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SQLReducer extends Reducer<Text,IntWritable,MyDBWritable,NullWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        MyDBWritable myDBWritable=new MyDBWritable();
        for(IntWritable it:values){
            myDBWritable.setP_name(key.toString());
            myDBWritable.setP_age(it.get()+100);
            context.write(myDBWritable,NullWritable.get());
        }

    }
}
