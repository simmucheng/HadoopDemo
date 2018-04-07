package com.huawei.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class HWReducer extends org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count =0;
        for(IntWritable iw:values){
            count+=iw.get();
        }
        context.write(key,new IntWritable(count));
        context.getCounter("r",Util.getInfo(this,"")).increment(1);
    }
}
