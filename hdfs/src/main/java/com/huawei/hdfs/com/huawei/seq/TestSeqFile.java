package com.huawei.hdfs.com.huawei.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

public class TestSeqFile {
    @Test
    public void write() throws IOException {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fs=FileSystem.get(conf);
        Path p=new Path("/Users/simmucheng/seq/1.seq");
        if(fs.exists(p)){
            fs.delete(p,true);
        }
        SequenceFile.Writer writer=new SequenceFile.Writer(fs,conf,p,IntWritable.class,Text.class);

        for(int i=0;i<100;i++){
            writer.sync();
            writer.append(new IntWritable(i),new Text("tom"+i));
        }
        writer.close();

    }

    @Test
    public void read() throws IOException {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fs=FileSystem.get(conf);
        Path p=new Path("/Users/simmucheng/seq/1.seq");
        SequenceFile.Reader reader=new SequenceFile.Reader(fs,p,conf);

        //reader.seek(130);
        reader.sync(130);
        IntWritable key=new IntWritable();
        Text value=new Text();
        while(reader.next(key,value)){
            System.out.println(reader.getPosition()+" "+key.get()+" "+value.toString());
        }
        reader.close();

    }
}
