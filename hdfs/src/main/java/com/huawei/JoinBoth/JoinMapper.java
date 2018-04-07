package com.huawei.JoinBoth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable,Text,JoinBean,NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str=value.toString();
        FileSplit split=(FileSplit) context.getInputSplit();
        String path=split.getPath().toString();
        JoinBean joinBean=new JoinBean();
        if(path.contains("customer")){
            joinBean.setType(0);
            String cid=str.substring(0,str.indexOf(","));
            String cusinfo=str;
            joinBean.setCid(Integer.parseInt(cid));
            joinBean.setCustomerInfo(cusinfo);
        }
        else {
            joinBean.setType(1);
            String cid=str.substring(str.lastIndexOf(',')+1);
            String pid=str.substring(0,str.indexOf(','));
            String proinfo=str.substring(0,str.lastIndexOf(','));
            joinBean.setPid(Integer.parseInt(pid));
            joinBean.setProductorInfo(proinfo);
            joinBean.setCid(Integer.parseInt(cid));
        }

        context.write(joinBean,NullWritable.get());

    }
}
