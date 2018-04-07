package com.huawei.JoinBoth;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner extends Partitioner<JoinBean,NullWritable>{


    /**
     * 按照用户id来划分
     * @param joinBean
     * @param nullWritable
     * @param numPartitions
     * @return
     */
    public int getPartition(JoinBean joinBean, NullWritable nullWritable, int numPartitions) {
        return (joinBean.getCid()%numPartitions);
    }
}
