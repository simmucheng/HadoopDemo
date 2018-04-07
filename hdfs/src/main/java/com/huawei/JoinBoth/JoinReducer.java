package com.huawei.JoinBoth;

import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<JoinBean,NullWritable,Text,NullWritable>{
    @Override
    protected void reduce(JoinBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        JoinBean joinBean=new JoinBean();
        Iterator<NullWritable> it=values.iterator();
        it.next();
        int type=key.getType();
        int cid=key.getCid();
        String custominfo=key.getCustomerInfo();
        while(it.hasNext()){
            it.next();
            String proinfo=key.getProductorInfo();
            context.write(new Text(custominfo+","+proinfo),NullWritable.get());
        }
    }
}
