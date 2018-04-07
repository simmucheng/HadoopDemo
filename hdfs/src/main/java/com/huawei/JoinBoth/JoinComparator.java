package com.huawei.JoinBoth;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinComparator extends WritableComparator{
    public JoinComparator() {
        super(JoinBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JoinBean ja=(JoinBean)a;
        JoinBean jb=(JoinBean)b;
        return ja.compareTo(jb);
    }
}
