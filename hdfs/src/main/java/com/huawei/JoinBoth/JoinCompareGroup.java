package com.huawei.JoinBoth;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinCompareGroup extends WritableComparator{
    public JoinCompareGroup() {
        super(JoinBean.class,true);
    }

    /**
     * 按照cid进行分组
     * 不管是分组对比器还是排序比较器，都需要重写compare(WritableComparable类型的方法)
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JoinBean ja=(JoinBean)a;
        JoinBean jb=(JoinBean)b;
        return (ja.getCid()-jb.getCid());
    }
}
