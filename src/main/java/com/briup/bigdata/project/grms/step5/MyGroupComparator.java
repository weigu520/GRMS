package com.briup.bigdata.project.grms.step5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
    public MyGroupComparator(){
        super(TupleKey.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TupleKey tka=(TupleKey)a;
        TupleKey tkb=(TupleKey)b;
        return super.compare(tka.getGid(),tkb.getGid());
    }
}
