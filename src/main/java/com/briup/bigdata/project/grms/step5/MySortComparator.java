package com.briup.bigdata.project.grms.step5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySortComparator extends WritableComparator {
    public MySortComparator() {
        super(TupleKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b);
    }
}
