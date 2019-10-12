package com.briup.bigdata.project.grms.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<TupleKey, Text> {
    @Override
    public int getPartition(TupleKey tupleKey, Text text, int i) {
        return tupleKey.getGid().hashCode()%i;
    }
}
