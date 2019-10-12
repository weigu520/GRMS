package com.briup.bigdata.project.grms.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/*
组合键
 */
public class TupleKey implements WritableComparable<TupleKey> {
    private Text gid;
    private Text flag;

    public TupleKey() {
        this.gid=new Text();
        this.flag=new Text();
    }

    @Override
    public int compareTo(TupleKey o) {
        int gidComp=this.gid.compareTo(o.gid);
        int flagComp=this.flag.compareTo(o.flag);
        return gidComp==0?flagComp:gidComp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleKey tupleKey = (TupleKey) o;
        return Objects.equals(gid, tupleKey.gid) &&
                Objects.equals(flag, tupleKey.flag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gid, flag);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.gid.write(dataOutput);
        this.flag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.gid.readFields(dataInput);
        this.flag.readFields(dataInput);
    }

    public Text getGid() {
        return gid;
    }

    public void setGid(Text gid) {
        this.gid.set(gid.toString());
    }
    public void setGid(String gid) {
        this.gid.set(gid);
    }

    public Text getFlag() {
        return flag;
    }

    public void setFlag(Text flag) {
        this.flag.set(flag.toString());
    }
    public void setFlag(String flag) {
        this.flag.set(flag);
    }

    @Override
    public String toString() {
        return this.gid+"\t"+this.flag;
    }
}
