package com.briup.bigdata.project.grms.step8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class UserAndGoods implements WritableComparable<UserAndGoods>, DBWritable {
    private Text uid;
    private Text gid;
    private IntWritable exp;

    public UserAndGoods() {
        this.uid=new Text();
        this.gid=new Text();
        this.exp=new IntWritable();
    }

    @Override
    public int compareTo(UserAndGoods o) {
        int uc=this.uid.compareTo(o.uid);
        int gc=this.gid.compareTo(o.gid);
        int ec=this.exp.compareTo(o.exp);
        return uc==0?(gc==0?ec:gc):uc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserAndGoods that = (UserAndGoods) o;
        return Objects.equals(uid, that.uid) &&
                Objects.equals(gid, that.gid) &&
                Objects.equals(exp, that.exp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uid, gid, exp);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.uid.write(dataOutput);
        this.gid.write(dataOutput);
        this.exp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid.readFields(dataInput);
        this.gid.readFields(dataInput);
        this.exp.readFields(dataInput);
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        // insert into bd1903.tbl_uge(year,sid,temp)
        // values(?,?,?);
        ps.setString(1,this.uid.toString());
        ps.setString(2,this.gid.toString());
        ps.setInt(3,this.exp.get());
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        // select year,sid,temp from bd1903.tbl_uge;
        this.uid.set(rs.getString(1));
        this.gid.set(rs.getString(2));
        this.exp.set(rs.getInt(3));
    }

    public Text getUid() {
        return uid;
    }

    public void setUid(Text uid) {
        this.uid.set(uid.toString());
    }
    public void setUid(String uid) {
        this.uid.set(uid);
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

    public IntWritable getExp() {
        return exp;
    }

    public void setExp(IntWritable exp) {
        this.exp.set(exp.get());
    }
    public void setExp(int exp) {
        this.exp.set(exp);
    }

    @Override
    public String toString() {
        return this.uid+"\t"+this.gid+"\t"+this.exp;
    }
}
