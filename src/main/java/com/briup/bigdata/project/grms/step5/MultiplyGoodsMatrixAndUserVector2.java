package com.briup.bigdata.project.grms.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class MultiplyGoodsMatrixAndUserVector2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector2(),args));
    }

    @Override
    public int run(String[] strings) throws Exception{
        Configuration conf=this.getConf();
        Path in1=new Path("/grms/result3/part-r-00000");
        Path in2=new Path("/grms/result4/part-r-00000");
        Path out=new Path("/grms/newresult5");

        Job job= Job.getInstance(conf,"作业5：商品共现矩阵乘以用户购买向量，形成临时的推荐结果");
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,in1, KeyValueTextInputFormat.class,MultiplyGoodsMatrixAndUserVectorFirstMapper2.class);
        MultipleInputs.addInputPath(job,in2,KeyValueTextInputFormat.class,MultiplyGoodsMatrixAndUserVectorSecondMapper2.class);
        job.setMapOutputKeyClass(TupleKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);

        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupComparator.class);
        job.setSortComparatorClass(MySortComparator.class);

        return job.waitForCompletion(true)?0:1;
    }

    // 处理物品的共现矩阵数据
    static class MultiplyGoodsMatrixAndUserVectorFirstMapper2 extends Mapper<Text,Text,TupleKey,Text> {
        private TupleKey k2=new TupleKey();
        private Text v2=new Text();

        @Override
        protected void map(Text k1,Text v1,Context context) throws IOException, InterruptedException{
            this.k2.setGid(k1.toString());
            this.k2.setFlag("0");
            this.v2.set(v1.toString());
            context.write(this.k2,this.v2);
        }
    }

    // 处理用户的购买向量数据
    static class MultiplyGoodsMatrixAndUserVectorSecondMapper2 extends Mapper<Text,Text,TupleKey,Text>{
        private TupleKey k2=new TupleKey();
        private Text v2=new Text();

        @Override
        protected void map(Text k1,Text v1,Context context) throws IOException, InterruptedException{
            this.k2.setGid(k1.toString());
            this.k2.setFlag("1");
            this.v2.set(v1.toString());
            context.write(this.k2,this.v2);
        }
    }

    static class MultiplyGoodsMatrixAndUserVectorReducer2 extends Reducer<TupleKey,Text,Text, IntWritable> {
        private Text k3=new Text();
        private IntWritable v3=new IntWritable();

        @Override
        protected void reduce(TupleKey k2,Iterable<Text> v2s,Context context) throws IOException, InterruptedException{
            Iterator<Text> it=v2s.iterator();

            String gm=new Text(it.next()).toString();
            String uv=new Text(it.next()).toString();

            String[] gms=gm.split("[,]");
            String[] uvs=uv.split("[,]");

            for(String gmi: gms){ // 20001:3
                String[] gmis=gmi.split("[:]");
                for(String uvi: uvs){ // 10001:1
                    String[] uvis=uvi.split("[:]");
                    this.k3.set(uvis[0]+","+gmis[0]);
                    this.v3.set(Integer.parseInt(gmis[1])*Integer.parseInt(uvis[1]));
                    context.write(this.k3,this.v3);
                }
            }
        }
    }
}
