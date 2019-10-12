package com.briup.bigdata.project.grms.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {
        //共现矩阵
//        20001	20001:3,20002:2,20005:2,20006:2,20007:1
//        20002	20001:2,20002:3,20005:2,20006:2,20007:2
//        20003	20003:1,20004:1,20006:1
//        20004	20003:1,20004:2,20006:1,20007:1
//        20005	20001:2,20002:2,20005:2,20006:2,20007:1
//        20006	20001:2,20002:2,20003:1,20004:1,20005:2,20006:3,20007:1
//        20007	20001:1,20002:2,20004:1,20005:1,20006:1,20007:3
        //购买向量
//        20001	10001:1,10004:1,10005:1
//        20002	10001:1,10003:1,10004:1
//        20003	10002:1
//        20004	10002:1,10006:1
//        20005	10001:1,10004:1
//        20006	10001:1,10002:1,10004:1
//        20007	10001:1,10003:1,10006:1
    public static class MultiplyGoodsMatrixAndUserVectorFirstMapper
        extends Mapper<LongWritable, Text, Text,Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.set(strs[0]);//代表用户10001
            this.v2.set(strs[1]);
            context.write(this.k2,this.v2);
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorSecondMapper
            extends Mapper<LongWritable, Text, Text,Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.set(strs[0]);//代表商品20001
            this.v2.set(strs[1]);
            context.write(this.k2,this.v2);
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<Text,Text,Text, IntWritable>{
        private Text k3=new Text();
        private IntWritable v3=new IntWritable();
        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            Map<String,Integer> users = new HashMap<>();
            Map<String,Integer> goods = new HashMap<>();

            for (Text v2 : v2s) {
                //用户
                if ("1".equals(v2.toString().substring(0,1))){
                    String[] strs1 = v2.toString().split("[,]");
                    for (String str1 : strs1) {
                        String[] s1 = str1.split("[:]");
                        users.put(s1[0],Integer.parseInt(s1[1]));
                    }
                }
                if ("2".equals(v2.toString().substring(0,1))){
                    String[] strs1 = v2.toString().split("[,]");
                    for (String str1 : strs1) {
                        String[] s1 = str1.split("[:]");
                        goods.put(s1[0],Integer.parseInt(s1[1]));
                    }
                }
            }

            users.forEach((k,v)->{
                goods.forEach((k1,v1)->{
                    this.k3.set(k+","+k1);
                    this.v3.set(v*v1);
                    try {
                        context.write(this.k3,this.v3);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            });

        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in1=new Path("/rawdatares3/part-r-00000");
        Path in2=new Path("/rawdatares4/part-r-00000");
        Path out=new Path("/rawdatares5");

        Job job= Job.getInstance(conf,"数据去重");
        job.setJarByClass(this.getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        MultipleInputs.addInputPath(job,in1,TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorSecondMapper.class);

        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args));
    }
}
