package com.briup.bigdata.project.grms.step7;

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

public class DuplicateDataForResult extends Configured implements Tool {
    //1.FirstMapper处理用户的购买列表数据
//    10001	20001,20005,20006,20007,20002
//    10002	20006,20003,20004
//    10003	20002,20007
//    10004	20001,20002,20005,20006
//    10005	20001
//    10006	20004,20007
    private static Map<String,Integer> map=new HashMap<>();
    public static class DuplicateDataForResultFirstMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
//        private Text k2=new Text();
//        private IntWritable v2=new IntWritable(1);
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
//            StringBuilder sb = new StringBuilder();
            String[] strs = v1.toString().split("[\t]");
            String[] goods = strs[1].split("[,]");
            for (String good : goods) {
//                sb.append(strs[0]).append(",").append(good);
                map.put(strs[0]+","+good,1);
//                this.k2.set(sb.toString());
//                context.write(this.k2,this.v2);
            }
        }
    }

    //2.SecondMapper处理第6的推荐结果数据
//    10001,20001	10
//    10001,20002	11
//    10001,20003	1
//    10001,20004	2
//    10001,20005	9
//    10001,20006	10
//    10001,20007	8
    public static class DuplicateDataForResultSecondMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        private Text k2=new Text();
        private IntWritable v2=new IntWritable();
//        10001	20004	2
//        10001	20003	1
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");

            this.k2.set(strs[0]);
            this.v2.set(Integer.parseInt(strs[1]));
            context.write(this.k2,this.v2);

        }
    }

    public static class DuplicateDataForResultReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private Text k3=new Text();
        private IntWritable v3=new IntWritable();
        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {

            String[] strs = k2.toString().split("\t");
            String[] ug = strs[0].split(",");
            this.k3.set(ug[0]+"\t"+ug[1]);

            if (map.get(strs[0])==null) {
                for (IntWritable v2 : v2s) {
                    this.v3.set(v2.get());
                    context.write(this.k3,this.v3);
                }
            }

        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in1=new Path("/rawdatares/part-r-00000");
        Path in2=new Path("/rawdatares6/part-r-00000");
        Path out=new Path("/rawdatares7");

        Job job= Job.getInstance(conf,"数据去重");
        job.setJarByClass(this.getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);

        MultipleInputs.addInputPath(job,in1,TextInputFormat.class,DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,DuplicateDataForResultSecondMapper.class);

        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DuplicateDataForResult(),args));
    }
}
