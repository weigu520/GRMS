package com.briup.bigdata.project.grms.step3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GoodsCooccurrenceMatrix extends Configured implements Tool {
    public static class GoodsCooccurrenceMatrixMappper extends Mapper<LongWritable, Text,Text,Text>{
//         20007	20006
//         20007	20004
//         20007	20007
//         20007	20002
//         20007	20007
//         20007	20005
//         20007	20001
//         20007	20002
//         20007	20007
//         20007	20001:1,20002:2,20004:1,20005:1,20006:1,20007:3
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.set(strs[0]);
            this.v2.set(strs[1]);
            context.write(this.k2,this.v2);
        }
    }

    public static class GoodsCooccurrenceMatrixReducer extends Reducer<Text,Text,Text,Text>{
        private Text k3=new Text();
        private Text v3=new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            Map<String,Integer> map=new HashMap();
            StringBuilder sb = new StringBuilder();
            for (Text v2 : v2s) {
                map.put(v2.toString(),map.get(v2.toString())==null?1:(map.get(v2.toString())+1));
            }
            map.forEach((k,v)-> sb.append(k).append(":").append(v).append(","));
            map.clear();
            this.k3.set(k2.toString());
            this.v3.set(sb.substring(0,sb.length()-1));
            context.write(this.k3,this.v3);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path("/rawdatares2/part-r-00000");
        Path out=new Path("/rawdatares3");

        Job job= Job.getInstance(conf,"计算商品的共现次数(共现矩阵)");
        job.setJarByClass(this.getClass());

        job.setMapperClass(GoodsCooccurrenceMatrixMappper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceMatrix(),args));
    }
}
