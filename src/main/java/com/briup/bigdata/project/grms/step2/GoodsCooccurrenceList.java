package com.briup.bigdata.project.grms.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class GoodsCooccurrenceList extends Configured implements Tool {
    public static class GoodsCooccurrenceListMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        private Text k2=new Text();
        private NullWritable v2=NullWritable.get();
    //    10001	20001,20005,20006,20007,20002
    //    10002	20006,20003,20004
    //    10003	20002,20007
    //    10004	20001,20002,20005,20006
    //    10005	20001
    //    10006	20004,20007
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.set(strs[1]);
            context.write(this.k2,this.v2);
        }
    }
    public static class GoodsCooccurrenceListReducer extends Reducer<Text,NullWritable,Text,Text>{
        private Text k3=new Text();
        private Text v3=new Text();
//        20001	20001
//        20001	20001
//        20001	20002
//        20001	20005
//        20001	20006
//        20001	20007
//        20001	20001
//        20001	20006
//        20001	20005
//        20001	20002
        @Override
        protected void reduce(Text k2, Iterable<NullWritable> v2s, Context context) throws IOException, InterruptedException {
            String[] strs = k2.toString().split("[,]");
            //20001 20005 20006 20007 20002
            for (int i=0;i<strs.length;i++) {
                this.k3.set(strs[i]);
                for (int j=0;j<strs.length;j++){
                    this.v3.set(strs[j]);
                    context.write(this.k3,this.v3);
                }
            }
        }
    }


    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));

        Job job= Job.getInstance(conf,"计算商品的共现关系");
        job.setJarByClass(this.getClass());

        job.setMapperClass(GoodsCooccurrenceListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(GoodsCooccurrenceListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceList(),args));
    }
}
