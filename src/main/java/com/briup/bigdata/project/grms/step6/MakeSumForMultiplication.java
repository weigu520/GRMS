package com.briup.bigdata.project.grms.step6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class MakeSumForMultiplication extends Configured implements Tool {
    public static class MakeSumForMultiplicationMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        private Text k2=new Text();
        private IntWritable v2=new IntWritable();
//        10003,20001	2
//        10003,20001	1
//        10003,20002	3
//        10003,20002	2
//        10003,20004	1
//        10003,20005	2
//        10003,20005	1
//        10003,20006	2
//        10003,20006	1
//        10003,20007	2
//        10003,20007	3

//        10003,20001	3
//        10003,20002	5
//        10003,20004	1
//        10003,20005	3
//        10003,20006	3
//        10003,20007	5
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.set(strs[0]);
            this.v2.set(Integer.parseInt(strs[1]));
            context.write(this.k2,this.v2);
        }
    }
    public static class MakeSumForMultiplicationReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable v3=new IntWritable();

        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable v2 : v2s) {
                sum+=v2.get();
            }
            this.v3.set(sum);
            context.write(k2,this.v3);
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));

        Job job= Job.getInstance(conf,"对第5步计算的推荐的零散结果进行求和");
        job.setJarByClass(this.getClass());

        job.setMapperClass(MakeSumForMultiplicationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(MakeSumForMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MakeSumForMultiplication(),args));
    }
}
