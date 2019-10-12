package com.briup.bigdata.project.grms.step4;

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

public class UserBuyGoodsVector extends Configured implements Tool {
//             20001	10001:1,10004:1,10005:1
//             20002	10001:1,10003:1,10004:1
//             20003	10002:1
//             20004	10002:1,10006:1
//             20005	10001:1,10004:1
//             20006	10001:1,10002:1,10004:1
//             20007	10001:1,10003:1,10006:1

    public static class UserBuyGoodsVectorMapper extends Mapper<LongWritable, Text,Text,Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.set(strs[1]);
            this.v2.set(strs[0]);
            context.write(this.k2,this.v2);
        }
    }
//          20001  10001
//          20002  10001
//          20005  10001
//          20006  10001
//          20007  10001
//          20003  10002
//          20004  10002
//          20006  10002
//          20002  10003
//          20007  10003
//          20001  10004
//          20002  10004
//          20005  10004
//          20006  10004
//          20001  10005
//          20004  10006
//          20007  10006


    public static class UserBuyGoodsVectorReducer extends Reducer<Text,Text,Text,Text> {
        private Text v3=new Text();
        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text v2 : v2s) {
                sb.append(v2.toString()).append(":").append(1).append(",");
            }
            this.v3.set(sb.substring(0,sb.length()-1));
            context.write(k2,this.v3);
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));

        Job job= Job.getInstance(conf,"计算用户的购买向量");
        job.setJarByClass(this.getClass());

        job.setMapperClass(UserBuyGoodsVectorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(UserBuyGoodsVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsVector(),args));
    }
}
