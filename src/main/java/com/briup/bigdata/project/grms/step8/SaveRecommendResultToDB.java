package com.briup.bigdata.project.grms.step8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SaveRecommendResultToDB extends Configured implements Tool {
    public static class SaveRecommendResultToDBMapper extends Mapper<LongWritable,Text,UserAndGoods,NullWritable>{
        private UserAndGoods k2=new UserAndGoods();
        private NullWritable v2=NullWritable.get();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.setUid(strs[0]);
            this.k2.setGid(strs[1]);
            this.k2.setExp(Integer.parseInt(strs[2]));
            context.write(this.k2,this.v2);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path("/rawdatares7/part-r-00000");

        Job job= Job.getInstance(conf,"入库");
        job.setJarByClass(this.getClass());

        job.setMapperClass(SaveRecommendResultToDBMapper.class);
        job.setMapOutputKeyClass(UserAndGoods.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(UserAndGoods.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        // 配置数据的连接信息
        DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.jdbc.Driver","jdbc:mysql://ud2:3306/grms?useSSL=true","root","root");

        // 配置数据输出的信息
        DBOutputFormat.setOutput(job,"results","uid","gid","exp");
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SaveRecommendResultToDB(),args));
    }
}
