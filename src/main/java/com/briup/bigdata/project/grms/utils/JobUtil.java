package com.briup.bigdata.project.grms.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class JobUtil{
    private static Job job;
    private static String in;
    private static String out;
    private static Configuration configuration;

    public static void setConf(                 // 定义setConf方法
        Configuration c,                        // MapReduce作业传递的整个作业的配置对象
        Class cz,                               // MapReduce作业志行的jar包中包含的主类的镜像
        String name,                            // 作业的名字
        String vin,                             // 数据的输入路径
        String vout                             // 数据的输出路径
    ){
        try{
            if(c==null){
                throw new Exception("配置信息不能为null。");
            }
            job=Job.getInstance(c,name);        // 构建Job对象，设置配置对象和作业名
            job.setJarByClass(cz);              // 提供执行的作业的主类的镜像
            in=vin;                             // 将数据的输入路径传递给全局变量
            out=vout;                           // 将数据的输出路径传递给全局变变量
            configuration=c;
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void setMapper(               // 定义setMapper方法
                                                Class<? extends Mapper> x,              // 设置作业中运行的Mapper类的镜像参数
                                                Class<? extends Writable> y,            // 设置作业中Mapper的Key的数据类型参数
                                                Class<? extends Writable> z,            // 设置作业中Mapper的Value的数据类型参数
                                                Class<TextInputFormat> o          // 设置作业中数据输入的格式参数
    ){
        try{
            job.setMapperClass(x);
            job.setMapOutputKeyClass(y);
            job.setMapOutputValueClass(z);
            job.setInputFormatClass(o);
            o.getMethod("addInputPath",Job.class,Path.class).invoke(null,job,new Path(in));
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void setReducer(              // 定义setReducer方法
        Class<? extends Reducer> a,             // 设置作业中运行的Reducer类的镜像参数
        Class<? extends Writable> b,            // 设置作业中Reducer的Key的数据类型参数
        Class<? extends Writable> c,            // 设置作业中Reducer的Value的数据类型参数
        Class<? extends OutputFormat> d,        // 设置作业中数据输出的格式参数
        int rnum                                // 设置Reducer的个数
    ){
        try{
            job.setReducerClass(a);
            job.setOutputKeyClass(b);
            job.setOutputValueClass(c);
            job.setOutputFormatClass(d);
            d.getMethod("setOutputPath",Job.class,Path.class).invoke(null,job,new Path(out));
            job.setNumReduceTasks(rnum<=1?1:rnum);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void setTotalSort(float a,int b,int c) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException{
        job.setPartitionerClass(TotalOrderPartitioner.class);
        InputSampler.writePartitionFile(job,new RandomSampler(a,b,c));
        job.addCacheFile(new URI(TotalOrderPartitioner.getPartitionFile(getConfiguration())));
    }

    public static Configuration getConfiguration(){
        return job.getConfiguration();
    }

    public static void setConfiguration(Configuration configuration){
        JobUtil.configuration=configuration;
    }

    public static void setSecondarySort(Class<? extends WritableComparator> g,Class<? extends WritableComparator> s,Class<? extends Partitioner> p) throws ClassNotFoundException{
        job.setPartitionerClass(p);
        job.setGroupingComparatorClass(g);
        job.setSortComparatorClass(s);
    }

    public static void setCombiner(boolean flag,Class<? extends Reducer> combiner){
        if(flag&&combiner!=null) job.setCombinerClass(combiner);
    }

    public static int commit() throws Exception{
        return job.waitForCompletion(true)?0:1;         // 提交作业
    }

    public static Job getJob(){
        return job;
    }

    public static void setJob(Job xyz){
        JobUtil.job=xyz;
    }
}

