package com.briup.bigdata.project.grms.step9;

import com.briup.bigdata.project.grms.step1.UserBuyGoodsList;
import com.briup.bigdata.project.grms.step2.GoodsCooccurrenceList;
import com.briup.bigdata.project.grms.step3.GoodsCooccurrenceMatrix;
import com.briup.bigdata.project.grms.step4.UserBuyGoodsVector;
import com.briup.bigdata.project.grms.step5.MultiplyGoodsMatrixAndUserVector;
import com.briup.bigdata.project.grms.step6.MakeSumForMultiplication;
import com.briup.bigdata.project.grms.step7.DuplicateDataForResult;
import com.briup.bigdata.project.grms.step8.SaveRecommendResultToDB;
import com.briup.bigdata.project.grms.step8.UserAndGoods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GoodsRecommendationManagementSystemJobController extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsRecommendationManagementSystemJobController(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();

        //作业1配置
        Path in1=new Path("/grms/rawdata.txt");//作业1的输入,原始数据
        Path out1=new Path("/grms/result1");//作业1的输出,作业2的输入,作业7的输入
        Path out2=new Path("/grms/result2");//作业2的输出,作业3的输入
        Path out3=new Path("/grms/result3");//作业3的输出,作业5的输入
        Path out4=new Path("/grms/result4");//作业4的输出,作业5的输入
        Path out5=new Path("/grms/result5");//作业5的输出,作业6的输入
        Path out6=new Path("/grms/result6");//作业6的输出,作业7的输入
        Path out7=new Path("/grms/result7");//作业7的输出,作业8的输入

        Job job1= Job.getInstance(conf,"计算用户购买商品的列表");
        job1.setJarByClass(this.getClass());

        job1.setMapperClass(UserBuyGoodsList.UserBuyGoodsListMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1,in1);

        job1.setReducerClass(UserBuyGoodsList.UserBuyGoodsListReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,out1);

        //作业2的配置

        Job job2= Job.getInstance(conf,"计算商品的共现关系");
        job2.setJarByClass(this.getClass());

        job2.setMapperClass(GoodsCooccurrenceList.GoodsCooccurrenceListMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2,out1);

        job2.setReducerClass(GoodsCooccurrenceList.GoodsCooccurrenceListReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2,out2);

        //作业3的配置

        Job job3= Job.getInstance(conf,"计算商品的共现次数(共现矩阵)");
        job3.setJarByClass(this.getClass());

        job3.setMapperClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixMappper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3,out2);

        job3.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3,out3);

        //作业4的配置

        Job job4= Job.getInstance(conf,"计算用户的购买向量");
        job4.setJarByClass(this.getClass());

        job4.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4,in1);

        job4.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4,out4);

        //作业5的配置

        Job job5= Job.getInstance(conf,"数据去重");
        job5.setJarByClass(this.getClass());

        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setInputFormatClass(TextInputFormat.class);

        MultipleInputs.addInputPath(job5,out3,TextInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job5,out4,TextInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorSecondMapper.class);

        job5.setReducerClass(MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5,out5);

        //作业6的配置

        Job job6= Job.getInstance(conf,"对第5步计算的推荐的零散结果进行求和");
        job6.setJarByClass(this.getClass());

        job6.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMapper.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);
        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job6,out5);

        job6.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job6,out6);

        //作业7的配置

        Job job7= Job.getInstance(conf,"数据去重");
        job7.setJarByClass(this.getClass());

        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(IntWritable.class);
        job7.setInputFormatClass(TextInputFormat.class);

        MultipleInputs.addInputPath(job7,out1,TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job7,out6,TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultSecondMapper.class);

        job7.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(IntWritable.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7,out7);

        //作业8的配置

        Job job8= Job.getInstance(conf,"入库");
        job8.setJarByClass(this.getClass());

        job8.setMapperClass(SaveRecommendResultToDB.SaveRecommendResultToDBMapper.class);
        job8.setMapOutputKeyClass(UserAndGoods.class);
        job8.setMapOutputValueClass(NullWritable.class);
        job8.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job8,out7);

        job8.setOutputKeyClass(UserAndGoods.class);
        job8.setOutputValueClass(NullWritable.class);
        job8.setOutputFormatClass(DBOutputFormat.class);

        // 配置数据的连接信息
        DBConfiguration.configureDB(job8.getConfiguration(),"com.mysql.jdbc.Driver","jdbc:mysql://ud2:3306/grms?useSSL=true","root","root");

        // 配置数据输出的信息
        DBOutputFormat.setOutput(job8,"results","uid","gid","exp");

        //创建8个ControlledJob对象,将8个Job对象转化成可被控制的作业
        ControlledJob cj1=new ControlledJob(conf);
        cj1.setJob(job1);

        ControlledJob cj2=new ControlledJob(conf);
        cj2.setJob(job2);

        ControlledJob cj3=new ControlledJob(conf);
        cj3.setJob(job3);

        ControlledJob cj4=new ControlledJob(conf);
        cj4.setJob(job4);

        ControlledJob cj5=new ControlledJob(conf);
        cj5.setJob(job5);

        ControlledJob cj6=new ControlledJob(conf);
        cj6.setJob(job6);

        ControlledJob cj7=new ControlledJob(conf);
        cj7.setJob(job7);

        ControlledJob cj8=new ControlledJob(conf);
        cj8.setJob(job8);

        //作业1的输出,作业2的输入,作业7的输入
        //作业2的输出,作业3的输入
        //作业3的输出,作业5的输入
        //作业4的输出,作业5的输入
        //作业5的输出,作业6的输入
        //作业6的输出,作业7的输入
        //作业7的输出,作业8的输入
        cj2.addDependingJob(cj1);
        cj3.addDependingJob(cj2);
        cj5.addDependingJob(cj3);
        cj6.addDependingJob(cj5);
        cj7.addDependingJob(cj1);
        cj7.addDependingJob(cj6);
        cj8.addDependingJob(cj7);

        //构建JobControl对象，将8个可被控制的作业逐个添加
        JobControl jc=new JobControl("作业流控制");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        jc.addJob(cj7);
        jc.addJob(cj8);

        //构建线程对象，并启动线程，执行作业
        Thread t=new Thread(jc);
        t.start();

        //等待作业完成
        do{
            for(ControlledJob j: jc.getRunningJobList()){
                j.getJob().monitorAndPrintJob();
            }
        }while(!jc.allFinished());

        return 0;
    }
}
