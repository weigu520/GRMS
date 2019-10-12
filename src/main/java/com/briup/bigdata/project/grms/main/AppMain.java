package com.briup.bigdata.project.grms.main;

import com.briup.bigdata.project.grms.step9.GoodsRecommendationManagementSystemJobController;
import org.apache.hadoop.util.ToolRunner;

public class AppMain {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsRecommendationManagementSystemJobController(),args));
    }
}
