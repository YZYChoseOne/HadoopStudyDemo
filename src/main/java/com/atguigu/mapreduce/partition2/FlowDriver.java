package com.atguigu.mapreduce.partition2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取job
        Configuration conf = new Configuration();
          // 任务执行队列设置
        //conf.set("mapreduce.job.queuename","hive");

        Job job = Job.getInstance(conf);


        //2 设置jar包路径
        job.setJarByClass(FlowDriver.class);
        //3 关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4 设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5 设置最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);
        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\work\\program\\code\\HadoopStudyDemo\\src\\main\\resources\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\work\\program\\code\\HadoopStudyDemo\\src\\main\\resources\\phone_partition_DEBUG"));
        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
