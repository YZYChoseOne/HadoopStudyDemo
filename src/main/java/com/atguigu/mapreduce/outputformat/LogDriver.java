package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 设置jar包路径
        job.setJarByClass(LogDriver.class);
        //3 关联mapper和reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        //4 设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5 设置最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogOutputFormat.class);
        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\java_IDEA\\Hadoop-StudyMapReduceDemo\\src\\main\\resources\\log.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\java_IDEA\\Hadoop-StudyMapReduceDemo\\src\\main\\resources\\outputformat\\outputFormatWeb"));
        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
