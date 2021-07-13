package com.atguigu.mapreduce.mapjoin;

import com.atguigu.mapreduce.partition2.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 设置jar包路径
        job.setJarByClass(MapJoinDriver.class);
        //3 关联mapper和reducer
        job.setMapperClass(MapJoinMapper.class);
        //job.setReducerClass(FlowReducer.class);
        //4 设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5 设置最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
            // 加载缓存数据
        job.addCacheFile(new URI("file:///D:/java_IDEA/Hadoop-StudyMapReduceDemo/src/main/resources/joininput/pd"));
           // Map 端 Join 的逻辑不需要 Reduce 阶段，设置 reduceTask 数量为 0
        job.setNumReduceTasks(0);
        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\java_IDEA\\Hadoop-StudyMapReduceDemo\\src\\main\\resources\\joininput\\order"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\java_IDEA\\Hadoop-StudyMapReduceDemo\\src\\main\\resources\\mapjoinoutput"));
        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
