package com.atguigu.writeablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,FlowBean,Text > {
    private FlowBean outKey = new FlowBean();
    private Text outValue = new Text();
        // 重写map()
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 按行读取，切分
        String line = value.toString();
        String[] split = line.split("\t");
        // 封装输出
        outValue.set(split[0]);
        outKey.setUpFlow(Long.parseLong(split[1]));
        outKey.setDownFlow(Long.parseLong(split[2]));
        outKey.setSumFlow();
        // 输出
        context.write(outKey,outValue);
    }
}
