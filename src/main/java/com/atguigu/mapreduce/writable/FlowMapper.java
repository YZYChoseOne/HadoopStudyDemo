package com.atguigu.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    private Text outKey = new Text();
    private FlowBean outBean = new FlowBean();
        // 重写map()
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
       // 抓取数据
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];
        // 封装
        outKey.set(phone);
        outBean.setUpFlow(Long.parseLong(up));
        outBean.setDownFlow(Long.parseLong(down));
        outBean.setSumFlow();
        // 输出
        context.write(outKey,outBean);
    }
}
