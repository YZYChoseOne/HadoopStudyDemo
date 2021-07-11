package com.atguigu.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {
    private Text outKey = new Text();
    private TableBean outValue = new TableBean();
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("line*****************"+line);

        // 封装
        if(fileName.contains("order")) { // 商品表
            String[] split = line.split("\t");

            outKey.set(split[1]);
            outValue.setId(split[0]);
            outValue.setPid(split[1]);
            outValue.setAmount(Integer.parseInt(split[2]));
            outValue.setPname("");
            outValue.setFlag("order");
            System.out.println("orderkey情况:" + outKey);
            System.out.println("ordevaluer情况:" + outValue);
        }else {
            String[] split = line.split("\t");
            outKey.set(split[0]);
            outValue.setId("");
            outValue.setPid(split[0]);
            outValue.setAmount(0);
            outValue.setPname(split[1]);
            outValue.setFlag("pd");
            System.out.println("otherkey情况:" + outKey);
            System.out.println("othervalue情况:" + outValue);
        }

        context.write(outKey,outValue);
    }
}
