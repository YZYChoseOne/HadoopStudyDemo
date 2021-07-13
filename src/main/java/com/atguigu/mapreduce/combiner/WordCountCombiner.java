package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {
   // private Text outKey = new Text();
    private IntWritable outValue = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
           sum += value.get();
        }
        outValue.set(sum);
        context.write(key,outValue);
    }
}
