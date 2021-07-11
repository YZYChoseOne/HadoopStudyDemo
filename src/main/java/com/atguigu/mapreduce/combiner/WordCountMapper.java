package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <KEYIN>
 * <VALUEIN>
 * <KEYOUT>
 * <VALUEOUT>
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text keyOut = new Text();
    private IntWritable valueOut = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            keyOut.set(word);
            context.write(keyOut,valueOut);
        }
    }
}

