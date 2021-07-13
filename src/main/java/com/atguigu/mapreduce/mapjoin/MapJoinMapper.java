package com.atguigu.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private HashMap<String,String> pdMap = new HashMap<>();
    private Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取缓存文件 并把文件内容封装到集合
        URI[] cacheFiles = context.getCacheFiles();

        //获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        // 从流中读取数据 通过包装流转换为 reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        // 逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            // 切割
            String[] split = line.split("\t");

            //赋值
            pdMap.put(split[0],split[1]);
        }
        IOUtils.closeStreams(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取大表数据 处理order.txt
        String line = value.toString();
        String[] split = line.split("\t");
        // 通过大表每行数据的 pid,去 pdMap 里面取出 pname
        String pname = pdMap.get(split[1]);

        // 将大表每行数据的 pid 替换为 pname
        text.set(split[0]+"\t"+pname+"\t"+split[2]);

        context.write(text,NullWritable.get());

    }


}
