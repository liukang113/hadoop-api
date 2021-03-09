package com.bigdata.flowcount.mr;

import com.bigdata.flowcount.FlowBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: kang.liu
 * Date  : 2018/6/1 16:51
 * Description:
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行数据
        String line = value.toString();
        String[] fileds = line.split("\t");
        String phoneNumber = fileds[1];
        long upFlow = Long.parseLong(fileds[fileds.length - 3]);
        long dFlow = Long.parseLong(fileds[fileds.length - 2]);
        context.write(new Text(phoneNumber), new FlowBean(upFlow, dFlow));
    }
}