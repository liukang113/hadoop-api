package com.bigdata.fensi.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: kang.liu
 * Date  : 2018/6/4 12:28
 * Description: 好友互粉
 */
public class ShareFriendsOne {

    static class ShareFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text> {
        /* 数据格式
        A:B,C,D,F,E,O
        B:A,C,E,K
        C:F,A,D,I
        D:A,E,F,L*/
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineData = value.toString();
            String[] splitData = lineData.split(":");
            String person = splitData[0];
            String friends = splitData[1];
            // 第一阶段的map数据结果<好友，人>

            for (String friend : friends.split(",")) {
                // 输出<好友，人>
                context.write(new Text(friend), new Text(person));
            }

        }
    }


    static class ShareFriendsOneReduceer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            persons.forEach(person -> {
                stringBuilder.append(person).append(",");
            });
            context.write(new Text(friend), new Text(stringBuilder.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ShareFriendsOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ShareFriendsOneMapper.class);
        job.setReducerClass(ShareFriendsOneReduceer.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\work_space\\studentsystem\\hadoop-api\\src\\main\\java\\com\\bigdata\\fensi\\frienddata\\1111.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\work_space\\studentsystem\\hadoop-api\\src\\main\\java\\com\\bigdata\\fensi\\temp"));

        job.waitForCompletion(true);
    }
}