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
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * Author: kang.liu
 * Date  : 2018/6/4 15:01
 * Description:
 */
public class ShareFriendsTwo {

    static class ShareFriendsTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineData = value.toString();
            String[] splitData = lineData.split("\t");
            String friend = splitData[0];
            String[] persons = splitData[1].split(",");
            Arrays.sort(persons);
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    // 发出 <人-人，好友> ，这样，相同的“人-人”对的所有好友就会到同1个reduce中去
                    context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
                }
            }
        }
    }

    static class ShareFriendsTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();

            for (Text friend : friends) {
                sb.append(friend).append(" ");

            }
            context.write(person_person, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ShareFriendsTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ShareFriendsTwoMapper.class);
        job.setReducerClass(ShareFriendsTwoReducer.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\work_space\\studentsystem\\hadoop-api\\src\\main\\java\\com\\bigdata\\fensi\\temp\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\work_space\\studentsystem\\hadoop-api\\src\\main\\java\\com\\bigdata\\fensi\\temp2"));

        job.waitForCompletion(true);

    }

}