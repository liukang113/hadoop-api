package com.bigdata.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * Author: kang.liu
 * Date  : 2018/5/31 15:39
 * Description:
 */
public class WordCountRunner {

    private static String HDFSUri = "hdfs://bigdata01:9000";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 设置本地文件系统
        //本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
        //到底在哪里，就看以下两行配置你用哪行，默认就是file:///
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
        conf.set("yarn.resourcemanager.hostname", "bigdata01");
        conf.set("hadoop.tmp.dir","/home/bigdata/app/hadoop/data/tmp");
        conf.set("mapreduce.application.classpath",
                "/home/bigdata/app/hadoop/share/hadoop/mapreduce/*, /home/bigdata/app/hadoop/share/hadoop/mapreduce/lib-examples/*");

        //如果实在非hadoop用户环境下提交任务
        System.setProperty("HADOOP_USER_NAME","hadoop");
        System.out.println("HADOOP_USER_NAME: "+System.getProperty("HADOOP_USER_NAME"));

        Path outPath = new Path(args[0]);
        //FileSystem里面包括很多系统，不局限于hdfs
        FileSystem fileSystem = FileSystem.get(URI.create(HDFSUri),conf);
        //删除输出路径
        if(fileSystem.exists(outPath))
        {
            fileSystem.delete(outPath,true);
        }


        Job wcjob = Job.getInstance(conf,"word count");
        //指定我这个job所在的jar包
//		wcjob.setJar("/home/hadoop/wordcount.jar");
		 wcjob.setJarByClass(WordCountRunner.class);

        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setReducerClass(WordCountReduce.class);
        //设置我们的业务逻辑Mapper类的输出key和value的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(IntWritable.class);
        //设置我们的业务逻辑Reducer类的输出key和value的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(IntWritable.class);

        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, "hdfs://bigdata01:9000/test/data");
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://bigdata01:9000/output/wordcount"));

        //向yarn集群提交这个job
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}