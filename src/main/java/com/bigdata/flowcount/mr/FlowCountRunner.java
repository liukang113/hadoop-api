package com.bigdata.flowcount.mr;


import com.bigdata.flowcount.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: kang.liu
 * Date  : 2018/5/31 15:39
 * Description:
 *
 * @author ll
 */
public class FlowCountRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
        // 指定我这个job所在的jar包
        // wacko.setJar("/home/hadoop/wordcount.jar");
        wcjob.setJarByClass(FlowCountRunner.class);

        wcjob.setMapperClass(FlowCountMapper.class);
        wcjob.setReducerClass(FlowCountReducer.class);
        //设置我们的业务逻辑Mapper类的输出key和value的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(FlowBean.class);
        //设置我们的业务逻辑Reducer类的输出key和value的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(FlowBean.class);

        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, "hdfs://node1:9000/input/flowcount.data");
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://node1:9000/output/flowcount3"));

        //向yarn集群提交这个job
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}