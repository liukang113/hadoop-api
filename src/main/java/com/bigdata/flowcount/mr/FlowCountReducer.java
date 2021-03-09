package com.bigdata.flowcount.mr;

import com.bigdata.flowcount.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: kang.liu
 * Date  : 2018/6/1 17:04
 * Description:
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 循环遍历相同key 传输的Bean计算
        long sum_upFlow = 0;
        long sum_dFlow = 0;
        for (FlowBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_dFlow += bean.getdFlow();
        }
        // 汇总
        FlowBean result = new FlowBean(sum_upFlow, sum_dFlow);
        context.write(new Text(key), result);
    }
}