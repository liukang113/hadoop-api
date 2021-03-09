package com.bigdata.flowcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * @Author: kang.liu
 * Date  : 2018/6/1 16:47
 * Description:
 */
public class FlowBean implements Writable {

    private long upFlow;
    private long dFlow;
    private long sumFlow;

    public FlowBean() {
    }

    public FlowBean(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow + dFlow;
    }

    /**
     * 序列化方法
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dFlow);
        out.writeLong(sumFlow);

    }
    /**
     * 反序列化方法
     * 注意：反序列化的顺序跟序列化的顺序完全一致
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        dFlow = in.readLong();
        sumFlow = in.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {

        final StringBuffer sb = new StringBuffer("FlowBean{");
        sb.append("upFlow=").append(upFlow);
        sb.append(", dFlow=").append(dFlow);
        sb.append(", sumFlow=").append(sumFlow);
        sb.append('}');
        return sb.toString();
    }
}