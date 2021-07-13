package hadoop.apache.bean;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Sam Ma
 * @date 2021/07/13
 * 定义reduce最终输出的数据格式: phone手机号，上行流量总和，下行流量总和，总流量
 */
@Getter
@Setter
public class ReduceData implements WritableComparable {

    /**
     * 统计的手机号码
     */
    private String phoneNum;

    /**
     * 上行流量总和
     */
    private long upFlowSum;

    /**
     * 下行流量总和
     */
    private long downFlowSum;

    /**
     * 使用流量总和
     */
    private long totalUsed;

    public ReduceData() {
    }

    public ReduceData(Long upFlowSum, Long downFlowSum) {
        this.upFlowSum = upFlowSum;
        this.downFlowSum = downFlowSum;
        this.totalUsed = upFlowSum + downFlowSum;
    }

    public void set(long upFlowSum, long downFlowSum) {
        this.upFlowSum = upFlowSum;
        this.downFlowSum = downFlowSum;
        this.totalUsed = upFlowSum + downFlowSum;
    }

    @Override
    public int compareTo(Object other) {
        return (int) (this.totalUsed - ((ReduceData) other).totalUsed);
    }

    /**
     * 序列化
     *
     * @param output
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeChars(phoneNum);
        output.writeLong(upFlowSum);
        output.writeLong(downFlowSum);
        output.writeLong(totalUsed);
    }

    /**
     * 反序列化，字段属性顺序必须一致
     *
     * @param input
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        this.phoneNum = input.readLine();
        this.upFlowSum = input.readLong();
        this.downFlowSum = input.readLong();
        this.totalUsed = input.readLong();
    }

    @Override
    public String toString() {
        return phoneNum + "\t" + upFlowSum + "\t" + downFlowSum + "\t" + totalUsed;
    }

}
