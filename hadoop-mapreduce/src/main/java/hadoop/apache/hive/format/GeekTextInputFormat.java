package hadoop.apache.hive.format;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Arrays;

/**
 * Geek自定义Text Input值
 *
 * @author Sam Ma
 * @date 2022/09/29
 */
public class GeekTextInputFormat implements InputFormat<LongWritable, BytesWritable>, JobConfigurable {

    public static class GeekLineRecordReader implements
            RecordReader<LongWritable, BytesWritable>, JobConfigurable {
        LineRecordReader reader;
        Text text;

        public GeekLineRecordReader(LineRecordReader reader) {
            this.reader = reader;
            text = reader.createValue();
        }

        @Override
        public void configure(JobConf jobConf) {
        }

        @Override
        public boolean next(LongWritable key, BytesWritable value) throws IOException {
            while (reader.next(key, text)) {
                String newStr = decode();
                // text -> byte[] -> value
                byte[] textBytes = newStr.getBytes();
                int length = text.getLength();
                // trim additional bytes
                if (length != textBytes.length) {
                    textBytes = Arrays.copyOf(textBytes, length);
                }
                value.set(textBytes, 0, textBytes.length);
                return true;
            }
            return false;   // no more data
        }

        private String decode() {
            return text.toString().replaceAll("gee+k", "");
        }

        @Override
        public LongWritable createKey() {
            return reader.createKey();
        }

        @Override
        public BytesWritable createValue() {
            return new BytesWritable();
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }
    }

    /** GeekTextInputFormat#业务的具体实现 */
    TextInputFormat format;
    JobConf job;

    public GeekTextInputFormat() {
        format = new TextInputFormat();
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        format.configure(job);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return format.getSplits(job, numSplits);
    }

    @Override
    public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit genericSplit, JobConf job,
                                                                     Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        GeekLineRecordReader reader = new GeekLineRecordReader(new LineRecordReader(job,
                (FileSplit) genericSplit));
        reader.configure(job);
        return reader;
    }

}
