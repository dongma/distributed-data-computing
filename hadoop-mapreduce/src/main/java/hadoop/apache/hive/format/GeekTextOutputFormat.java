package hadoop.apache.hive.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * 自定义FileOutputFormat内容，输出替换文本内容
 *
 * @author Sam Ma
 * @date 2022/09/29
 */
public class GeekTextOutputFormat<K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {

    public static class GeekRecordWriter implements FileSinkOperator.RecordWriter,
            JobConfigurable {
        FileSinkOperator.RecordWriter writer;
        BytesWritable bytesWritable;

        public GeekRecordWriter(FileSinkOperator.RecordWriter writer) {
            this.writer = writer;
            this.bytesWritable = new BytesWritable();
        }

        @Override
        public void write(Writable w) throws IOException {
            byte[] input; // get input data
            if (w instanceof Text) {
                input = encode(w.toString());
            } else {
                assert (w instanceof BytesWritable);
                input = ((BytesWritable) w).getBytes();
            }
            byte[] output = input;  // Encode
            bytesWritable.set(output, 0, output.length);
            writer.write(bytesWritable);
        }

        /**
         * 文件输入时每随机2～256个单词，就插入一个gee..k，字母e个数等于前面出现的非gee...k单词的个数
         *
         * @param content
         * @return
         */
        private byte[] encode(String content) {
            String[] words = content.split(" ");
            StringBuilder sb = new StringBuilder();
            int bound = 254;
            int r = new Random().nextInt(bound) + 2;
            int j = 0;
            for (int i = 0; i < words.length; i++, j++) {
                sb.append(words[i]).append(" ");
                if (j == r) {
                    sb.append("g");
                    for (int i1 = 0; i1 < j; i1++) {
                        sb.append("e");
                    }
                    sb.append("k").append(" ");
                    j = 0;
                    r = new Random().nextInt(bound) + 2;
                }
            }
            return sb.toString().getBytes();
        }

        @Override
        public void close(boolean abort) throws IOException {
            writer.close(abort);
        }

        @Override
        public void configure(JobConf jobConf) {
        }
    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass,
                                                             boolean isCompressed, Properties tableProperties,
                                                             Progressable progress) throws IOException {
        GeekRecordWriter writer = new GeekRecordWriter(super.getHiveRecordWriter(jc,
                finalOutPath, BytesWritable.class, isCompressed,
                tableProperties, progress));
        writer.configure(jc);
        return writer;
    }

}
