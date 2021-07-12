package hadoop.apache.org.mr

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2021/07/12
 * 使用hadoop mapreduce分析每一个手机号耗费的总上行流量、下行流量、总流量
 */
object AnalysisPhoneFlow {

  private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  class MapStage extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {

    override def map(k1: LongWritable, v1: Text, outputCollector: OutputCollector[Text, IntWritable],
                     reporter: Reporter): Unit = {

    }

  }

  class ReduceStage extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {

    override def reduce(k2: Text, iterator: util.Iterator[IntWritable],
                        outputCollector: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {

    }

  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      logger.warn("AnalysisPhoneFlow <input-path> <output-path>")
      System.exit(1)
    }

    val jobConf: JobConf = new JobConf(this.getClass)
    jobConf.setJobName("analysis phone flow Job")
    jobConf.setOutputKeyClass(classOf[Text])
    jobConf.setOutputValueClass(classOf[IntWritable])
    jobConf.setMapperClass(classOf[MapStage])
    jobConf.setReducerClass(classOf[ReduceStage])
    jobConf.setInputFormat(classOf[TextInputFormat])
    jobConf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(jobConf, new Path(args(0)))
    FileOutputFormat.setOutputPath(jobConf, new Path(args(1)))
    JobClient.runJob(jobConf)
  }

}
