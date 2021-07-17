package hadoop.apache.org.mr

import java.util

import com.typesafe.scalalogging.Logger
import hadoop.apache.bean.{FlowBean, ReduceData}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred._

/**
 * @author Sam Ma
 * @date 2021/07/12
 * 使用hadoop mapreduce分析每一个手机号耗费的总上行流量、下行流量、总流量
 */
object AnalysisPhoneFlow {

  private[this] val logger = Logger(AnalysisPhoneFlow.getClass)

  class MapStage extends MapReduceBase with Mapper[LongWritable, Text, Text, FlowBean] {
    private val phoneKey = new Text()

    /** map stage用于读取一行数据，切分字段；并以手机号为key, bean对象为value进行输出 */
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, FlowBean],
                     reporter: Reporter): Unit = {
      val line: String = value.toString
      val items: Array[String] = line.split("\t")
      /*logger.info("using tab character to split string, phone: {}, upflow: {}, downflow: {}",
        items(1), items(7), items(8))*/
      phoneKey.set(items(1))
      val flowBean: FlowBean = new FlowBean(items(8).toLong, items(9).toLong)
      output.collect(phoneKey, flowBean)
    }

  }

  /** reduce stage用于对同一个手机号，统计其上行流量总和、下行流量总和 */
  class ReduceStage extends MapReduceBase with Reducer[Text, FlowBean, ReduceData, Text] {

    override def reduce(phoneNum: Text, values: util.Iterator[FlowBean],
                        output: OutputCollector[ReduceData, Text], reporter: Reporter): Unit = {
      var downFlowSum, upFlowSum = 0L
      while (values.hasNext) {
        val flowBean: FlowBean = values.next()
        upFlowSum = flowBean.getUpFlow + upFlowSum
        downFlowSum = flowBean.getDownFlow + downFlowSum
      }
      /* ReduceData实现了WritableComparable接口（按totalUsed进行排序），value数据设置为空 */
      output.collect(new ReduceData(phoneNum.toString, upFlowSum, downFlowSum), new Text())
    }

  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      logger.warn("Invalid command args, please input <input-path> <output-path>")
      System.exit(1)
    }

    val jobConf: JobConf = new JobConf(this.getClass)
    jobConf.setJobName("analysis phone flow Job")
    // 设置map阶段输出Key的Class类型，Value类型
    jobConf.setMapOutputKeyClass(classOf[Text])
    jobConf.setMapOutputValueClass(classOf[FlowBean])

    // 设置reduce阶段输出的数据类型，key为ReduceData类型，value为Text类型
    jobConf.setOutputKeyClass(classOf[ReduceData])
    jobConf.setOutputValueClass(classOf[Text])

    // MapStage执行map阶段工作，ReduceStage对分组后的数据进行聚合
    jobConf.setMapperClass(classOf[MapStage])
    jobConf.setReducerClass(classOf[ReduceStage])
    jobConf.setInputFormat(classOf[TextInputFormat])
    jobConf.setOutputFormat(classOf[TextOutputFormat[ReduceData, Text]])

    FileInputFormat.setInputPaths(jobConf, new Path(args(0)))
    FileOutputFormat.setOutputPath(jobConf, new Path(args(1)))
    JobClient.runJob(jobConf)
  }

}
