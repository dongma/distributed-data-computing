package org.apache.databricks

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * @author Sam Ma
 * @date 2021/09/04
 * 使用Spark实现Hadoop 分布式数据传输工具 DistCp (distributed copy)
 */
object SparkDictCp {

  private[this] val logger = Logger(SparkDictCp.getClass)

  /*
   * spark-submit --class org.apache.databricks.SparkDictCp --master yarn --deploy-mode cluster
   * --driver-memory 1G --executor-memory 1G
   * --executor-cores 2 databricks-app-1.0.0-jar-with-dependencies.jar
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("spark DistCp App")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    logger.info("spark distCp app command line args: [{}]", args)

    // use apache commons-cli to parse application args
    /*val options = new Options()
    options.addOption("currency", true, "currency config")
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)*/

    val MAX_CONCURRENCES = /*if (cmd.hasOption("currency"))
      cmd.getOptionValue("currency").toInt
    else*/ 2

    val input = "hdfs://spotify-mac:9000/user/madong/input"
    val distPath = "hdfs://spotify-mac:9000/user/madong/dist"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listFiles(new Path(input), true)

    val fileArray = ArrayBuffer[String]()
    while (fileList.hasNext) {
      val path = fileList.next().getPath.toString
      fileArray.append(path)
    }
    logger.info("file list to copy from input path are [{}]", fileArray.toList)

    val rdd = sc.parallelize(fileArray, MAX_CONCURRENCES)
    rdd.foreachPartition(it => {
      val conf = new Configuration()
      val sfs = FileSystem.get(conf)

      while (it.hasNext) {
        val src = it.next()
        val dist = src.replace(input, distPath)
        FileUtil.copy(sfs, new Path(src), sfs, new Path(dist), false, conf)
      }
    })
  }

}
