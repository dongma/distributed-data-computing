package org.apache.databricks

import com.typesafe.scalalogging.Logger
import org.apache.commons.configuration.{PropertiesConfiguration => HierConf}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Sam Ma
 * @date 2021/08/24
 * 使用RDD API实现带词频的倒排索引，数据格式<word, [(doc1,frequency), (doc2,frequency)]>
 */
object InvertedIndex {

  private[this] val logger = Logger(InvertedIndex.getClass)

  /*
   * => 数据文件内容:
   * 0  it is what it is
   * 1  what is it
   * 2  it is a banana
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("invertedIndex")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.ui.port", "4701")
    val sparkContext = new SparkContext(conf)
    logger.info("command line args {} when start spark app", args)

    val cfg = new HierConf(args(0))
    val inputFile = cfg.getString("inputfile")
    sparkContext.textFile(inputFile).map(line => line.split("\t"))
      .map(phases => (phases(0), phases(1)))
      // 将(fname, "it is what it is")元组map为 [("it", fname), ("is", fname), ("what", fname), ("is", fname)]
      .map(tuple => tuple._2.split(" ").map(word => (word, tuple._1)))
      // 将RDD[Array[(String, String)]]通过flatmap拍平为RDD[("it", fname)]
      .flatMap(element => element)
      //
      .reduceByKey((fname, file) => {

      })
    sparkContext.stop()
  }

}
