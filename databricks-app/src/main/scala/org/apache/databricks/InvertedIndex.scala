package org.apache.databricks

import com.typesafe.scalalogging.Logger
import org.apache.commons.configuration.{PropertiesConfiguration => HierConf}
import org.apache.spark.rdd.RDD
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
    val wordAggregate: RDD[(String, Map[String, Int])] = sparkContext.textFile(inputFile).map(line => line.split("\t"))
      .map(phases => (phases(0), phases(1)))
      // 将(fname, "it is what it is")元组映射为[("it", (fname->1)), ("is", (fname->1)), ("what", (fname->1)), ("is", (fname->1))]
      .map(tuple => tuple._2.split(" ").map(word => (word, Map(tuple._1 -> 1))))
      // RDD[Array[(String, String)]]用flatmap拍平为RDD[("it", (fname->1))]
      .flatMap(element => element)
      // 针对相同reduce key，若在map中存在相同key，则累加key对应的value值
      .reduceByKey((map1, map2) => {
        val mergeMap = map1 ++ map2.map {
          case(k, v) => k -> (map1.getOrElse(k, 0) + map2.getOrElse(k, 0))
        }
        mergeMap
      })
    logger.info("use rdd api to aggregate words, result: {}", wordAggregate)
    sparkContext.stop()
  }

}
