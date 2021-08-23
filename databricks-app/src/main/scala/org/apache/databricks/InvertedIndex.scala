package org.apache.databricks

import com.typesafe.scalalogging.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Sam Ma
 * @date 2021/08/24
 * 使用RDD API实现带词频的倒排索引，数据格式<word, [(doc1,frequency), (doc2,frequency)]>
 */
object InvertedIndex {

  private[this] val logger = Logger(InvertedIndex.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("invertedIndex")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.ui.port", "4701")
    val sparkContext = new SparkContext(conf)

//    val cfg = new HierConf()
  }

}
