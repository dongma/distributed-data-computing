package org.apache.databricks

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Sam Ma
 * @date 2021/08/24
 * 使用RDD API实现带词频的倒排索引，数据格式<word, [(doc1,frequency), (doc2,frequency)]>
 */
object InvertedIndex {

  private[this] val logger = Logger(InvertedIndex.getClass)

  /*
   * => 数据文件内容 (文件列表有：0、1、2):
   * 0. it is what it is
   * 1. what is it
   * 2. it is a banana
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("invertedIndex")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listFiles(new Path("/tmp/input"), true)

    // wordRdd中数据：[(0, "it"), (1, "what"), (2, "banana")]
    var wordRdd = sc.emptyRDD[(String, String)]
    while (fileList.hasNext) {
      val path = fileList.next
      wordRdd = wordRdd.union(sc.textFile(path.getPath.toString)
        .flatMap(_.split("\\s+"))
        .map((path.getPath.getName, _)))
    }

    // 按单词、文件进行累加，[((0, "it"), 2), ((0, "is"), 2), ((0, "what"), 1)]
    val countRdd = wordRdd.map((_, 1)).reduceByKey(_ + _)
    val invertedRdd = countRdd.map(data => (data._1._2, String.format("(%s, %s)", data._1._1, data._2.toString)))
      // [("it", (0, 2)), ("is", (0, 2)), ("is", (2, 1))] => [("it", (0, 2)), ("is", (0, 2),(2, 1))]
      .reduceByKey(_ + "," + _)
      // [("is", (0, 2),(2, 1))] => [("is"), {(0, 2),(2, 1)}]
      .map(tuple => String.format("\"%s\": {%s}", tuple._1, tuple._2))

    /* 21/09/04 10:04:16 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.1.13, 52312, None)
    21/09/04 10:04:16 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.1.13, 52312, None)
    "what": {(1, 1),(0, 1)}
    "banana": {(2, 1)}
    "it": {(1, 1),(0, 2),(2, 1)}
    "is": {(2, 1),(0, 2),(1, 1)}
    "a": {(2, 1)}
     */
    invertedRdd.foreach(println)
    sc.stop()
  }

}
