package org.apache.databricks.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType

/**
 * @author Sam Ma
 * @date 2021/08/31
 * 实现RunnableCommand接口，实现自定义spark command
 */
case class ShowVersionCommand() extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sparkVersion = System.getenv("SPARK_VERSION")
    val javaVersion = System.getenv("JAVA_VERSION")
    Seq(Row(String.format("java version: %s, spark version: %s", javaVersion, sparkVersion)))
  }

}


