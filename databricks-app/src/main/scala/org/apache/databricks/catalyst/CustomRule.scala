package org.apache.databricks.catalyst

import com.typesafe.scalalogging.Logger
import org.apache.databricks.InvertedIndex
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * @author Sam Ma
 * @date 2021/09/08
 * 自定义Spark SQL优化Rule，实现自定义优化逻辑
 */
case class CustomRule(spark: SparkSession) extends Rule[LogicalPlan] {

  private[this] val logger = Logger(InvertedIndex.getClass)

  /*
   * apply CustomReplaceDistinctRule to optimize spark sql
  21/09/15 22:59:10 WARN PlanChangeLogger:
  === Result of Batch Operator Optimization before Inferring Filters ===
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    /*case Distinct(child) => {
      logger.info("apply CustomReplaceDistinctRule to optimize spark sql")
      Aggregate(child.output, child.output, child)
    }*/
    case _ => println("apply CustomReplaceDistinctRule to optimize spark sql")
      plan
  }

}