package org.apache.databricks.catalyst

import org.apache.spark.sql.SparkSessionExtensions

/**
 * @author Sam Ma
 * @date 2021/09/08
 * 创建自己的Extension，并在对其进行注入 (CustomRule)
 */
class CustomSessionExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule {
      session => CustomRule(session)
    }
  }

}
