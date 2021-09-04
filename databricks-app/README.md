## Spark SQL
> 添加简单SQL命令到spark SQL、观察执行不同的SQL时候，引擎基于Rule规则会做哪些优化，以及如何实现自定义的优化规则。

### 1. 为Spark SQL添加一条自定义的命令，显示当前Spark版本和Java版本
```shell
spark sql> SHOW VERSION;
java version: 1.8, spark version: 2.4.5
```

是基于`spark 2.4.5`版本添加命令的，首先在`SqlBase.g4`中加入了新命令`SHOW VERSION`

```antlr
| SHOW VERSION                                                     #showVersion
// SHOW已经为关键字，将VERSION添加到`nonReserved`中
nonReserved
| VERSION
// 在SqlBase.g4最后加入`VERSION`的对应关系
VERSION: 'VERSION';
```

编译过`SqlBase.g4`后，在`SparkSqlParser(conf: SQLConf)`中实现`showVersion`方法：

```scala
/**
 * Create a [[SetDatabaseCommand]] logical plan.
 */
override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
  ShowVersionCommand()
}
```

继承`RunnableCommand`实现`SHOW VERSION`命令逻辑，也即从`classpath`中获取`spark`和`java`的版本

```scala
case class ShowVersionCommand() extends RunnableCommand {
  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sparkVersion = System.getenv("SPARK_VERSION")
    val javaVersion = System.getenv("JAVA_VERSION")
    Seq(Row(String.format("java version: %s, spark version: %s", javaVersion, sparkVersion)))
  }
}
```

用`mvn -Phive -Dhive-thriftserver -DskipTests clean package `命令编译源代码，之后执行如下命令启动`spark-sql`：

```shell
apache-spark-2.4.5 % JAVA_VERSION=1.8 SPARK_VERSION=2.4.5 spark-sql -S
```

执行结果如下所示：
<img src="resources/spark_sql_show_version.jpg" width="870" alt="show version执行结果"/>
