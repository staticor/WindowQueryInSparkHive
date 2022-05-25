

import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

// 示例代码， 使用spark 的 lag和leads 算子
object FooQuery {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("official doc1")

    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window

    val frame: DataFrame = spark.read.format("csv")
      .csv("data/dates.csv")

    val schemas = Seq("date1")
    val frame1 = frame.toDF(schemas: _*)

    frame1.createTempView("foo")

    // Create a windowSpec
    val wSpec: WindowSpec = Window.orderBy("date1")
    val foo: DataFrame = spark.sql("select * from foo")
    foo.show()
    foo.withColumn("previous",lag(foo("date1"),1)
      .over(wSpec))
      .withColumn("next",lead(foo("date1"),1)
      .over(wSpec)).select("previous","date1","next").show



    spark.close()
  }
}