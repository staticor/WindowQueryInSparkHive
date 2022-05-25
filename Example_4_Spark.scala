 
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions._
object Example_4_Spark {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]")

    val spark: SparkSession = SparkSession.builder().config(conf)
      .appName("CsvExample")
      .master("local").getOrCreate()


    import spark.implicits._
    val scoreDF = spark.sparkContext.makeRDD(Array( Score("a", 1, 80),
      Score("b", 1, 78),
      Score("c", 1, 95),
      Score("d", 2, 74),
      Score("e", 2, 92),
      Score("f", 3, 99),
      Score("g", 3, 99),
      Score("h", 3, 45),
      Score("i", 3, 55),
      Score("j", 3, 78))).toDF("name","class","score")
    scoreDF.createOrReplaceTempView("score")
    scoreDF.show()

    println("//***************  求每个班最高成绩学生的信息  ***************/")
    println("    /*******  开窗函数的表  ********/")
    spark.sql("select name,class,score, rank() over(partition by class order by score desc) rank from score").show()

    println("    /*******  计算结果的表  *******")
    spark.sql(
      """
        |select  name, class, score, rk
        |from (
        |   select name, class, score, rank() over(partition by class order by score desc) rk
        |   from score
        |) mid where rk = 1
        |""".stripMargin).show()
  }
}

case class Score(name: String, group: Int, score: Int)