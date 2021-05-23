
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, explode}


object test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("AreaClickApp")
      .enableHiveSupport()
      .getOrCreate()


    val list1 = List(Array("4","2"),Array("130","5"),Array("77","88"))

    val rdd1 = spark.sparkContext.makeRDD(list1)
    import spark.implicits._


    // 映射出来一个 RDD[Row], 因为 DataFrame其实就是 DataSet[Row]
    val rowRdd: RDD[(Seq[String], Seq[String])] = rdd1.map(x => (Seq(x(0),x(0)+1),Seq(x(1)+3,x(1))))
      val df2: DataFrame = rowRdd.toDF("out_DEVICE","out_DEVICE_ltm")
      df2.show()

 /*   val rdd2: RDD[Row] = spark.sparkContext.makeRDD( Seq(Row((1,2),(3,4)),Row((11,22),(33,44))))
    val rdd3: RDD[Row] = spark.sparkContext.makeRDD( Seq(Row(1,2),Row(3,4)))

    val df1: DataFrame = spark.createDataFrame(rowRdd,
      types.StructType(Array(StructField("out_DEVICE", types.DataType),
        StructField("out_DEVICE_ltm", ArrayType))))
    df1.show()*/

    spark.udf.register("zip_cols", (c1: Seq[String], c2: Seq[String]) => {
      if (null == c1 || null == c2) { // 因为是left join得到的device,所以会出现为null的情况
        Seq(("", ""))
      } else {
        c1.zip(c2) // Seq[(String, String)]
      }
    })

    val df: DataFrame = df2.withColumn("zip_cols", explode(callUDF("zip_cols",
      col("out_DEVICE"), col("out_DEVICE_ltm"))))
    df.show()
    val column: Column = df("zip_cols._1").as("sss")




    println("**********df排序******")
    val ord: Ordering[(Int, Int)] = Ordering.Tuple2(Ordering.Int, Ordering.Int.reverse)
    println(List((10, 6), (19, 44), (6, 99),(10,33)).sorted(ord).slice(0,1))

    //////////////////////
    val value: Int = 3/2
    println(value)

    val bool: Boolean = "qwemd5_balala".contains("md5")
    println(bool)

    println("************explode******************")
    import spark.implicits. _
    val df_tmp: DataFrame = List(Array(1,2),Array(2,3)).toDF("AA")
    df_tmp.show()
    df_tmp.select(explode(col("AA"))).show()

  }

}
