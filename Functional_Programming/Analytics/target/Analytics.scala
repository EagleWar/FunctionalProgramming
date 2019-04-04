import java.util

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf





object Analytics extends App {
  val conf = new SparkConf()
                      .setAppName("Wordcount")
                      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

  val sc = SparkContext.getOrCreate(conf)
  sc.textFile("test.txt").map(x=>print(x))

}
