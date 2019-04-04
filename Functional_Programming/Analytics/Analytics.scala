import java.util
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext



object Analytics extends App {
  /*
  Command to do not display the logs
  */
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf()
                      .setAppName("Wordcount")
                      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.


  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  //Read our File in HDFS to do Analysis
  //val textFile = sc.textFile("test.txt")

  val textFilebis = sqlContext.read.parquet ("../Hadoop")

  val textFile = textFilebis.drop(textFilebis.col("_1")).rdd.map(x => x.mkString(","))



  val latitude = 1 // position of latitude in the text file
  val battery = 3 // position of battery in the text file
  val temperature = 4 // position of temperature in the text file
  val id = 0 // postion of id in data stored

  /*
  In proportion to the whole number of devices is there more failing devices
  in the north hemisphere or the south hemisphere
  */
  val statsFailingHemisphere = textFile.map(x=>{
    val split = x.split(",")
    if(split(latitude).toDouble> 0.0 && split(battery).toFloat == 0.0 ) {
      ("Hemisphere_nord",1)
    }
    else if(split(latitude).toDouble < 0.0 && split(battery).toFloat == 0.0 ){
      ("Hemisphere_sud",1)
    }
    else{
      ("Hemisphere_sud",0)
    }
  }).reduceByKey((accum, n) => (accum + n))
  val maxFailHemis = statsFailingHemisphere.reduce((x, y) => if(x._2 > y._2) x else y)

  /*
  Is there more failing devices when the weather
  is hot or when the weather is called
  */
  val statsFailingTemperature = textFile.map(x=>{
    val split = x.split(",")
    if(split(temperature).toDouble < 0.0 && split(battery).toFloat == 0.0 ) {
      ("Cold",1)
    }
    else if(split(temperature).toDouble > 0.0 && split(battery).toFloat == 0.0 ){
      ("Hot",1)
    }
    else{
      ("Cold",0)
    }
  }).reduceByKey((accum, n) => (accum + n))
  val maxFailTemp = statsFailingTemperature.reduce((x, y) => if(x._2 > y._2) x else y)

  /*
  Among the failing devices which percentage fails beceause of low battery?
  */
  val statsFailingBattery = textFile.map(x => {
    val split = x.split(",")
    if(split(battery).toFloat == 0.0 ) {
      ("low_battery",1)
    }
    else{
      ("low_battery",0)
    }
  }).reduceByKey((accum, n) => (accum + n))
  val pourcLowBat = statsFailingBattery.map(x => x._2.toFloat/textFile.count * 100)

  /*
  number of failling per drone
  */
  val droneFailling = textFile.map(x => {
    val split = x.split(",")
    if(split(battery).toFloat == 0){
      val temp = split(id)
      (s"id:$temp", 1)
    }
    else {
      val temp = split(id)
      (s"id:$temp", 0)
    }
  }).reduceByKey((accum, n) => (accum + n))

  //Displaying the statistics that we have calculated
  println("---------------------Spark Analytics with RDDs-------------------")
  println("----------------------------Question 1----------------------")
  println("In proportion to the whole number of devices is there more failing devices")
  println("in the north hemisphere or the south hemisphere : ")
  statsFailingHemisphere.foreach(println)
  println(s"Answer : $maxFailHemis")

  println("----------------------------Question 2----------------------")
  println("Is there more failing devices when the weather")
  println("is hot or when the weather is cold : ")
  statsFailingTemperature.foreach(println)
  println(s"Answer : $maxFailTemp")

  println("----------------------------Question 3----------------------")
  println("Among the failing devices which percentage fails beceause of low battery :")
  statsFailingBattery.foreach(println)
  print("Answer : ")
  pourcLowBat.foreach(print)
  println(" %")

  println("----------------------------Question 4----------------------")
  println("Number of failling per drone : ")
  droneFailling.foreach(println)

  //Stop Spark SparkContext
  sc.stop()
}
