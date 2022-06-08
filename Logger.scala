import java.io.OutputStream
import org.apache.spark.SparkContext
import scala.util.Properties

case class TechnicalException(private val message: String = "",
                              private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class FunctionalException(private val message: String = "",
                               private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

class Reporting(name: String) {
  private val initialTime = System.nanoTime()
  private var logs: Seq[String] = Seq() //scalastyle:ignore var.field
  private var parameters: Map[String, String] = Map() //scalastyle:ignore var.field
  private var sparkOptions: Map[String, String] = Map() //scalastyle:ignore var.field
  private var exceptions: Seq[String] = Seq() //scalastyle:ignore var.field
  private var sqlScriptGlobalStatus: String = ""
  private val lineSeparator = System.lineSeparator()

  def getTime: Double = BigDecimal((System.nanoTime() - initialTime)/1e9).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

  def addLog(log: String): Unit = logs :+= log

  def addParameter(parameter: String, value: String): Unit =
    parameters += (parameter -> value)

  def addParameters(parameters: Map[String, String]): Unit =
    parameters.foreach{case(parameter: String, value: String) => addParameter(parameter, value)}

  def addSparkOptions(sc: SparkContext): Unit = {
    sparkOptions += ("applicationId" -> sc.applicationId)
    sparkOptions += ("number of cores per executor" -> sc.getConf.get("spark.executor.cores", "-1"))
    sparkOptions += ("number of executors" -> sc.getConf.get("spark.executor.instances", "-1"))
    sparkOptions += ("executor memory" -> sc.getConf.get("spark.executor.memory", "-1"))
  }

  def addException(exception: Throwable): Unit = exception match {
    case _: TechnicalException => addExceptionMessage("Technical", exception)
    case _: FunctionalException => addExceptionMessage("Functionnal", exception)
    case _: Throwable => addExceptionMessage("Unhandled kind of", exception)
  }

  private def addExceptionMessage(category: String, exception: Throwable): Unit =
    exceptions :+= s"$category exception : ${exception.getMessage}"

  def addFlagTime(flagName: String): Unit = addLog(s"$flagName: ${getTime}seconds")

  def addIngestionCount(numberRows: Long, tableDestination: String, ingestionMode: Option[String] = None): Unit = ingestionMode match {
    case Some(mode) => addLog(s"Ingested table $tableDestination in mode $mode with $numberRows rows")
    case None => addLog(s"Ingested table $tableDestination with $numberRows rows")
  }

  def addExtractionCount(numberRows: Long, outputPath: String, extractionMode: Option[String] = None): Unit = extractionMode match {
    case Some(mode) => addLog(s"Extracted table $outputPath in mode $mode with $numberRows rows")
    case None => addLog(s"Extracted table $outputPath with $numberRows rows")
  }

  def addExportExceptionMessage(msg: String) : Unit = {
    addLog(s"EXPORT EXCEPTION: $lineSeparator $msg")
  }

  def setsqlScriptGlobalStatus(status: Boolean): Unit = {
    if (status) {
      sqlScriptGlobalStatus = "OK"
    } else {
      sqlScriptGlobalStatus = "KO"
    }
  }

  def saveLogs(os: OutputStream): Unit = {
    // Write parameters
    os.write(s"Parameters:${Properties.lineSeparator}".getBytes)
    os.write(parameters.map{case (key, value) => s"$key: $value"}.mkString(Properties.lineSeparator).getBytes)

    // Write sparkoptions
    os.write(Properties.lineSeparator.getBytes)
    os.write(Properties.lineSeparator.getBytes)
    os.write(s"SparkOptions:${Properties.lineSeparator}".getBytes)
    os.write(sparkOptions.map{case (key, value) => s"$key: $value"}.mkString(Properties.lineSeparator).getBytes)

    // Write sql script status
    if (!sqlScriptGlobalStatus.equals("")) {
      os.write(Properties.lineSeparator.getBytes)
      os.write(Properties.lineSeparator.getBytes)
      os.write(s"GLOBAL STATUS: $sqlScriptGlobalStatus".getBytes)
    }

    // Write logs
    os.write(Properties.lineSeparator.getBytes)
    os.write(Properties.lineSeparator.getBytes)
    os.write(s"Logs:${Properties.lineSeparator}".getBytes)
    os.write(logs.mkString(Properties.lineSeparator).getBytes)

    // Write Errors
    if(exceptions.nonEmpty) {
      os.write(Properties.lineSeparator.getBytes)
      os.write(Properties.lineSeparator.getBytes)
      os.write(s"Exceptions:${Properties.lineSeparator}".getBytes)
      os.write(exceptions.mkString(Properties.lineSeparator).getBytes)
    }
  }
}

def writeConfig(potentialPath: Option[String], hdfs: FileSystem, reporting: Reporting, idTech: String): Unit = potentialPath match {
  case None => log.info(Message("no reporting"))
  case Some(path) =>
    reporting.addFlagTime("Elapsed time")
    val newPath = s"$path/preparation_${idTech}_${System.currentTimeMillis}"
    val os = hdfs.create(new Path(newPath))
    reporting.saveLogs(os)
    os.close()
    log.info(Message(s"Reporting successfully written at $newPath"))
  }


//exemple

val reporting = new Reporting("Preparation")
reporting.addSparkOptions(spark.sparkContext)
val arguments = parseArguments(Map(), args.toList)
reporting.addParameters(arguments)
reporting.addIngestionCount(spark.sql(s"select * from $outputPath").count, outputPath)
reporting.addLog(s"No Raw Data!")

writeConfig(prop.get(reportingOutputPath), hdfs, reporting, idTech)