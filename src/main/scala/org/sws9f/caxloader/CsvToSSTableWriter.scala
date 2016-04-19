package org.sws9f.caxloader

import org.apache.cassandra.io.sstable.CQLSSTableWriter
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.github.tototoshi.csv._
import java.time.LocalDate
import scalax.file.Path
import org.slf4j.{Logger,LoggerFactory}

//object CaxSSTableWriterTranx extends App with LazyLogging {
object CsvToSSTableWriter extends App {
  val log = LoggerFactory.getLogger(this.getClass)
  
  case class Config(keyspace:String = "", table:String="", columns:Seq[(String,String)] = Seq(), pk:String="", csv:String="", output:String="", dryRun:Boolean=false)
  
  val parser = new scopt.OptionParser[Config]("scopt") {
    head("scopt", "1.0")
    opt[String]('k', "keyspace") required() action { (x,c) =>
      c.copy(keyspace = x) } text("cassandra keyspace name")
    opt[String]('t', "table") required() action { (x,c) =>
      c.copy(table = x) } text("cassandra table name")
    opt[String]('p', "primary-key") required() action { (x,c) =>
      c.copy(pk = x) } text("cassandra table primary key")
    opt[String]('f', "file-csv") required() action { (x,c) =>
      c.copy(csv = x) } text("source csv file name")
    opt[String]('o', "output") required() action { (x,c) =>
      c.copy(output = x) } text("output folder path")
    opt[Boolean]('d', "dry-run") action { (x,c) =>
      c.copy(dryRun = x) } text("dry run, only output CREATE TABLE/INSERT ROW scripts")
    opt[Seq[(String,String)]]('c', "columns") required() action { (x,c) =>
      c.copy(columns = x) } text("columns name & data-type")
  }
  
  val cfgOption = parser.parse(args, Config())
  if (cfgOption == None) sys.exit(1)
  val cfg = cfgOption.get
  
  val scriptCreateTable = getScriptCreateTable(cfg)
  log.info(s"Create table script:\n$scriptCreateTable")
  val scriptInsert = getScriptInsert(cfg)
  log.info(s"Insert row script:\n$scriptInsert")
  
  if (cfg.dryRun) sys.exit(0)
  resetFolder(cfg.output)  
  doInsertSstable(cfg)
  log.info("End.")
  
  def doInsertSstable(cfg:Config) = {
    val writer: CQLSSTableWriter = CQLSSTableWriter.builder()
                                                  .inDirectory(cfg.output)
                                                  .forTable(scriptCreateTable)
                                                  .using(scriptInsert).build();
    val epochMiddle = scala.math.pow(2,31).toInt + 1
    val reader = CSVReader.open(cfg.csv)
    
    try {
      var line = reader.readNext()
      var lineCount = 0L
      val colsIndex = cfg.columns.zipWithIndex
      while(line != None) {
        val colValues = for((x,i) <- colsIndex) yield x match {
          case c if c._2 == "varchar" => line.get(i).asInstanceOf[Object]
          case c if c._2 == "int"     => line.get(i).toInt.asInstanceOf[Object]
          case c if c._2 == "double"  => line.get(i).toDouble.asInstanceOf[Object]
          case c if c._2 == "date"    => (LocalDate.parse(line.get(i)).toEpochDay().toInt + epochMiddle).asInstanceOf[Object]
        }
      
        if (lineCount % 100000 == 0) log.info(f"Inserting row $lineCount%,9d : ${colValues}")
        writer.addRow(colValues.asJava)
        line = reader.readNext()
        lineCount += 1
      }
    } catch {
      case e:Exception => log.error(s"$e")
    } finally {
      writer.close
      reader.close
    }
    
  }
  
  def getScriptCreateTable(cfg:Config) = {
    """CREATE TABLE [ks].[table] ( [columns]
    |  PRIMARY KEY ([pk])
    |);""".stripMargin
    .replace("[columns]", cfg.columns.foldLeft("")( (x,y) => s"$x${y._1} ${y._2}, " ) )
    .replace("[ks]", cfg.keyspace)
    .replace("[table]", cfg.table)
    .replace("[pk]", cfg.pk)
  }
  
  def getScriptInsert(cfg:Config) = {
    "INSERT INTO [ks].[table] ([columns]) VALUES ([qmarks]);"
    .replace("[ks]", cfg.keyspace)
    .replace("[table]", cfg.table)
    .replace("[columns]", cfg.columns.foldLeft("")( (x,y) => if (x.length > 0) s"$x, ${y._1}" else y._1 ) )
    .replace("[qmarks]", cfg.columns.foldLeft("")( (x,y) => if (x.length > 0) s"$x, ?" else "?" ) )
  }
  
  def resetFolder(path: String) {
    val destPath: Path = Path.fromString(path)
    if (destPath.exists) {
      log.info("Folder %s exists, deleting it.".format(path))
      try {
        destPath.deleteRecursively(continueOnFailure = false)
      } catch {
        case e: java.io.IOException => log.error("cannot delete file: " + e)
      }
    }
    log.info(s"Create output folder: $path")
    destPath.createDirectory()
  }
}

