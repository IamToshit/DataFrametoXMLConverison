package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import scala.reflect.io.Directory
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

object DF {
  def main(args: Array[String]): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate;
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val dt1 = spark.read.format("csv").option("header","true").load("C:\\Users\\hp\\Desktop\\emp_tbl1.txt").toDF

    val folder_to_download = "C:\\Folder_A"
    val xml_file_name = "person"

    var first_path = folder_to_download + "\\"  + xml_file_name

    println(first_path)

    dt1.repartition(1).write.format("xml").option("rowTag","NewTag").save(first_path)

    var sourceFilename = first_path + "\\"  + "part-00000"
    var destinationFilename = folder_to_download + "\\" + xml_file_name + ".xml"

    val path = Files.move(
      Paths.get(sourceFilename),
      Paths.get(destinationFilename),
      StandardCopyOption.REPLACE_EXISTING
    )
    if (path != null) {
      println(s"moved the file $sourceFilename successfully")
    } else {
      println(s"could NOT move the file $sourceFilename")
    }

    val directory = new Directory(new File(first_path))
    directory.deleteRecursively()
  }
}
