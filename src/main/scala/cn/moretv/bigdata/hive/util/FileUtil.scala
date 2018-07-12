package cn.moretv.bigdata.hive.util

import java.io.{File, FileWriter}

import scala.collection.mutable

object FileUtil {

  val fileMap = new mutable.HashMap[String, FileWriter]()
  private val lineSeparator = System.getProperty("line.separator")

  def writeFile(filename: String, line: String) = {
    val key = s"sql/$filename"
    val sqlDir = new File("sql")
    if(!sqlDir.exists()) sqlDir.mkdir()
    fileMap.get(key) match {
      case Some(out) => {
        out.write(line)
        out.write(lineSeparator)
        out.flush()
      }
      case None => {
        val out = new FileWriter(key,true)
        fileMap += key -> out
        out.write(line)
        out.write(lineSeparator)
        out.flush()
      }
    }
  }


  def destroy(): Unit = {
    fileMap.foreach(_._2.close())
  }
}
