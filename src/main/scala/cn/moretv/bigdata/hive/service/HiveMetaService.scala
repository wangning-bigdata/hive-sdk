package cn.moretv.bigdata.hive.service

import cn.moretv.bigdata.hive.dao.HiveMetaDao
import cn.moretv.bigdata.hive.dao.impl.HiveMetaDaoImpl
import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum
import cn.moretv.bigdata.hive.vo.{HiveColumn, HivePartition, HiveTable}

import scala.collection.mutable.ListBuffer

case class HiveMetaService(env: EnvEnum) {

  val hiveMetaDao: HiveMetaDao = HiveMetaDaoImpl(env)


  /**
    * 获取特定数据库下符合表模式的所有表
    * @param dbName 数据库名
    * @param tablePattern 表模式
    * @return hive表集合
    */
  def getTables(dbName: String, tablePattern: String = "%"): Seq[HiveTable] = {
    hiveMetaDao.getTables(dbName,tablePattern)
  }

  /**
    * 获取指定hive表的所有分区
    * @param dbName 数据库名称
    * @param tableName 表名
    * @return 分区信息集合
    */
  def getAllPartitions(dbName: String, tableName: String): Map[String, Map[String, String]] ={
    val tables = hiveMetaDao.getTables(dbName, tableName)
    val tableId = tables.head.tableId
    val partitions = hiveMetaDao.getAllPartitions(tableId)
    partitions.map(hp => {
      val location = hp.location
      val partMap = hp.partName.split("/").map(kv => {
        val kvs = kv.split("=")
        (kvs(0),kvs(1))
      }).toMap
      location -> partMap
    }).toMap
  }

  /**
    * 获取字段列表，不包含分区字段
    * @param dbName 数据库名称
    * @param tableName 表名
    * @return
    */
  def getColumns(dbName: String, tableName: String): List[HiveColumn] = {
    val tables = hiveMetaDao.getTables(dbName, tableName)
    val tableId = tables.head.tableId
    hiveMetaDao.getColumns(tableId)
  }
  /**
    * 获取已合并可执行的Hive drop & add SQL
    *
    * @param dbName       数据库名
    * @param tablePattern 表模式，默认为所有表
    * @return 已合并可执行的Hive drop & add partition SQL
    */
  def getAllPartitionsSql(dbName: String, tablePattern: String = "%"): Map[String, (List[String], List[String])] = {
    val tables = hiveMetaDao.getTables(dbName, tablePattern)
    tables.map(table => {
      val tableName = table.tableName
      val hivePartitions = hiveMetaDao.getAllPartitions(table.tableId)
      if (hivePartitions.nonEmpty) {
        tableName -> mergeSql(table, hivePartitions)
      } else {
        tableName -> (Nil, Nil)
      }
    }).toMap
  }

  /**
    * 获取指定分区区间的已合并可执行的Hive drop & add partition SQL
    *
    * @param dbName         数据库名
    * @param tablePattern   表模式，默认为所有表
    * @param startPartition 分区起始
    * @param endPartition   分区结束
    * @return 已合并可执行的Hive drop & add partition SQL
    */
  def getPartitionsSql(dbName: String, tablePattern: String = "%", startPartition: String, endPartition: String): Map[String, (List[String], List[String])] = {
    val tables = hiveMetaDao.getTables(dbName, tablePattern)
    tables.map(table => {
      val tableName = table.tableName
      val hivePartitions = hiveMetaDao.getPartitions(table.tableId, startPartition, endPartition)
      tableName -> mergeSql(table, hivePartitions)
    }).toMap
  }

  def getLastPartitionQueryAllSql(dbName: String, tablePattern: String = "%"): Seq[String] = {
    val tables = hiveMetaDao.getTables(dbName, tablePattern)
    tables.map(table => {
      val hivePartition = hiveMetaDao.getLastPartition(table.tableId)
      if (hivePartition != null) {
        val partName = hivePartition.partName
        val partKeys = partName.split("/").map(p => {
          val kv = p.split("=")
          s"${kv(0)} = '${kv(1)}'"
        })

        s"select * from `$dbName`.`${table.tableName}` where ${partKeys.mkString(" and ")} limit 1"
      } else {
        ""
      }
    })
  }

  /**
    * 合并drop和add partition的SQL，提高执行效率
    *
    * @param table          hiveTable对象
    * @param hivePartitions partition列表
    * @return 合并后的drop和add partition语句
    */
  private def mergeSql(table: HiveTable, hivePartitions: Seq[HivePartition]): (List[String], List[String]) = {
    val dbName = table.dbName
    val tableName = table.tableName
    val partList = hivePartitions.map(part => {
      val partName = part.partName
      val location = part.location
      val partKeys = partName.split("/").map(p => {
        val kv = p.split("=")
        s"${kv(0)}='${kv(1)}'"
      })

      val dropSqls = partKeys.map(p => s"ALTER TABLE $dbName.${table.tableName} DROP PARTITION ($p) ")
      val addSql = s"PARTITION (${partKeys.mkString(",")}) LOCATION '$location'"
      (dropSqls, addSql)
    })
    //以下三行为，为了合并重复的drop partition的语句，并获得数量最少的SQL集合
    val dropSqls = partList.map(_._1)
    val minLength = dropSqls.map(_.length).min
    val dropSqlList = (0 until minLength).map(i => {
      dropSqls.map(arr => arr(i)).distinct
    }).map(s => (s.size, s)).minBy(_._1)._2.toList.sortBy(x => x)

    //以下部分为合并add partition 提升执行效率
    val addSqlList = partList.map(_._2).toList

    val size = addSqlList.size
    //SQL批处理数量
    val batchNum = 100
    val minAddSqlList = if (size <= batchNum) {
      List(s"ALTER TABLE ods_view.$tableName add if not exists ${addSqlList.mkString(" ")}")
    } else {
      val listBuffer = new ListBuffer[String]()

      val num = if (size % batchNum == 0) size / batchNum else size / batchNum + 1
      var (part1, part2) = addSqlList.splitAt(batchNum)
      listBuffer += s"ALTER TABLE ods_view.$tableName add if not exists ${part1.mkString(" ")}"
      (1 until num).map(i => {
        val twoList = part2.splitAt(batchNum)
        part1 = twoList._1
        part2 = twoList._2
        listBuffer += s"ALTER TABLE ods_view.$tableName add if not exists ${part1.mkString(" ")}"
      })
      listBuffer.toList
    }
    (dropSqlList, minAddSqlList)
  }

  def getDropColumnSql(dbName: String, tableName: String, columns: Seq[String]): String = {
    val tableId = hiveMetaDao.getTables(dbName, tableName).head.tableId
    val hiveColumns = hiveMetaDao.getColumns(tableId)
    val finalColumns = hiveColumns.filter(hc => {
      !columns.contains(hc.columnName)
    })
    val columnSql = finalColumns.map(c => s"${c.columnName} ${c.columnType}").mkString(" , ")
    s"ALTER TABLE `$dbName`.`$tableName` replace columns ($columnSql)"
  }


}
