package cn.moretv.bigdata.hive.dao.impl

import cn.moretv.bigdata.hive.dao.HiveMetaDao
import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum
import cn.moretv.bigdata.hive.util.SqlUtil
import cn.moretv.bigdata.hive.vo.{HiveColumn, HivePartition, HiveTable}

import scala.collection.JavaConversions._

case class HiveMetaDaoImpl(env: EnvEnum) extends HiveMetaDao {

  val db = SqlUtil(env)
  val tableSql = "SELECT a.TBL_ID,a.TBL_NAME FROM TBLS a JOIN DBS b on a.DB_ID = b.DB_ID WHERE b.NAME = ? AND a.TBL_NAME LIKE ? "
  val allPartitionSql = "select part_name,location from PARTITIONS a join SDS b on a.sd_id = b.sd_id where a.tbl_id = ? order by part_name desc"
  val partitionSql = "select part_name,location from PARTITIONS a join SDS b on a.sd_id = b.sd_id where a.tbl_id = ? and part_name >= ? and part_name <= ? order by part_name desc"
  val lastPartitionSql = "select part_name,location from PARTITIONS a join SDS b on a.sd_id = b.sd_id where a.tbl_id = ? order by part_name desc limit 1"
  val columnSql = "select c.COLUMN_NAME,c.TYPE_NAME from TBLS a join SDS b on a.SD_ID = b.SD_ID join COLUMNS_V2 c on b.CD_ID = c.CD_ID where a.TBL_ID = ?"

  /**
    * 获取某个数据库下所有的表
    *
    * @param dbName       数据库名称
    * @param tablePattern 表模式（like语句表达式）
    * @return Hive表集合
    */
  override def getTables(dbName: String, tablePattern: String = "%"): List[HiveTable] = {
    val tableArray = db.selectArrayList(tableSql, dbName, tablePattern)
    tableArray.map(arr => {
      val tableId = arr(0).toString.toLong
      val tableName = arr(1).toString
      new HiveTable(dbName, tableName, tableId)
    }).toList
  }

  /**
    * 获取该表对应的所有partition
    *
    * @param tableId 表ID，唯一标识
    * @return 该表下所有的partition，以及对应的location
    */
  override def getAllPartitions(tableId: Long): List[HivePartition] = {
    val partArray = db.selectArrayList(allPartitionSql, tableId)
    partArray.map(arr => {
      val partName = arr(0).toString
      val location = arr(1).toString
      HivePartition(partName, location)
    }).toList
  }

  /**
    * 获取指定区间内的partition集合
    *
    * @param tableId        表ID，唯一标识
    * @param startPartition 开始partition，可以只有前半部分，如key_day=20171220/key_hour=10,可以只写key_day=20171220,但是必须从开头写起
    * @param endPartition   结束partition，规则同startPartition
    * @return 指定区间内的partition集合
    */
  override def getPartitions(tableId: Long, startPartition: String, endPartition: String) = {
    val partArray = db.selectArrayList(partitionSql, tableId, startPartition, endPartition)
    partArray.map(arr => {
      val partName = arr(0).toString
      val location = arr(1).toString
      HivePartition(partName, location)
    }).toList
  }

  /**
    * 获取表对应的所有column信息，不包含分区字段
    *
    * @param tableId 表ID，唯一标识
    * @return 表中所有的列名及类型，不包含分区字段
    */
  override def getColumns(tableId: Long):List[HiveColumn] = {
    val columnArray = db.selectArrayList(columnSql, tableId)
    columnArray.map(arr => {
      val columnName = arr(0).toString
      val columnType = arr(1).toString
      HiveColumn(columnName, columnType, false)
    }).toList
  }

  /**
    * 释放资源
    */
  override def destroy(): Unit = db.destory()

  /**
    * 获取最新的partition
    *
    * @param tableId 表ID，唯一标识
    * @return 返回最新的partition
    */
  override def getLastPartition(tableId: Long) = {
    val partArray = db.selectOne(lastPartitionSql, tableId)
    if (partArray.length > 0) {
      val partName = partArray(0).toString
      val location = partArray(1).toString
      HivePartition(partName, location)
    } else {
      null
    }
  }

  override def execute(sql: String) = {
    db.delete(sql)
  }

}
