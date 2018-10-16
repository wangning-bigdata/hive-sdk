package cn.moretv.bigdata.hive.dao

import cn.moretv.bigdata.hive.vo.{HiveColumn, HivePartition, HiveTable}

/**
  * 从HiveMetaStore中获取表的基础信息，包括表分区、表字段等
  */
trait HiveMetaDao {

  /**
    * 获取某个数据库下所有的表
    *
    * @param dbName       数据库名称
    * @param tablePattern 表模式（like语句表达式）
    * @return Hive表集合
    */
  def getTables(dbName: String, tablePattern: String): Seq[HiveTable]

  /**
    * 获取该表对应的所有partition
    *
    * @param tableId 表ID，唯一标识
    * @return 该表下所有的partition，以及对应的location
    */
  def getAllPartitions(tableId: Long): Seq[HivePartition]

  /**
    * 获取最新的partition
    *
    * @param tableId 表ID，唯一标识
    * @return 返回最新的partition
    */
  def getLastPartition(tableId: Long): HivePartition

  /**
    * 获取指定区间内的partition集合
    *
    * @param tableId        表ID，唯一标识
    * @param startPartition 开始partition，可以只有前半部分，如key_day=20171220/key_hour=10,可以只写key_day=20171220,但是必须从开头写起
    * @param endPartition   结束partition，规则同startPartition
    * @return 指定区间内的partition集合
    */
  def getPartitions(tableId: Long, startPartition: String, endPartition: String): Seq[HivePartition]

  /**
    * 获取表对应的所有column信息，不包含分区字段
    *
    * @param tableId 表ID，唯一标识
    * @return 表中所有的列名及类型，不包含分区字段
    */
  def getColumns(tableId: Long): List[HiveColumn]

  /**
    * 释放资源
    */
  def destroy(): Unit

  /**
    * 执行SQL
    *
    * @param sql 待执行SQL
    * @return 影响的行数
    */
  def execute(sql: String): Int

  /**
    * 获取表对应的CD_ID
    *
    * @param tableId 表ID
    */
  def queryTableCdid(tableId: Long): Long

  /**
    * 查询各分区的SD_ID
    *
    * @param tableId 表ID
    * @return 各分区的SD_ID集合
    */
  def queryPartitionSdid(tableId: Long): List[Long]

  /**
    * 更新CD_ID
    * @param cdid CD_ID
    * @param sdid
    * @return
    */
  def updatePartitionCdid(cdid: Long, sdid: List[Long]): Int
}
