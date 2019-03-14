package cn.moretv.bigdata.hive

import java.sql.ResultSet

import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum
import cn.moretv.bigdata.hive.service.{HiveMetaService, HiveSqlService, SparkSqlService}
import cn.moretv.bigdata.hive.vo.{HiveColumn, HiveTable}
import org.slf4j.LoggerFactory

/**
  * 与hive交互的各种操作的交互类
  *
  * @author lian.kai
  * @since 2018-07-10
  */
case class HiveSdk(env: EnvEnum) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val hiveMetaService = HiveMetaService(env)
  private val hiveSqlService = HiveSqlService(env)
  private val sparkSqlService = SparkSqlService(env)

  /**
    * 判断表是否存在
    *
    * @param dbName    数据库名
    * @param tableName 表名
    * @return true 存在，false 不存在
    */
  def exists(dbName: String, tableName: String): Boolean = {
    hiveMetaService.getTables(dbName, tableName).nonEmpty
  }

  /**
    * 判断表是否存在
    *
    * @param dbName    数据库名
    * @param tablePattern 表名模式
    * @return 匹配到的表名集合
    */
  def getTables(dbName: String, tablePattern: String = "%"): List[String] = {
    hiveMetaService.getTables(dbName, tablePattern).map(_.tableName).toList
  }

  /**
    * 创建hive表
    *
    * @param hiveTable      hive表
    * @param hiveColumnList 字段列表
    */
  def createTable(hiveTable: HiveTable, hiveColumnList: Seq[HiveColumn]): Unit = {
    val createTableSql = HiveTable.getCreateTableSql(hiveTable, hiveColumnList)
    hiveSqlService.execute(createTableSql)
  }

  /**
    * 添加一个或多个字段
    *
    * @param dbName         数据库名
    * @param tableName      表名
    * @param hiveColumnList 字段列表
    */
  def addColumns(dbName: String, tableName: String, hiveColumnList: Seq[HiveColumn]): Unit = {
    val hiveTable = new HiveTable(dbName, tableName)
    val addColumnSql = HiveTable.getAddColumnSql(hiveTable, hiveColumnList)
    hiveSqlService.execute(addColumnSql)
  }

  /**
    * 改变字段名称或类型
    *
    * @param dbName        数据库名
    * @param tableName     表名
    * @param hiveColumnMap 字段列表，Map[旧字段名，新字段信息]
    */
  def changeColumns(dbName: String, tableName: String, hiveColumnMap: Map[String, HiveColumn]): Unit = {
    val hiveTable = new HiveTable(dbName, tableName)
    val changeColumns = HiveTable.getChangeColumnSql(hiveTable, hiveColumnMap)
    changeColumns.foreach(hiveSqlService.execute)
  }

  /**
    * 替换hive表schema
    *
    * @param dbName         数据库名
    * @param tableName      表名
    * @param hiveColumnList 字段列表
    */
  def replaceColumns(dbName: String, tableName: String, hiveColumnList: Seq[HiveColumn]): Unit = {
    val hiveTable = new HiveTable(dbName, tableName)
    val replaceColumnSql = HiveTable.getReplaceColumnSql(hiveTable, hiveColumnList)
    hiveSqlService.execute(replaceColumnSql)
  }

  /**
    * 删除指定字段
    *
    * @param dbName    数据库名
    * @param tableName 表名
    * @param columns   字段列表
    */
  def dropColumns(dbName: String, tableName: String, columns: Seq[String]): Unit = {
    val sql = hiveMetaService.getDropColumnSql(dbName, tableName, columns)
    hiveSqlService.execute(sql)
  }

  /**
    * 获取指定表的所有分区信息
    *
    * @param dbName    数据库名
    * @param tableName 表名
    * @return 分区信息集合 Map[数据存储路径, Map[分区字段名，分区字段值] ]
    */
  def getAllPartitions(dbName: String, tableName: String): Map[String, Map[String, String]] = {
    hiveMetaService.getAllPartitions(dbName, tableName)
  }

  /**
    * 获取指定表的所有字段信息
    *
    * @param dbName    数据库名
    * @param tableName 表名
    * @return 字段信息集合
    */
  def getColumns(dbName: String, tableName: String): List[HiveColumn] = {
    hiveMetaService.getColumns(dbName, tableName)
  }

  /**
    * 添加单个分区
    *
    * @param dbName       数据库名
    * @param tableName    表名
    * @param partitionMap 分区字段和值
    * @param location     存储位置
    */
  def addPartition(dbName: String, tableName: String, partitionMap: Map[String, String], location: String): Unit = {
    val partitionPartSql = partitionMap.map(kv => {
      val (key, value) = kv
      s"$key='$value'"
    }).mkString(", ")

    val addPartitionSql = s"ALTER TABLE $dbName.$tableName ADD IF NOT EXISTS PARTITION($partitionPartSql) LOCATION '$location'"
    hiveSqlService.execute(addPartitionSql)
  }

  /**
    * 添加单个分区，先删除分区再添加分区
    *
    * @param dbName       数据库名
    * @param tableName    表名
    * @param partitionMap 分区字段和值
    * @param location     存储位置
    */
  def addPartitionAfterDrop(dbName: String, tableName: String, partitionMap: Map[String, String], location: String): Unit = {
    dropPartition(dbName, tableName, partitionMap)
    addPartition(dbName, tableName, partitionMap, location)
  }

  /**
    * 添加多个分区
    *
    * @param dbName           数据库名
    * @param tableName        表名
    * @param partitionInfoMap 多个分区的信息集合，key为location，value为分区字段和值的集合
    */
  def addPartitions(dbName: String, tableName: String, partitionInfoMap: Map[String, Map[String, String]]): Unit = {
    val partSql = partitionInfoMap.map(e => {
      val location = e._1
      val partMap = e._2
      val sql = partMap.map(kv => {
        val (key, value) = kv
        s"$key='$value'"
      }).mkString(", ")
      s"PARTITION($sql) LOCATION '$location'"
    }).mkString(" ")
    val addPartitionSql = s"ALTER TABLE $dbName.$tableName ADD IF NOT EXISTS $partSql"
    hiveSqlService.execute(addPartitionSql)
  }

  /**
    * 添加多个分区，先删除分区，再添加分区
    *
    * @param dbName           数据库名
    * @param tableName        表名
    * @param partitionInfoMap 多个分区的信息集合，key为location，value为分区字段和值的集合
    */
  def addPartitionsAfterDrop(dbName: String, tableName: String, partitionInfoMap: Map[String, Map[String, String]]): Unit = {
    partitionInfoMap.foreach(e => {
      val partMap = e._2
      dropPartition(dbName, tableName, partMap)
    })
    addPartitions(dbName, tableName, partitionInfoMap)
  }

  /**
    * 删除分区
    *
    * @param dbName       数据库名称
    * @param tableName    表名
    * @param partitionMap 分区字段和值集合
    */
  def dropPartition(dbName: String, tableName: String, partitionMap: Map[String, String]): Unit = {
    val partitionPartSql = partitionMap.map(kv => {
      val (key, value) = kv
      s"$key='$value'"
    }).mkString(", ")

    val addPartitionSql = s"ALTER TABLE $dbName.$tableName DROP IF EXISTS PARTITION($partitionPartSql) "
    hiveSqlService.execute(addPartitionSql)
  }

  /**
    * 刷新所有分区，刷新方式为将所有分区的CD_ID改为表最新的CD_ID
    *
    * @param dbName       数据库名称
    * @param tablePattern 表模式，如：log_medusa_main3x_%
    */
  def refreshAllPartitions(dbName: String, tablePattern: String): Unit = {
    hiveMetaService.refreshAllPartitions(dbName, tablePattern)
  }

  /**
    * 刷新匹配到的表的TBLPROPERTIES
    *
    * @param dbName       数据库名称
    * @param tablePattern 表名模式
    */
  def refreshTblProperties(dbName: String, tablePattern: String): Unit = {
    val tables = hiveMetaService.getTables(dbName, tablePattern)

    tables.foreach(table => {
      val tableName = table.tableName
      val tableId = table.tableId
      try {
        val sql = s"DELETE FROM TABLE_PARAMS WHERE TBL_ID = $tableId AND PARAM_KEY LIKE 'spark%' "
        hiveMetaService.hiveMetaDao.execute(sql)
        logger.info(s"tableName $tableName -> meta_sql[$sql] executing...")
        val sqlList = hiveMetaService.getLastPartitionQueryAllSql(dbName, tablePattern)
        sqlList.foreach(sql => {
          sparkSqlService.executeQuery(sql)
        })
        logger.info(s"tableName $tableName -> execute success")
      } catch {
        case e: Exception => {
          logger.error(s"table [$tableName] refresh failed", e)
        }
      }
    })

  }

  /**
    * 执行DDL语句或添加删除分区语句
    *
    * @param sql HQL语句
    */
  def execute(sql: String): Unit = {
    hiveSqlService.execute(sql)
  }

  /**
    * 执行一次性的查询，由于连接未释放，因此不适合反复查询的场景
    *
    * @param sql HQL语句
    */
  def executeDisposableQuery(sql: String): ResultSet = {
    hiveSqlService.executeDisposableQuery(sql)
  }

  /**
    * 执行一次性的查询，由于连接未释放，因此不适合反复查询的场景
    *
    * @param sql HQL语句
    */
  def executeQuery(sql: String)(op: ResultSet => Unit): Unit = {
    hiveSqlService.executeQuery(sql)(op)
  }

}
