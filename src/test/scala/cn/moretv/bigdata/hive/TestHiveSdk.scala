package cn.moretv.bigdata.hive

import cn.moretv.bigdata.hive.global.{EnvEnum, RowFormatSerdeEnum}
import cn.moretv.bigdata.hive.vo.{HiveColumn, HiveTable}
import org.junit.Test

class TestHiveSdk {

  private val hiveSdkTest = HiveSdk(EnvEnum.TEST)
  private val testDbName = "ods_view"
  private val testTableName = "test_temp_table"

  @Test
  def test_createTable(): Unit = {

    val hiveColumnList = List(HiveColumn("name", "string", false), HiveColumn("age", "int", false), HiveColumn("gender", "string", false), HiveColumn("key_day", "string", true), HiveColumn("key_hour", "string", true))
    val dropSql = s"drop table if exists $testDbName.$testTableName"
    hiveSdkTest.execute(dropSql)
    val hiveTable = new HiveTable(testDbName, testTableName, true, RowFormatSerdeEnum.PARQUET, "/data_warehouse/ods_view.db/test_temp_table")
    hiveSdkTest.createTable(hiveTable, hiveColumnList)
    val columns = hiveSdkTest.getColumns(testDbName, testTableName)
    assert(columns.length == 3 && columns.exists(_.columnName == "name"))

  }

  @Test
  def test_getColumns(): Unit = {
    val columns = hiveSdkTest.getColumns(testDbName, testTableName)
    assert(columns.contains(HiveColumn("name", "string", false)) && columns.contains(HiveColumn("age", "int", false)) && columns.contains(HiveColumn("gender", "string", false)))
  }


  @Test
  def test_addColumns(): Unit = {
    val hiveColumnList = List(HiveColumn("sex", "string", false), HiveColumn("height", "int", false))
    val columnsBefore = hiveSdkTest.getColumns(testDbName, testTableName)
    hiveSdkTest.addColumns(testDbName, testTableName, hiveColumnList)
    val columnsAfter = hiveSdkTest.getColumns(testDbName, testTableName)
    assert(!columnsBefore.exists(_.columnName == "sex") && !columnsBefore.exists(_.columnName == "height") && columnsAfter.exists(_.columnName == "sex") && columnsAfter.exists(_.columnName == "height"))
  }

  @Test
  def test_dropColumns(): Unit ={
    val dropColumns = List("sex","age")
    val columns1 = hiveSdkTest.getColumns(testDbName, testTableName)
    hiveSdkTest.dropColumns(testDbName,testTableName,dropColumns)
    val columns2 = hiveSdkTest.getColumns(testDbName, testTableName)
    assert(dropColumns.forall(c => columns1.exists(_.columnName == c)) && !dropColumns.exists(c => columns2.exists(_.columnName == c)))
  }

  @Test
  def test_changeColumns(): Unit ={
    val changeMap = Map("height" -> new HiveColumn("height","double"), "gender" -> new HiveColumn("weight","bigint"))
    val columns1 = hiveSdkTest.getColumns(testDbName, testTableName)
    hiveSdkTest.changeColumns(testDbName,testTableName,changeMap)
    val columns2 = hiveSdkTest.getColumns(testDbName, testTableName)
    assert(columns1.exists(x => x.columnName == "height" && x.columnType == "int") && columns1.exists(x => x.columnName == "gender" && x.columnType == "string") && !columns2.exists(x => x.columnName == "height" && x.columnType == "int") && !columns2.exists(x => x.columnName == "gender" && x.columnType == "string") && columns2.exists(x => x.columnName == "height" && x.columnType == "double") && columns2.exists(x => x.columnName == "weight" && x.columnType == "bigint"))
  }

  @Test
  def test_dropPartition_addPartition_getAllPartitions(): Unit = {
    val partitionMap = Map("key_day" -> "20180709", "key_hour" -> "12")
    val location = "hdfs://hans/data_warehouse/ods_view.db/test_temp_table/key_day=20180709/key_hour=12"
    hiveSdkTest.dropPartition(testDbName, testTableName, partitionMap)
    val partitions1 = hiveSdkTest.getAllPartitions(testDbName, testTableName)
    hiveSdkTest.addPartition(testDbName, testTableName, partitionMap, location)
    val partitions2 = hiveSdkTest.getAllPartitions(testDbName, testTableName)
    hiveSdkTest.dropPartition(testDbName, testTableName, partitionMap)
    val partitions3 = hiveSdkTest.getAllPartitions(testDbName, testTableName)
    hiveSdkTest.addPartition(testDbName, testTableName, partitionMap, location)
    assert(!partitions1.contains(location) && partitions2.size == 1 && partitions2(location)("key_day") == "20180709" && !partitions3.contains(location))

  }

  @Test
  def test_addPartitions(): Unit = {
    val partitionMap1 = Map("key_day" -> "20180709", "key_hour" -> "12")
    val location1 = "hdfs://hans/data_warehouse/ods_view.db/test_temp_table/key_day=20180709/key_hour=12"
    val partitionMap2 = Map("key_day" -> "20180710", "key_hour" -> "07")
    val location2 = "hdfs://hans/data_warehouse/ods_view.db/test_temp_table/key_day=20180710/key_hour=07"
    val partitionInfoMap = Map(location1 -> partitionMap1, location2 -> partitionMap2)
    hiveSdkTest.dropPartition(testDbName, testTableName, partitionMap1)
    hiveSdkTest.dropPartition(testDbName, testTableName, partitionMap2)
    val partitions1 = hiveSdkTest.getAllPartitions(testDbName, testTableName)
    hiveSdkTest.addPartitions(testDbName, testTableName, partitionInfoMap)
    val partitions2 = hiveSdkTest.getAllPartitions(testDbName, testTableName)
    assert(!partitions1.contains(location1) && !partitions1.contains(location2) && partitions2(location1)("key_day") == "20180709" && partitions2(location2)("key_day") == "20180710")
  }

}
