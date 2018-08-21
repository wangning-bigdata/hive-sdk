package cn.moretv.bigdata.hive.vo

import cn.moretv.bigdata.hive.global.RowFormatSerdeEnum
import cn.moretv.bigdata.hive.global.RowFormatSerdeEnum.RowFormatSerdeEnum

/**
  * Hive表映射类
  *
  * @param dbName    数据库名称，此值为冗余字段，本不该出现在此，但是为了拼接方便，定义了此值
  * @param tableName 表名称
  * @param tableId   表ID，唯一标识
  */
case class HiveTable(dbName: String, tableName: String, tableId: Long, isExternal: Boolean, rowFormatSerdeEnum: RowFormatSerdeEnum, location: String) {

  def this(dbName: String, tableName: String) = this(dbName,tableName,0L,false,null,null)

  def this(dbName: String, tableName: String, tableId: Long) = this(dbName,tableName,tableId,false,null,null)

  def this(dbName: String, tableName: String, isExternal: Boolean, rowFormatSerdeEnum: RowFormatSerdeEnum, location: String) = this(dbName,tableName,0L,isExternal,rowFormatSerdeEnum,location)

}

object HiveTable {

  /**
    * 拼装建表语句并返回
    * @param hiveTable hive表
    * @param hiveColumnList 字段集合
    * @return 建表语句
    */
  def getCreateTableSql(hiveTable: HiveTable, hiveColumnList: Seq[HiveColumn]): String = {
    if( hiveTable.rowFormatSerdeEnum != null && hiveTable.location != null){
      val stringBuffer = new StringBuffer("CREATE ")
      if(hiveTable.isExternal){
        stringBuffer.append("EXTERNAL TABLE ")
      }else {
        stringBuffer.append("TABLE ")
      }
      stringBuffer.append(s"${hiveTable.dbName}.${hiveTable.tableName} ")
      stringBuffer.append("( \n")
      val columnStr = hiveColumnList.filter(c => !c.isPartitionColumn).map(hiveColumn => {
        s"  `${hiveColumn.columnName.trim}` ${hiveColumn.columnType.trim}"
      }).mkString(", \n")
      stringBuffer.append(columnStr)
      stringBuffer.append(") \n")
      stringBuffer.append("PARTITIONED BY ( \n")
      val partitionStr = hiveColumnList.filter(c => c.isPartitionColumn).map(hiveColumn => {
        s"  `${hiveColumn.columnName.trim}` ${hiveColumn.columnType.trim}"
      }).mkString(", \n")
      stringBuffer.append(partitionStr)
      stringBuffer.append(") \n")
      if(hiveTable.rowFormatSerdeEnum == RowFormatSerdeEnum.PARQUET){
        stringBuffer.append("STORED AS PARQUET \n")
      }else if(hiveTable.rowFormatSerdeEnum == RowFormatSerdeEnum.JSON){
        stringBuffer.append("ROW FORMAT SERDE \n  'org.apache.hive.hcatalog.data.JsonSerDe' \nSTORED AS TEXTFILE \n")
      }
      stringBuffer.append("LOCATION \n")
      stringBuffer.append(s"  '${hiveTable.location}'")
      stringBuffer.toString

    }else throw new RuntimeException(s"Can not assemble sql, while rowFormatSerdeEnum was [${hiveTable.rowFormatSerdeEnum}] and location was [${hiveTable.location}]")
  }

  /**
    * 拼接添加字段SQL
    * @param hiveTable hive表
    * @param hiveColumnList 字段集合
    * @return 添加字段语句
    */
  def getAddColumnSql(hiveTable: HiveTable, hiveColumnList: Seq[HiveColumn]): String = {
    val stringBuffer = new StringBuffer("ALTER TABLE ")
    stringBuffer.append(s"${hiveTable.dbName}.${hiveTable.tableName} ")
    stringBuffer.append("ADD COLUMNS ( \n")
    val columnStr = hiveColumnList.map(hiveColumn => {
      s"  `${hiveColumn.columnName.trim}` ${hiveColumn.columnType.trim}"
    }).mkString(", \n")
    stringBuffer.append(columnStr)
    stringBuffer.append(" )")
    stringBuffer.toString
  }

  /**
    * 拼接添加字段SQL
    * @param hiveTable hive表
    * @param hiveColumnList 字段集合
    * @return 添加字段语句
    */
  def getReplaceColumnSql(hiveTable: HiveTable, hiveColumnList: Seq[HiveColumn]): String = {
    val stringBuffer = new StringBuffer("ALTER TABLE ")
    stringBuffer.append(s"${hiveTable.dbName}.${hiveTable.tableName} ")
    stringBuffer.append("REPLACE COLUMNS ( \n")
    val columnStr = hiveColumnList.map(hiveColumn => {
      s"  `${hiveColumn.columnName.trim}` ${hiveColumn.columnType.trim}"
    }).mkString(", \n")
    stringBuffer.append(columnStr)
    stringBuffer.append(" )")
    stringBuffer.toString
  }

  /**
    * 获取更改字段名或字段类型的语句
    * @param hiveTable hive表
    * @param hiveColumnMap 更改前字段名 -> 更改后的字段名和字段类型
    * @return
    */
  def getChangeColumnSql(hiveTable: HiveTable, hiveColumnMap: Map[String, HiveColumn]): List[String] = {
    val stringBuffer = new StringBuffer("ALTER TABLE ")
    stringBuffer.append(s"${hiveTable.dbName}.${hiveTable.tableName} ")
    stringBuffer.append("CHANGE ")
    val sql = stringBuffer.toString
    hiveColumnMap.map(e => {
      val originalColumnName = e._1
      val hiveColumn = e._2
      sql + s"`${originalColumnName.trim}` `${hiveColumn.columnName.trim}` ${hiveColumn.columnType.trim}"
    }).toList
  }

}

