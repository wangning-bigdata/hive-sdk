package cn.moretv.bigdata.hive

import cn.moretv.bigdata.hive.global.EnvEnum
import cn.moretv.bigdata.hive.service.{HiveMetaService, HiveSqlService}
import cn.moretv.bigdata.hive.util.FileUtil

object InfoBackup {

  val hiveMetaService = HiveMetaService(EnvEnum.PRODUCT)
  val hiveSqlService = HiveSqlService(EnvEnum.PRODUCT)

  def main(args: Array[String]): Unit = {
    var dbName = "ods_view"
    var tablePattern = "%"
    if(args.length == 2){
      dbName = args(0)
      tablePattern = args(1)
    }

    val tables = hiveMetaService.getTables(dbName, tablePattern)
    val partMap = hiveMetaService.getAllPartitionsSql(dbName, tablePattern)

    tables.foreach(table => {
      val tableName = table.tableName
      val createTableSql = hiveSqlService.getCreateTableSql(dbName,tableName)
      val addSqlList = partMap.getOrElse(tableName,(List(""),List("")))._2
      FileUtil.writeFile(s"raw_table_info_backup.hive.sql", s"# tableName: $tableName")
      FileUtil.writeFile(s"raw_table_info_backup.hive.sql", s"drop table $tableName;")
      FileUtil.writeFile(s"raw_table_info_backup.hive.sql", createTableSql+";")
      addSqlList.foreach(sql => FileUtil.writeFile(s"raw_table_info_backup.hive.sql", sql+";"))
    })

  }
}
