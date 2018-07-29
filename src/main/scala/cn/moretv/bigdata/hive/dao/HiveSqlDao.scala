package cn.moretv.bigdata.hive.dao

import java.sql.ResultSet

trait HiveSqlDao {

  /**
    * 获取指定数据库中特定表的创建SQL语句
    *
    * @param dbName    数据库名
    * @param tableName 表名
    * @return
    */
  def showCreateTable(dbName: String, tableName: String): String

  /**
    * 链接hiveserver2执行Hive SQL
    *
    * @param sql 需要执行的SQL
    */
  def execute(sql: String): Unit

}
