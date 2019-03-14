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

  /**
    * 链接hiveserver2执行Hive SQL，由于返回ResultSet，因此不适合反复查询
    *
    * @param sql 需要执行的SQL
    * @return 查询出的结果集
    */
  def executeDisposableQuery(sql: String): ResultSet

  /**
    * 链接hiveserver2执行Hive SQL
    *
    * @param sql 需要执行的SQL
    * @return 查询出的结果集
    */
  def executeQuery(sql:String)(op: ResultSet => Unit): Unit

}
