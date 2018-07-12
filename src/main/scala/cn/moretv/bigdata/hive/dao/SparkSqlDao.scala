package cn.moretv.bigdata.hive.dao

import java.sql.ResultSet

trait SparkSqlDao {

  /**
    * 链接spark thrift server执行SQL，并返回结果
    *
    * @param sql 需要执行的SQL
    * @return SQL执行结果
    */
  def executeQuery(sql: String): ResultSet

  /**
    * 释放资源
    */
  def destroy(): Unit
}
