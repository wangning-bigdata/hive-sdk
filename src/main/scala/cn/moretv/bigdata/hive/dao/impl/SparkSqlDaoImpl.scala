package cn.moretv.bigdata.hive.dao.impl

import java.sql.{Connection, ResultSet, Statement}
import javax.sql.DataSource

import cn.moretv.bigdata.hive.dao.SparkSqlDao
import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum
import cn.moretv.bigdata.hive.util.DataSourceUtil

case class SparkSqlDaoImpl(env: EnvEnum) extends SparkSqlDao {


  lazy val dataSourceUtil: DataSourceUtil = DataSourceUtil(env)
  lazy val dataSource: DataSource = dataSourceUtil.getSparkDataSource
  lazy val conn: Connection = dataSource.getConnection
  lazy val stmt: Statement = conn.createStatement()

  /**
    * 链接hiveserver2执行Hive SQL，并返回结果
    *
    * @param sql 需要执行的SQL
    * @return SQL执行结果
    */
  override def executeQuery(sql: String): ResultSet = stmt.executeQuery(sql)

  /**
    * 释放资源
    */
  override def destroy(): Unit = {
    stmt.close()
    conn.close()
  }
}
