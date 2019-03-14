package cn.moretv.bigdata.hive.dao.impl

import java.sql.{Connection, ResultSet, Statement}

import cn.moretv.bigdata.hive.dao.HiveSqlDao
import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum
import cn.moretv.bigdata.hive.util.{DataSourceUtil, ResultSetUtil}
import javax.sql.DataSource

case class HiveSqlDaoImpl(env: EnvEnum) extends HiveSqlDao {

  lazy val dataSourceUtil: DataSourceUtil = DataSourceUtil(env)
  lazy val dataSource: DataSource = dataSourceUtil.getHiveDataSource


  /**
    * 获取指定数据库中特定表的创建SQL语句
    *
    * @param dbName    数据库名
    * @param tableName 表名
    * @return
    */
  override def showCreateTable(dbName: String, tableName: String): String = {
    val createTableSql = s"show create table `$dbName`.`$tableName`"
    val conn: Connection = dataSource.getConnection
    val stmt:Statement = conn.createStatement()
    val rs = stmt.executeQuery(createTableSql)
    val result = ResultSetUtil.rs2String(rs)
    stmt.close()
    conn.close()
    result
  }

  /**
    * 链接hiveserver2执行Hive SQL
    *
    * @param sql 需要执行的SQL
    */
  override def execute(sql: String): Unit = {
    val conn: Connection = dataSource.getConnection
    val stmt:Statement = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
    conn.close()
  }

  /**
    * 链接hiveserver2执行Hive SQL
    *
    * @param sql 需要执行的SQL
    * @return 查询结果集
    */
  override def executeDisposableQuery(sql: String): ResultSet = {
    val conn: Connection = dataSource.getConnection
    val stmt:Statement = conn.createStatement()
    stmt.executeQuery(sql)
  }

  /**
    * 链接hiveserver2执行Hive SQL
    *
    * @param sql 需要执行的SQL
    * @return 查询出的结果集
    */
  override def executeQuery(sql: String)(op: ResultSet => Unit): Unit = {
    val conn: Connection = dataSource.getConnection
    val stmt:Statement = conn.createStatement()
    val resultSet = stmt.executeQuery(sql)
    op(resultSet)
    stmt.close()
    conn.close()
  }
}