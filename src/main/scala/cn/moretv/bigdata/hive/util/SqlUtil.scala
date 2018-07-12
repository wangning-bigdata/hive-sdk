package cn.moretv.bigdata.hive.util

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.sql.{Connection, SQLException}
import java.util.{List, Map}
import javax.sql.DataSource

import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum
import org.apache.commons.dbutils.handlers.{ArrayHandler, ArrayListHandler, MapListHandler}
import org.apache.commons.dbutils.{DbUtils, QueryRunner}


case class SqlUtil(env: EnvEnum) {

  /**
    * 定义变量
    */
  val queryRunner: QueryRunner = new QueryRunner
  lazy val dataSourceUtil = new DataSourceUtil(env)
  lazy val dataSource: DataSource = dataSourceUtil.getMysqlDataSource
  lazy val conn: Connection = dataSource.getConnection

  /**
    * 向数据库中插入记录
    *
    * @param sql    预编译的sql语句
    * @param params 插入的参数
    * @return 影响的行数
    * @throws SQLException
    */
  def insert(sql: String, params: Any*): Int = {
    queryRunner.update(conn, sql, asJava(params): _*)
  }

  /**
    * 通过指定的SQL语句和参数查询数据
    *
    * @param sql    预编译的sql语句
    * @param params 查询参数
    * @return 查询结果
    */
  def selectOne(sql: String, params: Any*): Array[AnyRef] = {
    queryRunner.query(conn, sql, new ArrayHandler(), asJava(params): _*)
  }

  /**
    * 通过指定的SQL语句和参数查询数据
    *
    * @param sql    预编译的sql语句
    * @param params 查询参数
    * @return 查询结果
    */
  def selectMapList(sql: String, params: Any*): List[Map[String, AnyRef]] = {
    queryRunner.query(conn, sql, new MapListHandler(), asJava(params): _*)
  }

  /**
    * 通过指定的SQL语句和参数查询数据
    *
    * @param sql    预编译的sql语句
    * @param params 查询参数
    * @return 查询结果
    */
  def selectArrayList(sql: String, params: Any*): List[Array[AnyRef]] = {
    queryRunner.query(conn, sql, new ArrayListHandler(), asJava(params): _*)
  }

  def executeSql(sql:String) = {
    queryRunner.update(sql)
  }

  /**
    * 删除错乱的数据
    *
    * @param sql    delete sql
    * @param params delete sql params
    * @return
    */
  def delete(sql: String, params: Any*) = {
    queryRunner.update(conn, sql, asJava(params): _*)
  }

  /**
    * 释放资源，如关闭数据库连接
    */
  def destory() {
    DbUtils.closeQuietly(conn)
  }

  def update(sql: String, params: Any*): Int = {
    queryRunner.update(conn, sql, asJava(params): _*)
  }

  def asJava(params: Seq[Any]) = {
    params.map {
      case null => null
      case e: Long => new JLong(e)
      case e: Double => new JDouble(e)
      case e: String => e
      case e: Short => new JShort(e)
      case e: Int => new Integer(e)
      case e: Float => new JFloat(e)
      case e: Any => e.asInstanceOf[Object]
    }
  }

}
