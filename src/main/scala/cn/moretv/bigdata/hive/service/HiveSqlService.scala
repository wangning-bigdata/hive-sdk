package cn.moretv.bigdata.hive.service

import java.sql.ResultSet

import cn.moretv.bigdata.hive.dao.HiveSqlDao
import cn.moretv.bigdata.hive.dao.impl.HiveSqlDaoImpl
import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum
import org.slf4j.LoggerFactory

case class HiveSqlService(env: EnvEnum) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  val hiveSqlDao:HiveSqlDao = HiveSqlDaoImpl(env)

  def getCreateTableSql(dbName:String,tableName:String):String = {
    hiveSqlDao.showCreateTable(dbName,tableName)
  }

  def execute(sql:String): Unit = {
    logger.info(sql)
    hiveSqlDao.execute(sql)
  }

  def executeDisposableQuery(sql:String): ResultSet = {
    logger.info(sql)
    hiveSqlDao.executeDisposableQuery(sql)
  }


}
