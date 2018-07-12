package cn.moretv.bigdata.hive.service

import java.sql.ResultSet

import cn.moretv.bigdata.hive.dao.SparkSqlDao
import cn.moretv.bigdata.hive.dao.impl.SparkSqlDaoImpl
import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum
import org.slf4j.LoggerFactory

case class SparkSqlService(env: EnvEnum) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  val sparkSqlDao:SparkSqlDao = SparkSqlDaoImpl(env)

  def executeQuery(sql:String): ResultSet = {
    logger.info(sql)
    sparkSqlDao.executeQuery(sql)
  }

}
