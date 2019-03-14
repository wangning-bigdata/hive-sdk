package cn.moretv.bigdata.hive.util

import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.slf4j.LoggerFactory


object DefaultDataSourceFactory {

  val logger = LoggerFactory.getLogger(this.getClass)
  //基本的设置
  val DRIVER_CLASSNAME = "driverClassName"
  val URL = "url"
  val USERNAME = "username"
  val PASSWORD = "password"

  //初始化时连接池中connection数量
  val INITIALSIZE = "initialSize"

  //最大连接数量
  val MAXSIZE = "maxPoolSize"

  //最大的空闲连接数量
  val MAXIDLE = "maxIdle"

  //最小的空闲链接数量
  val MINSIZE = "minPoolSize"

  //最大的等待时间，单位是毫秒
  val MAXWAIT = "maxWait"

  //是否开启自动提交，跟事务的控制有关
  val DEFAULTAUTOCOMMIT = "defaultAutoCommit"

  def createDataSource(properties: Properties): ComboPooledDataSource = {
    val dataSource: ComboPooledDataSource = new ComboPooledDataSource()

    val driverClassName = properties.getProperty(DRIVER_CLASSNAME)
    val url = properties.getProperty(URL)
    val username = properties.getProperty(USERNAME)
    val password = properties.getProperty(PASSWORD)
    val initialSize = properties.getProperty(INITIALSIZE).toInt
    val maxSize = properties.getProperty(MAXSIZE).toInt
    val minSize = properties.getProperty(MINSIZE).toInt
    val maxIdle = properties.getProperty(MAXWAIT).toInt
    val defaultAutoCommit = properties.getProperty(DEFAULTAUTOCOMMIT).toBoolean

    logger.info("connection properties: {}", properties)

    dataSource.setJdbcUrl(url)
    dataSource.setDriverClass(driverClassName)
    dataSource.setUser(username)
    dataSource.setPassword(password)
    dataSource.setInitialPoolSize(initialSize)
    dataSource.setMaxPoolSize(maxSize)
    dataSource.setMinPoolSize(minSize)
    dataSource.setMaxIdleTime(maxIdle)
    dataSource.setAutoCommitOnClose(defaultAutoCommit)
    dataSource.setTestConnectionOnCheckin(false)
    dataSource.setTestConnectionOnCheckout(true)

    dataSource
  }
}
