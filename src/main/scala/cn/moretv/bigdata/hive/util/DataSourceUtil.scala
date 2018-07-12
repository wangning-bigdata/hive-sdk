package cn.moretv.bigdata.hive.util

import java.util.Properties
import javax.sql.DataSource

import cn.moretv.bigdata.hive.global.EnvEnum
import cn.moretv.bigdata.hive.global.EnvEnum.EnvEnum

/**
  * 获取对应的数据源
  *
  * @param env 生产环境 or 测试环境
  * @author lian.kai
  * @since 2018-07-10
  */
case class DataSourceUtil(env: EnvEnum) {

  private val DEFAULT_HIVE_CONFIG_FILE = "conf/c3p0-hive-default.properties"
  private val DEFAULT_SPARK_CONFIG_FILE = "conf/c3p0-spark-default.properties"
  private val DEFAULT_MYSQL_CONFIG_FILE = "conf/c3p0-mysql-default.properties"
  private val PRODUCT_HIVE_CONFIG_FILE = "c3p0-hive-product.properties"
  private val PRODUCT_SPARK_CONFIG_FILE = "c3p0-spark-product.properties"
  private val PRODUCT_MYSQL_CONFIG_FILE = "c3p0-mysql-product.properties"
  private val DEFAULT_PRODUCT_HIVE_CONFIG_FILE = "conf/c3p0-hive-product.properties"
  private val DEFAULT_PRODUCT_SPARK_CONFIG_FILE = "conf/c3p0-spark-product.properties"
  private val DEFAULT_PRODUCT_MYSQL_CONFIG_FILE = "conf/c3p0-mysql-product.properties"
  private val TEST_HIVE_CONFIG_FILE = "c3p0-hive-test.properties"
  private val TEST_SPARK_CONFIG_FILE = "c3p0-spark-test.properties"
  private val TEST_MYSQL_CONFIG_FILE = "c3p0-mysql-test.properties"
  private val DEFAULT_TEST_HIVE_CONFIG_FILE = "conf/c3p0-hive-test.properties"
  private val DEFAULT_TEST_SPARK_CONFIG_FILE = "conf/c3p0-spark-test.properties"
  private val DEFAULT_TEST_MYSQL_CONFIG_FILE = "conf/c3p0-mysql-test.properties"


  lazy val getHiveDataSource: DataSource = getDataSource("hive")
  lazy val getSparkDataSource: DataSource = getDataSource("spark")
  lazy val getMysqlDataSource: DataSource = getDataSource("mysql")

  private def getDataSource(config: String): DataSource = {
    val classLoader = this.getClass.getClassLoader
    val (resource, defaultResource) =
      if (env == EnvEnum.PRODUCT) {
        config match {
          case "hive" => (PRODUCT_HIVE_CONFIG_FILE, DEFAULT_PRODUCT_HIVE_CONFIG_FILE)
          case "spark" => (PRODUCT_SPARK_CONFIG_FILE, DEFAULT_PRODUCT_SPARK_CONFIG_FILE)
          case "mysql" => (PRODUCT_MYSQL_CONFIG_FILE, DEFAULT_PRODUCT_MYSQL_CONFIG_FILE)
          case _ => throw new RuntimeException(s"config name <$config> is not valid, which should be one of [hive,mysql,spark]")
        }
      } else if (env == EnvEnum.TEST) {
        config match {
          case "hive" => (TEST_HIVE_CONFIG_FILE, DEFAULT_TEST_HIVE_CONFIG_FILE)
          case "spark" => (TEST_SPARK_CONFIG_FILE, DEFAULT_TEST_SPARK_CONFIG_FILE)
          case "mysql" => (TEST_MYSQL_CONFIG_FILE, DEFAULT_TEST_MYSQL_CONFIG_FILE)
          case _ => throw new RuntimeException(s"config name <$config> is not valid, which should be one of [hive,mysql,spark]")
        }
      } else {
        config match {
          case "hive" => (TEST_HIVE_CONFIG_FILE, DEFAULT_HIVE_CONFIG_FILE)
          case "spark" => (TEST_SPARK_CONFIG_FILE, DEFAULT_SPARK_CONFIG_FILE)
          case "mysql" => (TEST_MYSQL_CONFIG_FILE, DEFAULT_MYSQL_CONFIG_FILE)
          case _ => throw new RuntimeException(s"config name <$config> is not valid, which should be one of [hive,mysql,spark]")
        }

      }

    val inputStream = classLoader.getResourceAsStream(resource)
    val prop = new Properties()
    if (inputStream != null) {
      prop.load(inputStream)
      DefaultDataSourceFactory.createDataSource(prop)
    } else {
      val in = classLoader.getResourceAsStream(defaultResource)
      prop.load(in)
      DefaultDataSourceFactory.createDataSource(prop)
    }
  }

}
