package cn.moretv.bigdata.hive.examples

import cn.moretv.bigdata.hive.HiveSdk
import cn.moretv.bigdata.hive.global.EnvEnum
import org.junit.Test

/**
  * 示例程序，用于临时处理一些情况，上线时要注释掉方法上的@Test注解
  */
class TempTask {

  val dbName = "ods_view"
  val hiveSdk = HiveSdk(EnvEnum.PRODUCT)

//  @Test
  def dropColumns(): Unit ={

    hiveSdk.dropColumns(dbName,"log_medusa_main3x_medusa_p2pvod_runtimestate",List("_msg"))
  }

//  @Test
  def refreshAllPartitions(): Unit ={
    val start = System.currentTimeMillis()
    hiveSdk.refreshAllPartitions(dbName,"log_medusa_main3x_medusa_p2pvod_runtimestate")
    val end = System.currentTimeMillis()
    println(end-start)
  }

  @Test
  def test_query(): Unit ={
    val sql = "select * from dws_medusa_bi.detail_recommend_content_detail where day_p = '20190305' limit 10"
    val resultSet = hiveSdk.executeDisposableQuery(sql)
    val metadata = resultSet.getMetaData
    while (resultSet.next()){
      (1 to metadata.getColumnCount).foreach(i => {
        val columnName = metadata.getColumnName(i)
        val typeName = metadata.getColumnTypeName(i)
        print(s"$columnName : $typeName : ")
        println(resultSet.getString(i))
      })
    }
  }

}
