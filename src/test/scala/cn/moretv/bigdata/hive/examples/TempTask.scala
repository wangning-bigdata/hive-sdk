package cn.moretv.bigdata.hive.examples

import cn.moretv.bigdata.hive.HiveSdk
import cn.moretv.bigdata.hive.global.EnvEnum

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

}
