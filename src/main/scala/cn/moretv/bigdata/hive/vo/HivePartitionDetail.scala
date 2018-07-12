package cn.moretv.bigdata.hive.vo

case class HivePartitionDetail(partName: String, location: String,partitionMap:Map[String,String])
