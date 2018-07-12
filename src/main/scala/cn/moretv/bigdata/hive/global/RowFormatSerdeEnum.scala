package cn.moretv.bigdata.hive.global

/**
  * @author lian.kai
  * @since 2018-07-04
  */
object RowFormatSerdeEnum extends Enumeration {

  type RowFormatSerdeEnum = Value
  val JSON,PARQUET = Value
}
