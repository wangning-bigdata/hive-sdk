package cn.moretv.bigdata.hive.vo

case class HiveColumn(columnName: String, columnType: String, isPartitionColumn: Boolean) {

  def this(columnName: String) = this(columnName, null, false)

  def this(columnName: String, columnType: String) = this(columnName, columnType, false)
}
