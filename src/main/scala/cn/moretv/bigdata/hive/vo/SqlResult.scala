package cn.moretv.bigdata.hive.vo

/**
  * @author luoziyu
  * date 2018/1/29.
  */
class SqlResult[T] {
  var resData: T = _
  var errorSqls: Seq[String] = _
  var isSuccess = false

}
