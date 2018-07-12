package cn.moretv.bigdata.hive.vo

/**
  * @author luoziyu
  * date 2018/1/30.
  *
  * 自定义ResultSet类
  *
  */
class MResultSet (rows: Seq[ResultRow]){
  var currIndex = -1
  var rowsSize = rows.size
  var currRow: ResultRow = _

  def next(): Boolean = {
    if (currIndex < rowsSize - 1 ) {
      currIndex += 1
      currRow = rows(currIndex)
      currRow != null
    } else {
      false
    }
  }

  def first(): Boolean = {
    currIndex = 0
    currRow = rows(currIndex)
    currRow != null
  }

  def last(): Boolean = {
    currIndex = rows.length - 1
    currRow = rows(currIndex)
    currRow != null
  }

  /**
    * 和Result保持一致，所以index从1开始
    * @param index
    * @return
    */
  def get(index: Int): Object = {
    if(index < 1) throw new RuntimeException("IndexOutOfBounds! First column in MResultSet is 1!")
    val col = currRow.cols(index - 1)
    col.columnValue
  }
}
