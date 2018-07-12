package cn.moretv.bigdata.hive.util

import java.sql.ResultSet

object ResultSetUtil {

  /**
    * 将ResultSet中存储的结果合并成String，只适用于ResultSet中只有一列的情况或者只合并第一列的情况
    * @param rs
    * @return
    */
  def rs2String(rs:ResultSet,separator: String = "\n"):String = {
    val sb = new StringBuffer("")
    while (rs.next()) {
      val result = rs.getString(1)
      sb.append(result)
      sb.append(separator)
    }
    sb.toString
  }
}
