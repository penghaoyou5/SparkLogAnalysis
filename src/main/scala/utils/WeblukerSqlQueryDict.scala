package utils

import java.sql.{DriverManager, ResultSet}
import com.mysql.jdbc.Driver


object WeblukerSqlQueryDict {
  val domainUserDict = scala.collection.mutable.Map("tom" -> 0)

  // Change to Your Database Config
  val conn_str = "jdbc:mysql://223.202.202.12:3306/webluker?user=reader&password=CsP_9r0up"
  // Load the driver
//  println(Driver);
  DriverManager.registerDriver(new Driver())
  Class.forName("com.mysql.jdbc.Driver")
//  classOf[com.mysql.jdbc.Driver]
  // Setup the connection
  val conn = DriverManager.getConnection(conn_str)

  // Configure to be Read Only
  val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

  // Execute Query
  println("start")
  val rs = statement.executeQuery("select dstdomainName,userId from seeC_dstdomainstruct")
  println("end")

  while (rs.next) {
//          println(rs.getString("dstdomainName") + "   " + rs.getInt("userId"))
    //      domainUserDict += (rs.getString("dstdomainName") -> rs.getInt("userId"))
    domainUserDict(rs.getString("dstdomainName")) = rs.getInt("userId")
  }
  conn.close
//  domainUserDict


  def main(args: Array[String]): Unit = {
    val domainUserDict = getDomainUserIdDict()
    println(domainUserDict.toBuffer)
  }

  def getDomainUserIdDict(): scala.collection.mutable.Map[String, Int] = {
    domainUserDict
  }

}
