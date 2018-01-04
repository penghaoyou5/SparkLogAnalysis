import java.lang
import java.text.SimpleDateFormat
import java.util.Date

object TimeTest {
  def main(args: Array[String]): Unit = {
    val timestamp = "1514350489.468".replace(".","")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")

    val timestampStr = dateFormat.format(new Date(new lang.Long(timestamp) ))
    println(timestampStr)

  }
}
