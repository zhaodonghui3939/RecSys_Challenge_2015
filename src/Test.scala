import java.text.SimpleDateFormat
import java.util.Calendar

object Test {
  def main(args: Array[String]) {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    val ss = "2014-04-07T12:34:45.034Z"
    val t = ss.substring(0,ss.length - 5).replace("T"," ")
    println(formatter.parse(t))
    val cal=Calendar.getInstance()
    cal.setTime(formatter.parse(t))
    println(cal.get(Calendar.HOUR_OF_DAY))
   // println(ss.replace("T"," "))
//    val b = formatter.parse("2014-04-07 00:00:00")
//    val s = formatter.parse("2014-04-08 09:05:08")
//    println((s.getTime - b.getTime)/3600/1000)
  }

}
