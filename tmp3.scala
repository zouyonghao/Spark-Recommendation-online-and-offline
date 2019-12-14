import java.net.Socket
object SocketToRedis extends Serializable {
      lazy val s = new Socket("localhost", 6379)
      def getRatings(userId: String): Array[(Int, Double)] = {
          val os = s.getOutputStream();
          os.write(("get " + userId + "\r\n").getBytes());
          os.flush();
          Thread.sleep(10);
          val length = s.getInputStream().available()
          val data = new Array[Byte](length + 1)
          s.getInputStream().read(data, 0, length)
          val dataString = data.slice(data.indexOf('\n'.toByte), length)
                .map(_.toChar)
                .mkString.trim
          if (dataString.length > 0) {
              println(dataString)
            dataString.split("\\|").map(i => (i.split(",")(0).toInt, i.split(",")(1).toDouble))
          }
          else
            Array()
      }

      def setRatings(userId: String, newRecommends: Array[(Int, Double)]) {
          val content = newRecommends.map(item => item._1.toString + "," + item._2.toString).mkString("|")
          val os = s.getOutputStream();
          os.write(("set " + userId + " " + content + "\r\n").getBytes());
          os.flush();
          Thread.sleep(10);
          val length = s.getInputStream().available()
          val data = new Array[Byte](length + 1)
          s.getInputStream().read(data, 0, length)
      }
}