package playground.jgrier


import java.net.InetAddress

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

case class Person(id: Int, name: String, ip: InetAddress) extends Serializable

object PersonStream {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val personStream = env.fromCollection(Seq(
      Person(1, "Toby North", InetAddress.getByName("192.168.0.40"))
    ))

    // Expected: Person(1,Toby North,/192.168.0.40)
    // Actual  : Person(1,Toby North,/0.0.0.0)

    personStream
      .map(e => e.ip)
      .print

    env.execute
  }
}