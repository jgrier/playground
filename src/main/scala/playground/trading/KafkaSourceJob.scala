package playground.trading

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09

object KafkaSourceJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val PositionPort = 2000
    val BidPort = 3000
    val PositionHost = "localhost"
    val BidHost = "localhost"

    val positions = env.socketTextStream(PositionHost, PositionPort)
      .map(Position.fromString(_, timeOffsetMode = true))
      .flatMap(BadDataHandler[Position])
      .addSink(new FlinkKafkaProducer09[Position]("localhost:9092", "positions", new PositionSerializationSchema))

    val quotes = env.socketTextStream(BidHost, BidPort)
      .map(Bid.fromString(_))
      .flatMap(BadDataHandler[Bid])
      .addSink(new FlinkKafkaProducer09[Bid]("localhost:9092", "quotes", new BidSerializationSchema))

    env.execute()
  }
}
