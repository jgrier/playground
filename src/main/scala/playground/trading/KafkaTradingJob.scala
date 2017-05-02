package playground.trading

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

object KafkaTradingJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "flinktrade2");

    val positions = env.addSource(new FlinkKafkaConsumer09[Position]("positions", new PositionSerializationSchema, kafkaProps))
      .keyBy(_.symbol)

    val quotes = env.addSource(new FlinkKafkaConsumer09[Bid]("quotes", new BidSerializationSchema, kafkaProps))
      .keyBy(_.symbol)

    positions
      .connect(quotes)
      .process(TradeEngine())
      .print()

    env.execute()
  }
}
