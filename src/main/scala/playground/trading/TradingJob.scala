package playground.trading

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TradingJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val PositionPort = 2000
    val BidAskPort = 3000
    val PositionHost = "localhost"
    val BidAskHost = "localhost"

    val orders = env.socketTextStream(PositionHost, PositionPort)
      .map(Position.fromString(_))
      .flatMap(BadDataHandler[Position])
      .keyBy(_.symbol)

    val fills = env.socketTextStream(BidAskHost, BidAskPort)
      .map(BidAsk.fromString(_))
      .flatMap(BadDataHandler[BidAsk])
      .keyBy(_.symbol)

    orders
      .connect(fills)
      .process(PositionManager())
      .print()

    env.execute()
  }
}
