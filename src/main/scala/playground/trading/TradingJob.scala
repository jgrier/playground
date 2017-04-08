package playground.trading

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TradingJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val PositionPort = 2000
    val BidPort = 3000
    val PositionHost = "localhost"
    val BidHost = "localhost"

    val positions = env.socketTextStream(PositionHost, PositionPort)
      .map(Position.fromString(_))
      .flatMap(BadDataHandler[Position])
      .keyBy(_.symbol)

    val quotes = env.socketTextStream(BidHost, BidPort)
      .map(Bid.fromString(_))
      .flatMap(BadDataHandler[Bid])
      .keyBy(_.symbol)

    positions
      .connect(quotes)
      .process(TradeEngine())
      .print()

    env.execute()
  }
}
