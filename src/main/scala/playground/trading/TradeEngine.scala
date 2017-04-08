package playground.trading

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction.{OnTimerContext, Context}
import org.apache.flink.streaming.api.functions.co.{RichCoProcessFunction, RichCoFlatMapFunction}
import org.apache.flink.util.Collector

object TradeEngine {
  def apply() = new TradeEngine
}

class TradeEngine extends RichCoProcessFunction[Position, Bid, Trade] {

  val DecayInterval = 3000
  val DownTick = 0.10f
  val UpTick = 0.10f

  var positionState: ValueState[Position] = _

  override def open(parameters: Configuration): Unit = {
    val positionDesc = new ValueStateDescriptor[Position]("position", createTypeInformation[Position])
    positionDesc.setQueryable("position")
    positionState = getRuntimeContext.getState(positionDesc)
  }

  override def processElement1(position: Position, ctx: Context, out: Collector[Trade]): Unit = {
    positionState.update(position.copy(askPrice = position.askPrice + UpTick))
    registerAskPriceDecayTimer(position.expirationMillis, ctx)
  }

  override def processElement2(bid: Bid, ctx: Context, out: Collector[Trade]): Unit = {
    val position = positionState.value()

    if (isGoodTrade(bid, position)) {
      makeTrade(bid, out)
      positionState.update(updatePosition(position, bid))
    }
  }

  override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[Trade]): Unit = {
    var position = positionState.value()
    if (position != null && position.askPrice > 0) {
      position = position.copy(askPrice = position.askPrice - DownTick)
      positionState.update(position)
      registerAskPriceDecayTimer(timestamp, ctx)
    }
  }

  private def registerAskPriceDecayTimer(timestamp: Long, ctx: Context): Unit = {
    ctx.timerService().registerProcessingTimeTimer(timestamp + DecayInterval)
  }

  private def makeTrade(bid: Bid, out: Collector[Trade]): Unit = {
    out.collect(Trade(bid.symbol, bid.quantity, bid.price))
  }

  private def updatePosition(position: Position, bid: Bid): Position = {
    val quantity = position.quantity - bid.quantity

    if (quantity > 0) {
      val profit = position.profit + (bid.price - position.buyPrice) * bid.quantity
      position.copy(quantity = quantity, profit = profit, askPrice = bid.price + UpTick, tradePrice = bid.price) // return new position
    }
    else {
      null // clear position
    }
  }

  private def isGoodTrade(bid: Bid, position: Position): Boolean = {
    haveInventory(bid, position) && isFavorablePrice(bid, position)
  }

  private def isFavorablePrice(bid: Bid, position: Position): Boolean = {
    bid.price >= position.askPrice
  }

  private def haveInventory(bid: Bid, position: Position): Boolean = {
    null != position && position.quantity >= bid.quantity
  }

}
