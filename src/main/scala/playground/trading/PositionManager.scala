package playground.trading

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction.{OnTimerContext, Context}
import org.apache.flink.streaming.api.functions.co.{RichCoProcessFunction, RichCoFlatMapFunction}
import org.apache.flink.util.Collector

object PositionManager {
  def apply() = new PositionManager
}

class PositionManager extends RichCoProcessFunction[Position, BidAsk, BidAsk] {

  var positionState: ValueState[Position] = _

  override def open(parameters: Configuration): Unit = {
    val positionDesc = new ValueStateDescriptor[Position]("position", createTypeInformation[Position])
    positionDesc.setQueryable("position")
    positionState = getRuntimeContext.getState(positionDesc)
  }

  override def processElement1(position: Position, ctx: Context, out: Collector[BidAsk]): Unit = {
    positionState.update(position)
    registerLiquidationTimer(position, ctx)
  }

  override def processElement2(bidAsk: BidAsk, ctx: Context, out: Collector[BidAsk]): Unit = {
    var position = positionState.value()

    if (isGoodTrade(bidAsk, position)) {
      position = makeTradeAndUpdatePosition(bidAsk, position)

      if (shouldLiquidatePosition(position)) {
        liquidatePosition(position, out)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[BidAsk]): Unit = {
    val position = positionState.value()
    if (position != null) {
      if (position.expirationMillis == timestamp) {
        liquidatePosition(position, out)
      }
    }
  }

  private def registerLiquidationTimer(position: Position, ctx: Context): Unit = {
    if(position.expirationMillis > 0) {
      ctx.timerService().registerProcessingTimeTimer(position.expirationMillis)
    }
  }

  private def liquidatePosition(position: Position, collector: Collector[BidAsk]): Unit = {
    collector.collect(BidAsk(position.symbol, -position.quantity, position.price))
    positionState.update(null) // clear our position
  }

  private def shouldLiquidatePosition(position: Position): Boolean = {
    position.quantity < 10
  }

  private def makeTradeAndUpdatePosition(bidAsk: BidAsk, position: Position): Position = {
    val quantity = position.quantity - bidAsk.quantity
    val profit = position.profit + (bidAsk.price - position.price) * bidAsk.quantity
    val newPosition = Position(position.symbol, quantity, position.price, profit, position.expirationMillis)
    positionState.update(newPosition)
    newPosition
  }

  private def isGoodTrade(bidAsk: BidAsk, position: Position): Boolean = {
    bidAsk.isBid && haveInventory(bidAsk, position) && isFavorablePrice(bidAsk, position)
  }

  private def isFavorablePrice(bidAsk: BidAsk, position: Position): Boolean = {
    bidAsk.price >= position.price
  }

  private def haveInventory(bidAsk: BidAsk, position: Position): Boolean = {
    null != position && position.quantity >= bidAsk.quantity
  }

}
