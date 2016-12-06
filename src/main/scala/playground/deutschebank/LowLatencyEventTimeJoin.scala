package playground.deutschebank

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction.{Context, OnTimerContext}
import org.apache.flink.streaming.api.functions.co.RichCoProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable


object LowLatencyEventTimeJoin {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val tradeStream = env.addSource(tradeSource)
    //    val customerStream = env.addSource(customerSource)

    //    val joinedStream = tradeStream
    //      .join(customerStream)
    //      .where(_.customerId)
    //      .equalTo(_.customerId)
    //      .window(TumblingEventTimeWindows.of(Time.milliseconds(4)))
    //      .apply((t, c) => (t, c))

    val tradeStream = env.addSource((sc: SourceFunction.SourceContext[Trade]) => {
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(1000, 0, "trade-1"), 1000)
      sc.emitWatermark(new Watermark(1000))
      Thread.sleep(3000)
      sc.collectWithTimestamp(Trade(1200, 0, "trade-2"), 1200)
      sc.emitWatermark(new Watermark(1200))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(1500, 0, "trade-3"), 1500)
      sc.emitWatermark(new Watermark(1500))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(1700, 0, "trade-4"), 1700)
      sc.emitWatermark(new Watermark(1700))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(1800, 0, "trade-5"), 1800)
      sc.emitWatermark(new Watermark(1800))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Trade(2000, 0, "trade-6"), 2000)
      sc.emitWatermark(new Watermark(2000))

      while (true) {
        Thread.sleep(1000)
      }
    })

    val customerStream = env.addSource((sc: SourceFunction.SourceContext[Customer]) => {
      sc.collectWithTimestamp(Customer(0, 0, "Customer data @ 0"), 0)
      sc.emitWatermark(new Watermark(0))
      Thread.sleep(2000)
      sc.collectWithTimestamp(Customer(500, 0, "Customer data @ 500"), 500)
      sc.emitWatermark(new Watermark(500))
      Thread.sleep(1000)
      sc.collectWithTimestamp(Customer(1500, 0, "Customer data @ 1500"), 1500)
      sc.emitWatermark(new Watermark(1500))
      Thread.sleep(6000)
      sc.collectWithTimestamp(Customer(1600, 0, "Customer data @ 1600"), 1600)
      sc.emitWatermark(new Watermark(1600))

      sc.emitWatermark(new Watermark(Long.MaxValue)) // do the rest

      while (true) {
        Thread.sleep(1000);
      }
    })

    val joinedStream = tradeStream
      .keyBy(_.customerId)
      .connect(customerStream.keyBy(_.customerId))
      .process(new EventTimeJoinFunction)

    joinedStream.print()

    env.execute
  }

  def tradeSource: TimeSkewedSource[Trade] = {
    val EventTimeSkew = 4
    TimeSkewedSource[Trade](
      maxEventTimeSkew = EventTimeSkew,
      intervalMs = 1000,
      generator = n => {
        val id = 0 //n % EventTimeSkew
        Trade(
          timestamp = n,
          customerId = id,
          tradeInfo = s"trade_${id}")
      })
  }

  def customerSource: TimeSkewedSource[Customer] = {
    val EventTimeSkew = 4
    TimeSkewedSource[Customer](
      maxEventTimeSkew = EventTimeSkew,
      intervalMs = 10000,
      generator = n => {
        val id = 0 //n % EventTimeSkew
        Customer(
          timestamp = n,
          customerId = id,
          customerInfo = s"customer_${id}")
      })
  }
}

class EventTimeJoinFunction extends RichCoProcessFunction[Trade, Customer, EnrichedTrade] {

  private var tradeBufferState: ValueState[mutable.PriorityQueue[Trade]] = null
  private var customerBufferState: ListState[Customer] = null

  // TODO: This will not work -- need to keep this value per trade
  private var initialJoinResultState: ValueState[EnrichedTrade] = null

  override def open(parameters: Configuration): Unit = {
    implicit val tradeOrdering = Ordering.by((t: Trade) => t.timestamp)
    implicit val customerOrdering = Ordering.by((c: Customer) => c.timestamp)
    tradeBufferState = getRuntimeContext.getState(new ValueStateDescriptor[mutable.PriorityQueue[Trade]]("tradeBuffer", createTypeInformation[mutable.PriorityQueue[Trade]], new mutable.PriorityQueue[Trade]))
    customerBufferState = getRuntimeContext.getListState(new ListStateDescriptor[Customer]("customerBuffer", createTypeInformation[Customer]))
    initialJoinResultState = getRuntimeContext.getState(new ValueStateDescriptor[EnrichedTrade]("initialJoinResult", createTypeInformation[EnrichedTrade], null))
  }

  override def processElement1(trade: Trade, context: Context, collector: Collector[EnrichedTrade]): Unit = {
    val timerService = context.timerService()
    val joinedData = join(trade)
    initialJoinResultState.update(joinedData)
    collector.collect(joinedData)
    if (timerService.currentWatermark() < context.timestamp()) {
      val tradeBuffer = tradeBufferState.value()
      tradeBuffer.enqueue(trade)
      tradeBufferState.update(tradeBuffer)
      timerService.registerEventTimeTimer(trade.timestamp)
    }
  }

  override def processElement2(customer: Customer, context: Context, collector: Collector[EnrichedTrade]): Unit = {
    customerBufferState.add(customer)
  }

  override def onTimer(l: Long, context: OnTimerContext, collector: Collector[EnrichedTrade]): Unit = {
    // look for trades that can be completed now
    val tradeBuffer = tradeBufferState.value()
    val watermark = context.timerService().currentWatermark()
    while (tradeBuffer.headOption.map(_.timestamp).getOrElse(Long.MaxValue) <= watermark) {
      val trade = tradeBuffer.dequeue()
      val joinedData = join(trade)
      if (!joinedData.equals(initialJoinResultState.value())) {
        collector.collect(joinedData)
      }
    }
  }

  private def join(trade: Trade): EnrichedTrade = {
    // get the customer info that was in effect at the time of this trade
    // doing this rather than jumping straight to the latest known info makes
    // this 100% deterministic.  If that's not a strict requirement we can simplify
    // this by joining against the latest available data right now.

    // TODO: Still need to clean up stale customer data

    val customerBuffer: Array[Customer] = customerBufferState.get.asScala.toArray
    val customerInfo = customerBuffer
      .filter(_.timestamp <= trade.timestamp)
      .sortBy(_.timestamp)
      .reverse
      .map(_.customerInfo)
      .headOption
      .getOrElse("No customer info available")

    EnrichedTrade(trade, customerInfo)
  }
}
