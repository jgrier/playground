package playground.southwest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction.{Context, OnTimerContext}
import org.apache.flink.streaming.api.functions.RichProcessFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * Represents an event
  */
case class Event(id: Long) {}

/**
  * Represents a notification
  */
case class Notification() {}

/**
  * Test job to show how to use a ProcessFunction to implement a very simple
  * state machine that will notify if an event isn't followed by a second one
  * withing a configurable amount of time
  */
object AbsentEventJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val ActualDelayBeetweenEvents = 5000
    val ExpectedDelayBetweenEvents = 1000

    val eventStream = env.addSource(new EventSourceFunction(ActualDelayBeetweenEvents))

    eventStream
      .keyBy(_.id)
      .process(new ExpectTwoFunction(ExpectedDelayBetweenEvents))
      .print

    env.execute()
  }
}

/**
  * Generates source data for testing
  * @param delayMs
  */
class EventSourceFunction(delayMs: Long) extends RichSourceFunction[Event] {

  var running = true

  override def cancel(): Unit = running = false;

  override def run(ctx: SourceContext[Event]): Unit = {
    while (running) {
      ctx.collect(Event(0))
      Thread.sleep(delayMs)
    }
  }
}

/**
  * The ProcessFunction -- This expects a second event to follow within the given timeout, and emits
  * an alert if it doesn't
  * @param timeoutMs - The timeout. If this expires before seeing the second event emit a notification
  */
class ExpectTwoFunction(timeoutMs: Long) extends RichProcessFunction[Event, Notification] {

  // The state we use to keep track of how many events we've seen
  var eventCountState: ValueState[Long] = null

  override def open(parameters: Configuration): Unit = {
    // register some state with Flink
    eventCountState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long](
        "eventCount",
        implicitly[TypeInformation[Long]],
        0L))
  }

  override def processElement(value: Event, ctx: Context, out: Collector[Notification]): Unit = {

    val eventCount = eventCountState.value()

    if (eventCount == 0) {
      // register a callback for timeoutMs milliseconds from now, if we haven't received a second event
      // by then send a notification -- otherwise do nothing
      ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + timeoutMs)
    }

    // Keep track of how many events we've seen so far
    eventCountState.update(eventCount + 1L)
  }

  override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[Notification]): Unit = {
    val eventCount = eventCountState.value()

    if (eventCount < 2) {
      out.collect(Notification())
    }
    eventCountState.update(0L)  // reset state machine
  }
}
