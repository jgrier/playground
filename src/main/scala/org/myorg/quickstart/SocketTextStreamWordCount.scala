package org.myorg.quickstart

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SocketTextStreamWordCount {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val source1 = env.addSource(new IntegerSource(true))
      .assignAscendingTimestamps(t => t)

    val source2 = env.addSource(new IntegerSource(false))
      .assignAscendingTimestamps(t => t)

    val unionStream = source1.union(source2)

    unionStream
      .assignAscendingTimestamps(t => t)
      .timeWindowAll(Time.milliseconds(1))
      .sum(0)
      .print()

    env.execute("Playground")
  }
}


class IntegerSource(val emit: Boolean) extends RichParallelSourceFunction[Long] {
  private var continue = true;
  private var i = 0L;

  override def cancel(): Unit = continue = false

  override def run(ctx: SourceContext[Long]): Unit = {
    while (continue) {
      Thread.sleep(500)
      if (emit) {
        ctx.collect(i)
      }
      i += 1
    }
  }
}