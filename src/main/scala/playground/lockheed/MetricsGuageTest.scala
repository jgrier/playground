package playground.lockheed

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Gauge
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark

object MetricsGaugeTest {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource((sc: SourceContext[String]) => {
      var i = 0;
      while (true) {
        Thread.sleep(1000)
        sc.collect(i.toString)
        i += 1
      }
    })


    stream
        .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[String] {

          override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = ???

          override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = ???
        })
      .map(new MyMapper())

    stream.print

    env.execute()
  }

  class MyMapper extends RichMapFunction[String, Int] {
    private val valueToExpose = 0

    override def map(value: String): Int = value.toInt

    override def open(parameters: Configuration): Unit = {
      getRuntimeContext
        .getMetricGroup
        .gauge[Int, Gauge[Int]]("MyGauge", new Gauge[Int]() {
        override def getValue: Int = valueToExpose
      })
    }
  }

}
