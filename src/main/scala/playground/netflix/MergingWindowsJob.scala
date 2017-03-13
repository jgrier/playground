package playground.netflix

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark

object MergingWindowsJob {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val playbackEventStream = env.addSource((sc: SourceFunction.SourceContext[PlaybackEvent]) => {
      var time = 0
      while (true) {
        sc.collectWithTimestamp(PlaybackEvent(time, 1), time)
        sc.emitWatermark(new Watermark(time))
        Thread.sleep(1000)
        time += 1000
      }
    })

    val mapEventStream = env.addSource((sc: SourceFunction.SourceContext[MapEvent]) => {
      var time = 0
      while (true) {
        sc.collectWithTimestamp(MapEvent(time, 1, 1), time)
        sc.collectWithTimestamp(MapEvent(time, 1, 2), time)
        sc.emitWatermark(new Watermark(time))
        Thread.sleep(1000)
        time += 1000
      }
    })

    val joinedStream = playbackEventStream
      .coGroup(mapEventStream)
      .where(_.userId)
      .equalTo(_.userId)
      .window(new SessionMergingWindowAssigner)
      .apply((p, m) => (p, m))

    joinedStream.print

    env.execute()
  }
}




