package playground.netflix

import java.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.CoGroupedStreams
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner.MergeCallback
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner.WindowAssignerContext
import org.apache.flink.streaming.api.windowing.triggers.Trigger.{OnMergeContext, TriggerContext}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}

import scala.collection.JavaConverters._

case class PlaybackEvent(timestamp: Long, userId: Long)

case class MapEvent(timestamp: Long, userId: Long, sessionId: Long)

case class SessionWindow(start: Long, end: Long, sessionId: Long) extends TimeWindow(start, end) {
  def cover(other: SessionWindow): SessionWindow = {
    SessionWindow(Math.min(start, other.start), Math.max(end, other.end), sessionId)
  }

  override def equals(o: scala.Any): Boolean = {
    super.equals(o) && o.asInstanceOf[SessionWindow].sessionId.equals(this.sessionId)
  }

  override def hashCode(): Int = super.hashCode() * 41 + sessionId.toInt

  override def toString: String = {
    s"SessionWindow(start = $start, end = $end, sessionId = $sessionId)"
  }
}

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

class SessionMergingWindowAssigner extends MergingWindowAssigner[CoGroupedStreams.TaggedUnion[PlaybackEvent, MapEvent], SessionWindow] {

  override def mergeWindows(windows: util.Collection[SessionWindow], callback: MergeCallback[SessionWindow]): Unit = {
    val scalaWindows = windows.asScala
    val (playbackWindows, sessionWindows) = scalaWindows.partition(_.sessionId == 0)
    val groupedSessions: Map[Long, Iterable[SessionWindow]] = sessionWindows.groupBy(_.sessionId)

    playbackWindows.foreach(pw => {
      // find matching session windows
      val matchingSessionWindows = sessionWindows.filter(sw => sw.start == pw.start && sw.end == pw.end)
      matchingSessionWindows.foreach(sw => {
        val mergedWindows = new util.LinkedList[SessionWindow]()
        mergedWindows.add(sw)
        mergedWindows.add(pw)
        callback.merge(mergedWindows, sw)
      })
    })

    /*
    groupedSessions.foreach(group => {
      val timeWindows: Iterable[SessionWindow] = group._2 ++ playbackWindows
      SessionWindowUtil.mergeWindows(timeWindows.asJavaCollection, callback)

    })
    */
  }

  override def isEventTime: Boolean = true

  override def assignWindows(element: TaggedUnion[PlaybackEvent, MapEvent], timestamp: Long, context: WindowAssignerContext): util.Collection[SessionWindow] = {
    val sessionId = if (element.isOne) 0L else element.getTwo.sessionId
    val windowStart = timestamp - (timestamp % 10000)
    Seq(SessionWindow(windowStart, windowStart + 10000, sessionId)).asJava
  }

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[SessionWindow] = createTypeInformation[SessionWindow].createSerializer(executionConfig)

  override def getDefaultTrigger(env: environment.StreamExecutionEnvironment): Trigger[TaggedUnion[PlaybackEvent, MapEvent], SessionWindow] = new SessionMergingEventTimeTrigger

}

// This is basically just a copy of the standard EventTimeTrigger -- but using my custom window type
class SessionMergingEventTimeTrigger extends Trigger[TaggedUnion[PlaybackEvent, MapEvent], SessionWindow] {
  override def onElement(element: TaggedUnion[PlaybackEvent, MapEvent], timestamp: Long, window: SessionWindow, ctx: TriggerContext): TriggerResult = {
    if (window.maxTimestamp() <= ctx.getCurrentWatermark) {
      TriggerResult.FIRE
    }
    else {
      ctx.registerEventTimeTimer(window.maxTimestamp())
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: SessionWindow, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: SessionWindow, ctx: TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp()) TriggerResult.FIRE else TriggerResult.CONTINUE
  }

  override def onMerge(window: SessionWindow, ctx: OnMergeContext): Unit = {
    ctx.registerEventTimeTimer(window.maxTimestamp())
  }

  override def canMerge: Boolean = true

  override def clear(window: SessionWindow, ctx: TriggerContext): Unit = {
    ctx.deleteEventTimeTimer(window.maxTimestamp())
  }

  override def toString: String = "SessionMergingEventTimeTimer"
}
