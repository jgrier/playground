package playground.netflix

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.CoGroupedStreams
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner.MergeCallback
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner.WindowAssignerContext
import org.apache.flink.streaming.api.windowing.triggers.Trigger

import scala.collection.JavaConverters._

class SessionMergingWindowAssigner extends MergingWindowAssigner[CoGroupedStreams.TaggedUnion[PlaybackEvent, MapEvent], SessionWindow] {

  override def mergeWindows(windows: java.util.Collection[SessionWindow], callback: MergeCallback[SessionWindow]): Unit = {
    val scalaWindows = windows.asScala
    val (playbackWindows, sessionWindows) = scalaWindows.partition(_.sessionId == 0)
    val groupedSessions: Map[Long, Iterable[SessionWindow]] = sessionWindows.groupBy(_.sessionId)

    playbackWindows.foreach(pw => {
      // find matching session windows
      val matchingSessionWindows = sessionWindows.filter(sw => sw.start == pw.start && sw.end == pw.end)
      matchingSessionWindows.foreach(sw => {
        val mergedWindows = new java.util.LinkedList[SessionWindow]()
        mergedWindows.add(sw)
        mergedWindows.add(pw)
        callback.merge(mergedWindows, sw)
      })
    })
  }

  override def isEventTime: Boolean = true

  override def assignWindows(element: TaggedUnion[PlaybackEvent, MapEvent], timestamp: Long, context: WindowAssignerContext): java.util.Collection[SessionWindow] = {
    val sessionId = if (element.isOne) 0L else element.getTwo.sessionId
    val windowStart = timestamp - (timestamp % 10000)
    Seq(SessionWindow(windowStart, windowStart + 10000, sessionId)).asJava
  }

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[SessionWindow] = createTypeInformation[SessionWindow].createSerializer(executionConfig)

  override def getDefaultTrigger(env: StreamExecutionEnvironment): Trigger[TaggedUnion[PlaybackEvent, MapEvent], SessionWindow] = new SessionMergingEventTimeTrigger

}
