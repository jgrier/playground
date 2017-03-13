package playground.netflix

import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.windowing.triggers.Trigger.{OnMergeContext, TriggerContext}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}

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
