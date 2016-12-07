package playground.lowlatencyjoin

import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.types.LongValue

import scala.util.Random

object TimeSkewedSource {
  def apply[T](maxEventTimeSkew: Long, intervalMs: Long, generator: Long => T): TimeSkewedSource[T] = {
    new TimeSkewedSource[T](maxEventTimeSkew, intervalMs, generator)
  }
}

class TimeSkewedSource[T](maxEventTimeSkew: Long, intervalMs: Long, generator: Long => T) extends RichParallelSourceFunction[T] with Checkpointed[LongValue] {

  @volatile var running = true

  // state!
  var currentElement: Long = 0

  override def cancel(): Unit = {
    running = false;
  }

  override def run(ctx: SourceContext[T]): Unit = {
    while (running) {
      val timestamps = nextRandomSequence()
      ctx.getCheckpointLock.synchronized {
        timestamps.foreach(t => ctx.collectWithTimestamp(generator(t), t))
        currentElement += maxEventTimeSkew
        ctx.emitWatermark(new Watermark(currentElement-1))
      }
      Thread.sleep(intervalMs)
    }
  }

  private def nextRandomSequence(): Seq[Long] = {
    Random.shuffle(Range.Long(currentElement, currentElement + maxEventTimeSkew, 1).toSeq)
  }

  override def restoreState(state: LongValue): Unit = {
    currentElement = state.getValue;
  }

  override def snapshotState(checkpointId: Long, checkpointTimestamp: Long): LongValue = {
    new LongValue(currentElement)
  }
}
