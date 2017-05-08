package playground.trading

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark

class WatermarkSource(periodMs: Long) extends RichParallelSourceFunction[Long]{
  @volatile private var _running: Boolean = true
  val epoch = System.currentTimeMillis()
  var seq = 0L

  override def cancel(): Unit = _running = false

  override def run(ctx: SourceContext[Long]): Unit = {
    while(_running){
      val wm = calcWatermark(seq)
      ctx.collectWithTimestamp(0, wm.getTimestamp)
      ctx.emitWatermark(wm)
      val nextWatermark = calcWatermark(seq + 1)
      Thread.sleep(nextWatermark.getTimestamp - System.currentTimeMillis())
      seq += 1
    }
  }

  private def calcWatermark(seq: Long): Watermark = {
    new Watermark(epoch + seq * periodMs)
  }
}
