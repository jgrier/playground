package playground.trading

import org.apache.flink.streaming.api.functions.co.RichCoMapFunction

class WatermarkerFunction[T](elementFactory: (T, Long) => T,
                             watermarkFactory: Long => T) extends RichCoMapFunction[T, Long, T] {

  private var watermark: Long = 0;

  override def map1(t: T): T = {
    watermark += 1 // a bit of a hack
    elementFactory(t, watermark)
  }

  override def map2(watermark: Long): T = {
    this.watermark = watermark
    watermarkFactory(watermark)
  }
}
