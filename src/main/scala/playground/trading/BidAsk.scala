package playground.trading

import scala.util.Try

object BidAsk {
  def fromString(s: String): Try[BidAsk] = {
    val parts = s.split(",")
    Try(
      new BidAsk(
        parts(0),
        parts(1).toLong,
        parts(2).toFloat
      )
    )
  }
}

case class BidAsk(symbol: String, quantity: Long, price: Float) {
  def value = quantity * price
  def isBid = quantity > 0
}
