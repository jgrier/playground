package playground.trading

import scala.util.Try

object Bid {
  def fromString(s: String): Try[Bid] = {
    val parts = s.split(",")
    Try(
      new Bid(
        parts(0),
        parts(1).toLong,
        parts(2).toFloat
      )
    )
  }
}

case class Bid(symbol: String, quantity: Long, price: Float) {
  def value = quantity * price
}
