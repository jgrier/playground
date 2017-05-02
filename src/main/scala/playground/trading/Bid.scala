package playground.trading

import scala.util.Try

object Bid {
  def fromString(s: String): Try[Bid] = {
    val parts = s.split(",")
    Try(
      new Bid(
        timestamp = System.currentTimeMillis(),
        symbol = parts(0),
        quantity = parts(1).toLong,
        price = parts(2).toFloat
      )
    )
  }
}

case class Bid(timestamp: Long, symbol: String, quantity: Long, price: Float) {
  def value = quantity * price

  override def toString: String = {
    s"${symbol},${quantity},${price}"
  }
}
