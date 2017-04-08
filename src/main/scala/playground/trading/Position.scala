package playground.trading

import scala.util.Try

object Position {
  def fromString(s: String): Try[Position] = {
    val parts = s.split(",")
    Try(
      new Position(
        parts(0),
        parts(1).toLong,
        parts(2).toFloat,
        parts(2).toFloat,
        parts(2).toFloat,
        0.0f,
        if (parts(3).toLong != 0) System.currentTimeMillis() + parts(3).toLong else 0L
      )
    )
  }
}

case class Position(symbol: String, quantity: Long, buyPrice: Float, askPrice: Float, tradePrice: Float, profit: Float, expirationMillis: Long) {
  def value = quantity * buyPrice
}