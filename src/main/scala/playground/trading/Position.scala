package playground.trading

import scala.util.Try

object Position {
  def fromString(s: String, timeOffsetMode: Boolean): Try[Position] = {
    val parts = s.split(",")
    Try(
      new Position(
        timestamp = System.currentTimeMillis(),
        symbol = parts(0),
        quantity = parts(1).toLong,
        buyPrice = parts(2).toFloat,
        askPrice = parts(2).toFloat,
        tradePrice = parts(2).toFloat,
        profit = 0.0f,
        expirationMillis =
          if (parts(3).toLong != 0) {
            if (timeOffsetMode) System.currentTimeMillis() + parts(3).toLong else parts(3).toLong
          } else 0L
      )
    )
  }
}

case class Position(timestamp: Long, symbol: String, quantity: Long, buyPrice: Float, askPrice: Float, tradePrice: Float, profit: Float, expirationMillis: Long) {
  def value = quantity * buyPrice

  override def toString: String = {
    s"${symbol},${quantity},${buyPrice},${expirationMillis}"
  }
}