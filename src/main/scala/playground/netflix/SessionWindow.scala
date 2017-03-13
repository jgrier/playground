package playground.netflix

import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class SessionWindow(start: Long, end: Long, sessionId: Long) extends TimeWindow(start, end) {
  def cover(other: SessionWindow): SessionWindow = {
    SessionWindow(Math.min(start, other.start), Math.max(end, other.end), sessionId)
  }

  override def equals(o: scala.Any): Boolean = {
    super.equals(o) && o.asInstanceOf[SessionWindow].sessionId.equals(this.sessionId)
  }

  override def hashCode(): Int = super.hashCode() * 41 + sessionId.toInt

  override def toString: String = {
    s"SessionWindow(start = $start, end = $end, sessionId = $sessionId)"
  }
}
