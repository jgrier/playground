package playground.trading

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.scala._

class PositionSerializationSchema extends SerializationSchema[Position] with DeserializationSchema[Position] {
  override def serialize(element: Position): Array[Byte] = {
    element.toString.getBytes
  }

  override def isEndOfStream(nextElement: Position): Boolean = false

  override def deserialize(message: Array[Byte]): Position = Position.fromString(new String(message), timeOffsetMode = false).get

  override def getProducedType: TypeInformation[Position] = createTypeInformation[Position]
}
