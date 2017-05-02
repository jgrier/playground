package playground.trading

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

class BidSerializationSchema extends SerializationSchema[Bid] with DeserializationSchema[Bid] {
  override def serialize(bid: Bid): Array[Byte] = {
    bid.toString.getBytes
  }

  override def isEndOfStream(nextElement: Bid): Boolean = false

  override def deserialize(message: Array[Byte]): Bid = Bid.fromString(new String(message)).get

  override def getProducedType: TypeInformation[Bid] = createTypeInformation[Bid]
}
