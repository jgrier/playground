package playground.jgrier

import org.apache.flink.cep.scala.{PatternStream, CEP}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class ObservationEvent(id: Long, value: Long)

object CepExampleJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource((sc: SourceContext[ObservationEvent]) => {
      var i = 0
      while (true) {
        Thread.sleep(100)
        sc.collect(ObservationEvent(i % 10, i))
        i += 1
      }
    })

    val windowedStream: DataStream[ObservationEvent] = stream.keyBy(_.id).countWindow(10).apply((key, window, elements, collector: Collector[ObservationEvent]) => collector.collect(ObservationEvent(1,1)))

    val pattern = Pattern
      .begin[ObservationEvent]("item1")
      .where(e => {println("where()"); false})

    val matchStream: PatternStream[ObservationEvent] = CEP.pattern(windowedStream.keyBy(_.id), pattern)

    val complexEvents: DataStream[Either[String, String]] = matchStream.select((matchedEvents, timestamp) => "timeout")(matchedEvents => "matched")

    complexEvents.print()

    env.execute()
  }
}
