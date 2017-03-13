package playground.jgrier.statemigration

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark

object StateMigrationJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(1000)

    val stream = env.addSource((sc: SourceContext[Long]) => {
      var i = 0L
      while (true) {
        sc.collectWithTimestamp(i, i)
        sc.emitWatermark(new Watermark(i))
        i += 1
        Thread.sleep(1000)
      }
    })


    stream
      .map((_, 1L))
      .keyBy(0)
      .map(new StatefulMapper).uid("stateful-mapper-1")
      .filter(_ => true).uid("filter")
      .startNewChain()
      .print()

    env.execute()
  }
}

class StatefulMapper extends RichMapFunction[(Long, Long), (Long, Long)] {
  val stateDesc = new ValueStateDescriptor[Long]("lastValue", createTypeInformation[Long], 0L)

  override def map(value: (Long, Long)): (Long, Long) = {
    getRuntimeContext.getState(stateDesc).update(value._1)
    value
  }
}
