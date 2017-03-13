package playground.jgrier

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object MultiparallelismJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val stream = env.addSource((sc: SourceContext[(String, Long)]) => {
      var i = 0
      while (true) {
        Thread.sleep(1000)
        sc.collect((i.toString, 1))
        i += 1
        i = i % 10
      }
    })

    stream
      .keyBy(0)
      .map(new PrintingMapFunction).setParallelism(10)
      .print

    env.execute()
  }
}

class PrintingMapFunction extends RichMapFunction[(String, Long), (String, Long)] {
  override def map(in: (String, Long)): (String, Long) = {
    val idx = getRuntimeContext.getIndexOfThisSubtask
    println(s"${idx}--> ${in}")
    in
  }
}