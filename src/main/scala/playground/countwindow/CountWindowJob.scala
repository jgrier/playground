package playground.countwindow

import javax.lang.model.util.Elements

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object CountWindowJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource((sc: SourceContext[Long]) => {
      var i = 0
      while(true){
        Thread.sleep(100)
        sc.collect(i)
        i+=1
      }
    })

    val windowedStream = stream
      .countWindowAll(10,1)
      .apply((window: GlobalWindow, elements: Iterable[Long], collector: Collector[String]) => {
        collector.collect(elements.toString)
      })
      .print


    env.execute()
  }
}

