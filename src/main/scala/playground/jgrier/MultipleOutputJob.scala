    package playground.jgrier

    import org.apache.flink.streaming.api.functions.source.SourceFunction
    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
    import org.apache.flink.api.scala._

    case class Type1(){}
    case class Type2(){}


    object MultipleOutputJob {
      def main(args: Array[String]) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // Stream of Type1
        val stream1 = env.addSource((sc: SourceFunction.SourceContext[Type1]) => {
          while(true){
            Thread.sleep(1000)
            sc.collect(Type1())
          }
        })

        // Mapping from Type1 to Type2
        val stream2 = stream1.map(t1 => Type2())

        // Collect both the original and the derived data
        stream1.print
        stream2.print

        env.execute()
      }
    }


