package playground.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object BatchJob {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.readTextFile("/Users/jgrier/tmp/batch")
    val lineCount: Long = data.count()
    println(s"lineCount = ${lineCount}")

//    env.execute()
  }
}
