package playground.trading

import java.lang.{Exception, Throwable}
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.query.QueryableStateClient
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

object PositionQuery {
  def main(args: Array[String]) {

    // Parse command line args
    val parameterTool = ParameterTool.fromArgs(args)
    val jobId = JobID.fromHexString(parameterTool.get("job"))
    val symbols = parameterTool.get("symbols").split(",")

    val config = new Configuration()
    config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost")

    val client = new QueryableStateClient(config)
    val execConfig = new ExecutionConfig

    // Create de-serializers
    val keySerializer = createTypeInformation[String].createSerializer(execConfig)
    val valueSerializer = createTypeInformation[Position].createSerializer(execConfig)

    val positions = for (symbol <- symbols) yield {

      // serialize the key
      val serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
        symbol,
        keySerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE)

      // make the query
      val serializedResult = client.getKvState(jobId, "position", symbol.hashCode(), serializedKey)

      // now wait for the result and return it
      try {
        val serializedValue = Await.result(serializedResult, FiniteDuration(1, TimeUnit.SECONDS))
        val value = KvStateRequestSerializer.deserializeValue(serializedValue, valueSerializer)
        List(value.symbol, value.quantity, value.buyPrice, value.askPrice, value.tradePrice, value.profit)
      } catch {
        case e: Exception => List()
      }
    }

    val headings = List("SYMBOL", "SHARES", "BUY PRICE", "ASK PRICE", "LAST TRADE PRICE", "PROFIT")
    print(Tabulator.format(headings +: positions.toList.filter(_.nonEmpty)))
  }
}
