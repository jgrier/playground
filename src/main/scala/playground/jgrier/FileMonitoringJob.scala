package playground.jgrier

import java.nio.file.StandardWatchEventKinds.ENTRY_CREATE
import java.nio.file._
import java.util

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.JavaConverters._
import scala.collection.mutable

object FileMonitoringJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val newFileStream = env.addSource(new FileWatcherJob("/tmp"))
    newFileStream.print
    env.execute()
  }
}

class FileWatcherJob(path: String) extends RichSourceFunction[String] {

  @volatile
  private var running = true

  @transient lazy val _path = FileSystems.getDefault.getPath(path)
  @transient lazy val watchService = _path.getFileSystem.newWatchService()

  override def open(parameters: Configuration): Unit = {
    _path.register(watchService, ENTRY_CREATE)
  }

  override def close(): Unit = {
    watchService.close()
  }

  def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (running) {
      val files = getNewFiles()
      files.foreach(file => {
        ctx.getCheckpointLock.synchronized {
          ctx.collect(file)
        }
      })
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  /**
    * Reimplement this to work on HDFS
    */
  private def getNewFiles(): mutable.Buffer[String] = {
    val watchKey = watchService.take()
    val events = watchKey.pollEvents().asInstanceOf[util.List[WatchEvent[Path]]].asScala
    val files: mutable.Buffer[String] = events.map(event => {
      event.kind match {
        case ENTRY_CREATE => {
          event.context().toString
        }
      }
    })
    watchKey.reset()
    files
  }

}
