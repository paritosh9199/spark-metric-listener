package com.spark.metrics

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import org.apache.spark.scheduler.{AccumulableInfo, SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerExecutorMetricsUpdate, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

class SparkMetricListener extends SparkListener {
  private val jobsCompleted = new AtomicInteger(0)
  private val stagesCompleted = new AtomicInteger(0)
  private val tasksCompleted = new AtomicInteger(0)
  private val executorRuntime = new AtomicLong(0L)
  private val recordsRead = new AtomicLong(0L)
  private val recordsWritten = new AtomicLong(0L)

  implicit val formats: DefaultFormats = DefaultFormats

  private var logs: String = ""
  private var applicationId: Option[String] = Some("NA")
  private var appName: String = ""
  private var appAttemptId: Option[String] = Some("NA")

  def addToLogs(logStr: String): Unit = {
    logs += s"$logStr\n"  
  }

  def getLogs(): String = {
    logs
  }
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    addToLogs("***************** Aggregate metrics *****************************")
    addToLogs(s"* Application ID: ${applicationId}")
    addToLogs(s"* Jobs = ${jobsCompleted.get()}, Stages = ${stagesCompleted.get()}, Tasks = ${tasksCompleted}")
    addToLogs(s"* Executor runtime = ${executorRuntime.get()}ms, Records Read = ${recordsRead.get()}, Records written = ${recordsWritten.get()}")
    addToLogs("*****************************************************************")

    print(getLogs())
  }

  def writeAsJsn[A](obj: A): String = {
//    write(obj)
    ""
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    applicationId = applicationStart.appId
    appName = applicationStart.appName
    appAttemptId = applicationStart.appAttemptId

    addToLogs(
      s"""
         |***************** Driver metrics ********************************
         |Application ID: ${applicationId.get}
         |Application Name: $appName
         |Driver attributes(Does not exist in 2.4.0): BLANK
         |App attempt ID: $appAttemptId
         |Driver logs: ${applicationStart.driverLogs}
         |*****************************************************************
         |""".stripMargin)

    addToLogs(write(applicationStart))
    //    super.onApplicationStart(applicationStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    addToLogs(write(jobEnd))
    jobsCompleted.incrementAndGet()
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    addToLogs(write(stageCompleted))
    stagesCompleted.incrementAndGet()
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    var accumulatedUpdates = executorMetricsUpdate
      .accumUpdates
      .map(f => {
        var p1 = f._1
        var p2 = f._2
        var p3 = f._3
        var p4 = f._4

        var p4Parsed = parseSeqOfAccumInfo(p4)

        s"""|=> Task ID: ${p1}| Stage ID: ${p2}| Stage Attempt ID: ${p3}
            |   accumUpdates:
            |${p4Parsed}
            |""".stripMargin
      }).mkString("\n")

    addToLogs(
      s"""*****************************************************************
         |Executor metrics update
         | => Exec id: ${executorMetricsUpdate.execId}
         | => AccumulatedUpdates:
         | -----------------------
         | START
         | ${accumulatedUpdates}
         | END
         | -----------------------
         |*****************************************************************
         |""".stripMargin)

    addToLogs(write(executorMetricsUpdate))

    super.onExecutorMetricsUpdate(executorMetricsUpdate)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    var a = taskEnd.reason


    addToLogs(
      s"""
         |*****************************************************************
         |onTaskEnd
         |
         |=> taskEnd.stageId: ${taskEnd.stageId}
         |=> taskEnd.stageAttemptId: ${taskEnd.stageAttemptId}
         |=> taskEnd.reason: ${taskEnd.reason}
         |=> taskEnd.taskType: ${taskEnd.taskType}
         |=> taskEnd.taskMetrics.inputMetrics.recordsRead: ${taskEnd.taskMetrics.inputMetrics.recordsRead}
         |=> taskEnd.taskMetrics.inputMetrics.bytesRead: ${taskEnd.taskMetrics.inputMetrics.bytesRead}
         |=> taskEnd.taskMetrics.outputMetrics.recordsWritten: ${taskEnd.taskMetrics.outputMetrics.recordsWritten}
         |=> taskEnd.taskMetrics.outputMetrics.bytesWritten: ${taskEnd.taskMetrics.outputMetrics.bytesWritten}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.recordsRead: ${taskEnd.taskMetrics.shuffleReadMetrics.recordsRead}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead: ${taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead: ${taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.localBytesRead: ${taskEnd.taskMetrics.shuffleReadMetrics.localBytesRead}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk: ${taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime: ${taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.localBlocksFetched: ${taskEnd.taskMetrics.shuffleReadMetrics.localBlocksFetched}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.remoteBlocksFetched: ${taskEnd.taskMetrics.shuffleReadMetrics.remoteBlocksFetched}
         |=> taskEnd.taskMetrics.shuffleReadMetrics.totalBlocksFetched: ${taskEnd.taskMetrics.shuffleReadMetrics.totalBlocksFetched}
         |=> taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten: ${taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten}
         |=> taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten: ${taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten}
         |=> taskEnd.taskMetrics.shuffleWriteMetrics.writeTime: ${taskEnd.taskMetrics.shuffleWriteMetrics.writeTime}
         |=> taskEnd.taskInfo.taskId: ${taskEnd.taskInfo.taskId}
         |=> taskEnd.taskInfo.index: ${taskEnd.taskInfo.index}
         |=> taskEnd.taskInfo.attemptNumber: ${taskEnd.taskInfo.attemptNumber}
         |=> taskEnd.taskInfo.launchTime: ${taskEnd.taskInfo.launchTime}
         |=> taskEnd.taskInfo.executorId: ${taskEnd.taskInfo.executorId}
         |=> taskEnd.taskInfo.host: ${taskEnd.taskInfo.host}
         |=> taskEnd.taskInfo.taskLocality: ${taskEnd.taskInfo.taskLocality}
         |=> taskEnd.taskInfo.speculative: ${taskEnd.taskInfo.speculative}
         |=> taskEnd.taskInfo.gettingResult: ${taskEnd.taskInfo.gettingResult}
         |=> taskEnd.taskInfo.id: ${taskEnd.taskInfo.id}
         |=> taskEnd.taskInfo.status: ${taskEnd.taskInfo.status}
         |=> taskEnd.taskInfo.duration: ${taskEnd.taskInfo.duration}
         |=> taskEnd.taskInfo.finished: ${taskEnd.taskInfo.finished}
         |=> taskEnd.taskInfo.accumulables:
         |${parseSeqOfAccumInfo(taskEnd.taskInfo.accumulables)}
         |*****************************************************************
         |""".stripMargin)

    addToLogs(write(taskEnd))


    //    addToLogs("***************** Task end metric *****************************")
    //    addToLogs(taskEnd.toString)
    //    addToLogs(s"* Application ID: ${applicationId}")
    //    addToLogs(s"* Jobs = ${jobsCompleted.get()}, Stages = ${stagesCompleted.get()}, Tasks = ${tasksCompleted}")
    //    addToLogs(s"* Executor runtime = ${executorRuntime.get()}ms, Records Read = ${recordsRead.get()}, Records written = ${recordsWritten.get()}")
    //    addToLogs("*****************************************************************")
    tasksCompleted.incrementAndGet()
    executorRuntime.addAndGet(taskEnd.taskMetrics.executorRunTime)
    recordsRead.addAndGet(taskEnd.taskMetrics.inputMetrics.recordsRead)
    recordsWritten.addAndGet(taskEnd.taskMetrics.outputMetrics.recordsWritten)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    addToLogs(
      s"""
         |*****************************************************************
         |onTaskGettingResult
         |
         |=> taskGettingResult.taskInfo.taskId: ${taskGettingResult.taskInfo.taskId}
         |=> taskGettingResult.taskInfo.index: ${taskGettingResult.taskInfo.index}
         |=> taskGettingResult.taskInfo.attemptNumber: ${taskGettingResult.taskInfo.attemptNumber}
         |=> taskGettingResult.taskInfo.launchTime: ${taskGettingResult.taskInfo.launchTime}
         |=> taskGettingResult.taskInfo.executorId: ${taskGettingResult.taskInfo.executorId}
         |=> taskGettingResult.taskInfo.host: ${taskGettingResult.taskInfo.host}
         |=> taskGettingResult.taskInfo.taskLocality: ${taskGettingResult.taskInfo.taskLocality}
         |=> taskGettingResult.taskInfo.speculative: ${taskGettingResult.taskInfo.speculative}
         |=> taskGettingResult.taskInfo.gettingResult: ${taskGettingResult.taskInfo.gettingResult}
         |=> taskGettingResult.taskInfo.id: ${taskGettingResult.taskInfo.id}
         |=> taskGettingResult.taskInfo.status: ${taskGettingResult.taskInfo.status}
         |=> taskGettingResult.taskInfo.duration: ${taskGettingResult.taskInfo.duration}
         |=> taskGettingResult.taskInfo.finished: ${taskGettingResult.taskInfo.finished}
         |=> taskGettingResult.taskInfo.accumulables:
         |${parseSeqOfAccumInfo(taskGettingResult.taskInfo.accumulables)}
         |*****************************************************************
         |""".stripMargin)

    addToLogs(write(taskGettingResult))
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    addToLogs(
      s"""
         |*****************************************************************
         |onTaskStart
         |
         |=> taskStart.stageId: ${taskStart.stageId}
         |=> taskStart.stageAttemptId: ${taskStart.stageAttemptId}
         |=> taskStart.taskInfo.taskId: ${taskStart.taskInfo.taskId}
         |=> taskStart.taskInfo.index: ${taskStart.taskInfo.index}
         |=> taskStart.taskInfo.attemptNumber: ${taskStart.taskInfo.attemptNumber}
         |=> taskStart.taskInfo.launchTime: ${taskStart.taskInfo.launchTime}
         |=> taskStart.taskInfo.executorId: ${taskStart.taskInfo.executorId}
         |=> taskStart.taskInfo.host: ${taskStart.taskInfo.host}
         |=> taskStart.taskInfo.taskLocality: ${taskStart.taskInfo.taskLocality}
         |=> taskStart.taskInfo.speculative: ${taskStart.taskInfo.speculative}
         |=> taskStart.taskInfo.gettingResult: ${taskStart.taskInfo.gettingResult}
         |=> taskStart.taskInfo.id: ${taskStart.taskInfo.id}
         |=> taskStart.taskInfo.status: ${taskStart.taskInfo.status}
         |=> taskStart.taskInfo.finished: ${taskStart.taskInfo.finished}
         |=> taskStart.taskInfo.accumulables:
         |${parseSeqOfAccumInfo(taskStart.taskInfo.accumulables)}
         |*****************************************************************
         |""".stripMargin)

    addToLogs(write(taskStart))
    //    => taskStart.taskInfo.duration: ${taskStart.taskInfo.duration}
  }

  def parseSeqOfAccumInfo(accumInfo: Seq[AccumulableInfo]): String = {
    if (accumInfo.length > 0)
      accumInfo
        .map(x => {
          s"   +- ID: ${x.id} | Name:   ${x.name} | Value:  ${x.value} | Update: ${x.update}"
        }).mkString("\n")
    else
      "[]"
  }
}
