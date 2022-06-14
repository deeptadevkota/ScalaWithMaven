package EventManager

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}
import scalaj.http._


class EventManager (val appName: String ,
                    val appID: String ,
                    val HTTP_endpoint : String = "predefined_HTTP_endpoint") extends SparkListener {



  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println (
      s"-------------------------------------------------------------------------------------------------------------"+
        s"\n                          Job - ${jobStart.jobId} started" +
        s"\n-------------------------------------------------------------------------------------------------------------\n\n")

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println (
      s"-------------------------------------------------------------------------------------------------------------"+
        s"\n                          Job - ${jobEnd.jobId} finished" +
        s"\n-------------------------------------------------------------------------------------------------------------" +
        s"\n                          Job Result: ${jobEnd.jobResult} " +
        s"\n-------------------------------------------------------------------------------------------------------------\n\n\n")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println (s"-------------------------------------------------------------------------------------------------------------"+
      s"\n                          Stage - ${stageSubmitted.stageInfo.stageId} submitted" +
      s"\n-------------------------------------------------------------------------------------------------------------"+
      s"\n                          Stage name: ${stageSubmitted.stageInfo.name} " +
      s"\n-------------------------------------------------------------------------------------------------------------\n\n\n")
  }
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println (
      s"-------------------------------------------------------------------------------------------------------------"+
        s"\n                          Stage - ${stageCompleted.stageInfo.stageId} completed" +
        s"\n-------------------------------------------------------------------------------------------------------------"+
        s"\n                          Stage name: ${stageCompleted.stageInfo.name} " +
        s"\n                          Tasks count: ${stageCompleted.stageInfo.numTasks} " +
        s"\n                          ExecutorRunTime=${stageCompleted.stageInfo.taskMetrics.executorRunTime} " +
        s"\n                          ExecutorCPUTime=${stageCompleted.stageInfo.taskMetrics.executorCpuTime} " +
        s"\n-------------------------------------------------------------------------------------------------------------\n\n\n")
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    println (
      s"-------------------------------------------------------------------------------------------------------------"+
        s"\n                          Task - ${taskStart.taskInfo.index}  of Stage - ${taskStart.stageId} Started" +
        s"\n-------------------------------------------------------------------------------------------------------------"+
        s"\n                          Id: ${taskStart.taskInfo.taskId} " +
        s"\n                          Executor Id: ${taskStart.taskInfo.executorId} " +
        s"\n                          Host: ${taskStart.taskInfo.host} " +
        s"\n-------------------------------------------------------------------------------------------------------------\n\n\n")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =  {
    println (
      s"-------------------------------------------------------------------------------------------------------------"+
        s"\n                          Task - ${taskEnd.taskInfo.index}  of Stage - ${taskEnd.stageId} Completed" +
        s"\n-------------------------------------------------------------------------------------------------------------"+
        s"\n                          Id: ${taskEnd.taskInfo.taskId} " +
        s"\n                          TaskType: ${taskEnd.taskType} " +
        s"\n                          Executor Id: ${taskEnd.taskInfo.executorId} " +
        s"\n                          Host: ${taskEnd.taskInfo.host} " +
        s"\n                          Reason: ${taskEnd.reason} " +
        s"\n                          Records Written=${taskEnd.taskMetrics.outputMetrics.recordsWritten} " +
        s"\n                          Records Read=${taskEnd.taskMetrics.inputMetrics.recordsRead} " +
        s"\n                          Executor RunTime=${taskEnd.taskMetrics.executorRunTime} " +
        s"\n                          Executor Cpu Time=${taskEnd.taskMetrics.executorCpuTime} " +
        s"\n                          PeakExecutionMemory: ${taskEnd.taskMetrics.peakExecutionMemory} " +
        s"\n-------------------------------------------------------------------------------------------------------------\n\n\n")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println(s"-------------------------------------------------------------------------------------------------------------"+
      s"\n                          Application Ends" +
      s"\n-------------------------------------------------------------------------------------------------------------"+
      s"\n                          App ID: ${appID} " +
      s"\n                          App name: ${appName} " +
      s"\n                          HTTP end point name: ${HTTP_endpoint} " +
      s"\n-------------------------------------------------------------------------------------------------------------\n\n\n")

    val result = Http("https://fake-server-app.herokuapp.com/users").postForm(Seq("application_ID" -> appID, "application_Name" -> appName)).asString

  }
}
