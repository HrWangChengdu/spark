/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.io._
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param metrics a `TaskMetrics` that is created at driver side and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
  */
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    var taskBinary: Broadcast[Array[Byte]],
    @transient var taskBinary_truncated: Broadcast[Array[Byte]],
    var partition: Partition,
    val locs: Seq[TaskLocation],
    val outputId: Int,
    localProperties: Properties,
    metrics: TaskMetrics,
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[U](stageId, stageAttemptId, partition.index, metrics, localProperties, jobId,
    appId, appAttemptId)
  with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def useTruncatedPartition(truncatedPartition: Partition) {
    assert(fullPartition == null)
    assert(truncatedPartition.index == partition.index)
    fullPartition = partition 
    partition = truncatedPartition
  }

  override def restoreToFullPartition() {
    assert(fullPartition != null)
    partition = fullPartition
    fullPartition = null
  }

  override def switchTruncatedTaskBinary() {
    val buffer = taskBinary
    taskBinary = taskBinary_truncated
    taskBinary_truncated = buffer
  }

  override def setFullTaskBinary(t: Broadcast[Array[Byte]]) {
    taskBinary = t
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    val extra_log = org.apache.log4j.LogManager.getLogger("extraLogger_" + SparkEnv.get.executorId)
    val basicLogComponent = "TaskAttempt ID:" + context.taskAttemptId().toString() +
      ",Partition Id:" + context.partitionId().toString +
      ",Stage Id:" + context.stageId().toString

    extra_log.info("[ResultTask]StartAt:" + System.currentTimeMillis().toString + ","
      + basicLogComponent)
    val funcRet = func(context, rdd.iterator(partition, context))
    extra_log.info("[ResultTask]EndAt:" + System.currentTimeMillis().toString + ","
      + basicLogComponent )

    funcRet
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}
