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

package org.apache.spark.streaming.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{State, StateImpl, Time}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.util.{EmptyStateMap, StateMap}
import org.apache.spark.util.Utils

/**
 * Record storing the keyed-state [[MapWithStateRDD]]. Each record contains a [[StateMap]] and a
 * sequence of records returned by the mapping function of `mapWithState`.
 */
private[streaming] case class MapWithStateRDDRecord[K, S, E](
    var stateMap: StateMap[K, S], var mappedData: Seq[E])

private[streaming] object MapWithStateRDDRecord extends Logging {
  def updateRecordWithData[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
    prevRecord: Option[MapWithStateRDDRecord[K, S, E]],
    dataIterator: Iterator[(K, V)],
    mappingFunction: (Time, K, Option[V], State[S]) => Option[E],
    batchTime: Time,
    timeoutThresholdTime: Option[Long],
    removeTimedoutData: Boolean
  ): MapWithStateRDDRecord[K, S, E] = {

    val t0 =  System.nanoTime
    // Create a new state map by cloning the previous one (if it exists) or by creating an empty one
    val newStateMap = prevRecord.map { _.stateMap.copy() }. getOrElse { new EmptyStateMap[K, S]() }

    val mappedData = new ArrayBuffer[E]
    val wrappedState = new StateImpl[S]()

    // Call the mapping function on each record in the data iterator, and accordingly
    // update the states touched, and collect the data returned by the mapping function
    dataIterator.foreach { case (key, value) =>
      wrappedState.wrap(newStateMap.get(key))
      val returned = mappingFunction(batchTime, key, Some(value), wrappedState)
      if (wrappedState.isRemoved) {
        newStateMap.remove(key)
      } else if (wrappedState.isUpdated
          || (wrappedState.exists && timeoutThresholdTime.isDefined)) {
        newStateMap.put(key, wrappedState.get(), batchTime.milliseconds)
      }
      mappedData ++= returned
    }
    logInfo("generate new map data takes %f ms".format((System.nanoTime - t0) / 1e6))


    // Get the timed out state records, call the mapping function on each and collect the
    // data returned
    if (removeTimedoutData && timeoutThresholdTime.isDefined) {
      newStateMap.getByTime(timeoutThresholdTime.get).foreach { case (key, state, _) =>
        wrappedState.wrapTimingOutState(state)
        val returned = mappingFunction(batchTime, key, None, wrappedState)
        mappedData ++= returned
        newStateMap.remove(key)
      }
    }

    MapWithStateRDDRecord(newStateMap, mappedData)
  }
}

/**
 * Partition of the [[MapWithStateRDD]], which depends on corresponding partitions of prev state
 * RDD, and a partitioned keyed-data RDD
 */
private[streaming] class MapWithStateRDDPartition(
    override val index: Int,
    @transient private var prevStateRDD: RDD[_],
    @transient private var partitionedDataRDD: RDD[_],
    @transient val childNoDep: Boolean = false) extends Partition {

  private[rdd] var previousSessionRDDPartition: Partition = null
  private[rdd] var partitionedDataRDDPartition: Partition = null

  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = other match {
    case that: MapWithStateRDDPartition => index == that.index
    case _ => false
  }

  override def noDepCopy(): Partition = {
    val part =  new MapWithStateRDDPartition(index, null, null)
    part.isNoDependency = true
    part
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    if (prevStateRDD != null) {
      if (childNoDep)
        previousSessionRDDPartition = prevStateRDD.truncatedPartitions()(index)
      else
        previousSessionRDDPartition = prevStateRDD.partitions(index)
    }
    if (partitionedDataRDD != null) {
      if (childNoDep)
        partitionedDataRDDPartition = partitionedDataRDD.truncatedPartitions()(index)
      else
        partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
    }
    oos.defaultWriteObject()
  }
}


/**
 * RDD storing the keyed states of `mapWithState` operation and corresponding mapped data.
 * Each partition of this RDD has a single record of type [[MapWithStateRDDRecord]]. This contains a
 * [[StateMap]] (containing the keyed-states) and the sequence of records returned by the mapping
 * function of  `mapWithState`.
 * @param prevStateRDD The previous MapWithStateRDD on whose StateMap data `this` RDD
  *                    will be created
 * @param partitionedDataRDD The partitioned data RDD which is used update the previous StateMaps
 *                           in the `prevStateRDD` to create `this` RDD
 * @param mappingFunction  The function that will be used to update state and return new data
 * @param batchTime        The time of the batch to which this RDD belongs to. Use to update
 * @param timeoutThresholdTime The time to indicate which keys are timeout
 */
private[streaming] class MapWithStateRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
    private var prevStateRDD: RDD[MapWithStateRDDRecord[K, S, E]],
    private var partitionedDataRDD: RDD[(K, V)],
    mappingFunction: (Time, K, Option[V], State[S]) => Option[E],
    batchTime: Time,
    timeoutThresholdTime: Option[Long]
  ) extends RDD[MapWithStateRDDRecord[K, S, E]](
    partitionedDataRDD.sparkContext,
    List(
      new OneToOneDependency[MapWithStateRDDRecord[K, S, E]](prevStateRDD),
      new OneToOneDependency(partitionedDataRDD))
  ) {


  val manuallyDoCompact: Boolean = partitionedDataRDD.sparkContext.getConf.getBoolean("spark.cacheopt.ManuallyDoCompact", false)
  val useIncCompact: Boolean = partitionedDataRDD.sparkContext.getConf.getBoolean("spark.cacheopt.UseIncCompact", false)
  val checkpointInterval: Int = partitionedDataRDD.sparkContext.getConf.getInt("spark.cacheopt.CheckpointInterval", 10)
  @volatile private var doFullScan = false
  @transient var org_prevStateRDD :RDD[MapWithStateRDDRecord[K, S, E]] = null
  @transient var org_partitionedDataRDD : RDD[(K, V)] = null

  require(prevStateRDD.partitioner.nonEmpty)
  require(partitionedDataRDD.partitioner == prevStateRDD.partitioner)

  override val partitioner = prevStateRDD.partitioner
  /**
   * Do not send real dependencies to save network traffic
   */
  override def remove_dependencies {
    assert((org_dependencies_ == null) && (dependencies_ != null))
    org_dependencies_ = dependencies_
    dependencies_ = null
    org_partitionedDataRDD = partitionedDataRDD
    org_prevStateRDD = prevStateRDD
    partitionedDataRDD = null
    prevStateRDD = null
  }

  /**
   * Restore prev dependencies
   */
  override def restore_dependencies {
    assert((org_dependencies_ != null) && (dependencies_ == null))
    dependencies_ = org_dependencies_
    org_dependencies_ = null
    partitionedDataRDD = org_partitionedDataRDD
    prevStateRDD = org_prevStateRDD
    org_partitionedDataRDD = null
    org_prevStateRDD = null
  }

  override def checkpoint(): Unit = {
    super.checkpoint()
    doFullScan = true
  }

  override def compute(
      partition: Partition, context: TaskContext): Iterator[MapWithStateRDDRecord[K, S, E]] = {

    logInfo("compute mapwithstate rdd " + id)

    val stateRDDPartition = partition.asInstanceOf[MapWithStateRDDPartition]
    assert(!stateRDDPartition.isNoDependency);
    assert(stateRDDPartition.previousSessionRDDPartition != null);
    assert(stateRDDPartition.partitionedDataRDDPartition != null);
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)
    val t0 =  System.nanoTime

    val prevRecord = if (prevStateRDDIterator.hasNext) Some(prevStateRDDIterator.next()) else None
    logInfo("getting prev record takes %f s".format((System.nanoTime - t0) / 1e9))

    logInfo("timeoutThresholdTime.isDefined %b".format(timeoutThresholdTime.isDefined))

    val newRecord = MapWithStateRDDRecord.updateRecordWithData(
      prevRecord,
      dataIterator,
      mappingFunction,
      batchTime,
      timeoutThresholdTime,
      //removeTimedoutData = doFullScan // remove timedout data only when full scan is enabled
      removeTimedoutData = true
    )
    logInfo("getting new record& prev record takes %f s".format((System.nanoTime - t0) / 1e9))

    if (useCacheOpt && manuallyDoCompact && batchTime.milliseconds % (100*checkpointInterval) == 0) {
      if (useIncCompact) {
        newRecord.stateMap.manuallyIncrementalCompact
      } else {
        newRecord.stateMap.manuallyCompact
      }
    }
    Iterator(newRecord)
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(prevStateRDD.partitions.length) { i =>
      new MapWithStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
  }

  override def getTruncatedPartitions(availableRDDs: List[RDD[_]]): Array[Partition] = {
    if (availableRDDs.contains(this)) {
      noDepPartitions
    } else {
      prevStateRDD.truncatedPartitions(availableRDDs)
      partitionedDataRDD.truncatedPartitions(availableRDDs)
      Array.tabulate(prevStateRDD.partitions.length) { i =>
        new MapWithStateRDDPartition(i, prevStateRDD, partitionedDataRDD, true)}
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prevStateRDD = null
    partitionedDataRDD = null
  }

  def setFullScan(): Unit = {
    doFullScan = true
  }
}

private[streaming] object MapWithStateRDD {

  def createFromPairRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
      pairRDD: RDD[(K, S)],
      partitioner: Partitioner,
      updateTime: Time): MapWithStateRDD[K, V, S, E] = {

    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions ({ iterator =>
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { case (key, state) => stateMap.put(key, state, updateTime.milliseconds) }
      Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[E]))
    }, preservesPartitioning = true)

    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)

    val noOpFunc = (time: Time, key: K, value: Option[V], state: State[S]) => None

    new MapWithStateRDD[K, V, S, E](
      stateRDD, emptyDataRDD, noOpFunc, updateTime, None)
  }

  def createFromRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
      rdd: RDD[(K, S, Long)],
      partitioner: Partitioner,
      updateTime: Time): MapWithStateRDD[K, V, S, E] = {

    val pairRDD = rdd.map { x => (x._1, (x._2, x._3)) }
    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({ iterator =>
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { case (key, (state, updateTime)) =>
        stateMap.put(key, state, updateTime)
      }
      Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[E]))
    }, preservesPartitioning = true)

    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)

    val noOpFunc = (time: Time, key: K, value: Option[V], state: State[S]) => None

    new MapWithStateRDD[K, V, S, E](
      stateRDD, emptyDataRDD, noOpFunc, updateTime, None)
  }
}
