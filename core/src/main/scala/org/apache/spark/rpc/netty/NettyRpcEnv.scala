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
package org.apache.spark.rpc.netty

import java.io._
import java.lang.ThreadLocal
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.nio.channels.{Pipe, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client._
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.sasl.{SaslClientBootstrap, SaslServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance, SerializerInstance, KryoSerializer, KryoSerializerInstance}
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.log4j.LogManager
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.StatusUpdate

private[netty] class NettyRpcEnv(
    val conf: SparkConf,
    javaSerializerInstance: JavaSerializerInstance,
    taskSentSer: ThreadLocal[SerializerInstance],
    host: String,
    securityManager: SecurityManager,
    val useKryo: Boolean) extends RpcEnv(conf) with Logging {

  private[netty] val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set("spark.rpc.io.numConnectionsPerPeer", "1"),
    "rpc",
    conf.getInt("spark.rpc.io.threads", 0))

  private[netty] val launchTaskSign:Byte = 1
  private[netty] val nonLaunchTaskSign:Byte = 0

  private val dispatcher: Dispatcher = new Dispatcher(this)

  private val streamManager = new NettyStreamManager(this)

  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    if (securityManager.isAuthenticationEnabled()) {
      java.util.Arrays.asList(new SaslClientBootstrap(transportConf, "", securityManager,
        securityManager.isSaslEncryptionEnabled()))
    } else {
      java.util.Collections.emptyList[TransportClientBootstrap]
    }
  }

  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  /**
   * A separate client factory for file downloads. This avoids using the same RPC handler as
   * the main RPC context, so that events caused by these clients are kept isolated from the
   * main RPC traffic.
   *
   * It also allows for different configuration of certain properties, such as the number of
   * connections per peer.
   */
  @volatile private var fileDownloadFactory: TransportClientFactory = _

  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
  // to implement non-blocking send/ask.
  // TODO: a non-blocking TransportClientFactory.createClient in future
  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads", 64))

  @volatile private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  /**
   * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
   * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
   */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
   * Remove the address's Outbox and stop it.
   */
  private[netty] def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  def startServer(bindAddress: String, port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] =
      if (securityManager.isAuthenticationEnabled()) {
        java.util.Arrays.asList(new SaslServerBootstrap(transportConf, securityManager))
      } else {
        java.util.Collections.emptyList()
      }
    server = transportContext.createServer(bindAddress, port, bootstraps)
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name), this.getClass().getName()).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        targetOutbox.send(message)
      }
    }
  }

  private[netty] def send(message: RequestMessage, senderType: String): Unit = {
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      //network_log.info("Local message {} " + message)
      // Message to a local RPC endpoint.
      try {
        // Set the size to zero. As it's local message
        dispatcher.postOneWayMessage(message, 0)
      } catch {
        case e: RpcEnvStoppedException => logWarning(e.getMessage)
      }
    } else {
      // Message to a remote RPC endpoint.
      var bf: ByteBuffer = null
      val network_log = org.apache.log4j.LogManager.getLogger("networkLogger")
      if (senderType.endsWith("category:LaunchTask")) {
        network_log.info(s"TempLog: TaskSent TestDesrializeLaunchTask")
        if (useKryo) {
          bf = taskSentSerialize(message)
          assert(bf.limit < bf.capacity)
          bf.limit(bf.limit + 1)
          bf.put(bf.limit - 1, launchTaskSign)
        } else {
          bf = serialize(message)
        }
        network_log.info("Task sent byte: " + bf.limit)
        logTrace("Task sent byte: " + bf.limit)

        if (conf.getBoolean("spark.TaskSentBreakDownLimited", false)) {
          network_log.info(s"TempLog: TaskSent messageSize ${bf.limit}")
        }

        if (conf.getBoolean("spark.TaskSentBreakDown", false)) {
          val recSize = serialize(message.receiver).limit
          val recNameSize = serialize(message.receiver.name).limit
          val addressSize = serialize(message.senderAddress).limit
          network_log.info(s"TempLog: TaskSent messageSize ${bf.limit}")
          network_log.info(s"TempLog: TaskSent receiverSize $recSize")
          network_log.info(s"TempLog: TaskSent receiverNameSize $recNameSize")
          network_log.info(s"TempLog: TaskSent senderAddressSize $addressSize")
        }
      } else {
        bf = serialize(message)
        if (useKryo) {
          assert(bf.limit < bf.capacity)
          bf.limit(bf.limit + 1)
          bf.put(bf.limit - 1, nonLaunchTaskSign)
        }
      }
      network_log.info(senderType + " sent breakdown size " + bf.limit)
      postToOutbox(message.receiver, OneWayOutboxMessage(bf))
      //postToOutbox(message.receiver, OneWayOutboxMessage(serialize(message)))
    }
  }

  private[netty] def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout, senderType: String): Future[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        logWarning(s"Ignored failure: $e")
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(s"Ignored message: $reply")
        }
    }

    try {
      if (remoteAddr == address) {
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage(message, p)
      } else {

        var bf: ByteBuffer = null
        val network_log = org.apache.log4j.LogManager.getLogger("networkLogger")
        if (senderType.endsWith("category:LaunchTask")) {
          if (useKryo) {
            bf = taskSentSerialize(message)
            assert(bf.limit < bf.capacity)
            bf.limit(bf.limit + 1)
            bf.put(bf.limit - 1, launchTaskSign)
          } else {
            bf = serialize(message)
          }

          if (conf.getBoolean("spark.TaskSentBreakDownLimited", false)) {
            network_log.info(s"TempLog: TaskSent messageSize ${bf.limit}")
          }

          if (conf.getBoolean("spark.TaskSentBreakDown", false)) {
            val recSize = serialize(message.receiver).limit
            val recNameSize = serialize(message.receiver.name).limit
            val addressSize = serialize(message.senderAddress).limit
            network_log.info(s"TempLog: TaskSent messageSize ${bf.limit}")
            network_log.info(s"TempLog: TaskSent receiverSize $recSize")
            network_log.info(s"TempLog: TaskSent receiverNameSize $recNameSize")
            network_log.info(s"TempLog: TaskSent senderAddressSize $addressSize")
          }
        } else {
          bf = serialize(message)
          if (useKryo) {
            assert(bf.limit < bf.capacity)
            bf.limit(bf.limit + 1)
            bf.put(bf.limit - 1, nonLaunchTaskSign)
          }
        }
        network_log.info(senderType + " sent breakdown size "  + bf.limit)
        val rpcMessage = RpcOutboxMessage(bf,
        //val rpcMessage = RpcOutboxMessage(serialize(message),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        postToOutbox(message.receiver, rpcMessage)
        promise.future.onFailure {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }

      val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
        override def run(): Unit = {
          onFailure(new TimeoutException(s"Cannot receive any reply in ${timeout.duration}"))
        }
      }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
      promise.future.onComplete { v =>
        timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  private[netty] def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }
  private[netty] def taskSentSerialize(content: Any): ByteBuffer = {
    if (taskSentSer.get() == null) {
      var ser:SerializerInstance = null
      try {
        conf.get("spark.taskSendSerializer")
        ser = new KryoSerializer(conf).newInstance().asInstanceOf[KryoSerializerInstance]
      } catch {
        case e: Exception =>
          ser = new JavaSerializer(conf).newInstance().asInstanceOf[JavaSerializerInstance]
      }
      taskSentSer.set(ser)
    }
    taskSentSer.get().serialize(content)
  }

  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  private[netty] def taskSentDeserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        if (taskSentSer.get() == null) {
          var ser:SerializerInstance = null
          try {
            conf.get("spark.taskSendSerializer")
            ser = new KryoSerializer(conf).newInstance().asInstanceOf[KryoSerializerInstance]
          } catch {
            case e: Exception =>
              ser = new JavaSerializer(conf).newInstance().asInstanceOf[JavaSerializerInstance]
          }
          taskSentSer.set(ser)
        }
        taskSentSer.get().deserialize[T](bytes)
      }
    }
  }


  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
    if (fileDownloadFactory != null) {
      fileDownloadFactory.close()
    }
  }

  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

  override def fileServer: RpcEnvFileServer = streamManager

  override def openChannel(uri: String): ReadableByteChannel = {
    val parsedUri = new URI(uri)
    require(parsedUri.getHost() != null, "Host name must be defined.")
    require(parsedUri.getPort() > 0, "Port must be defined.")
    require(parsedUri.getPath() != null && parsedUri.getPath().nonEmpty, "Path must be defined.")

    val pipe = Pipe.open()
    val source = new FileDownloadChannel(pipe.source())
    try {
      val client = downloadClient(parsedUri.getHost(), parsedUri.getPort())
      val callback = new FileDownloadCallback(pipe.sink(), source, client)
      client.stream(parsedUri.getPath(), callback)
    } catch {
      case e: Exception =>
        pipe.sink().close()
        source.close()
        throw e
    }

    source
  }

  private def downloadClient(host: String, port: Int): TransportClient = {
    if (fileDownloadFactory == null) synchronized {
      if (fileDownloadFactory == null) {
        val module = "files"
        val prefix = "spark.rpc.io."
        val clone = conf.clone()

        // Copy any RPC configuration that is not overridden in the spark.files namespace.
        conf.getAll.foreach { case (key, value) =>
          if (key.startsWith(prefix)) {
            val opt = key.substring(prefix.length())
            clone.setIfMissing(s"spark.$module.io.$opt", value)
          }
        }

        val ioThreads = clone.getInt("spark.files.io.threads", 1)
        val downloadConf = SparkTransportConf.fromSparkConf(clone, module, ioThreads)
        val downloadContext = new TransportContext(downloadConf, new NoOpRpcHandler(), true)
        fileDownloadFactory = downloadContext.createClientFactory(createClientBootstraps())
      }
    }
    fileDownloadFactory.createClient(host, port)
  }

  private class FileDownloadChannel(source: ReadableByteChannel) extends ReadableByteChannel {

    @volatile private var error: Throwable = _

    def setError(e: Throwable): Unit = {
      error = e
      source.close()
    }

    override def read(dst: ByteBuffer): Int = {
      Try(source.read(dst)) match {
        case Success(bytesRead) => bytesRead
        case Failure(readErr) =>
          if (error != null) {
            throw error
          } else {
            throw readErr
          }
      }
    }

    override def close(): Unit = source.close()

    override def isOpen(): Boolean = source.isOpen()

  }

  private class FileDownloadCallback(
      sink: WritableByteChannel,
      source: FileDownloadChannel,
      client: TransportClient) extends StreamCallback {

    override def onData(streamId: String, buf: ByteBuffer): Unit = {
      while (buf.remaining() > 0) {
        sink.write(buf)
      }
    }

    override def onComplete(streamId: String): Unit = {
      sink.close()
    }

    override def onFailure(streamId: String, cause: Throwable): Unit = {
      logDebug(s"Error downloading stream $streamId.", cause)
      source.setError(cause)
      sink.close()
    }

  }
}

private[netty] object NettyRpcEnv extends Logging {
  /**
   * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
   * Use `currentEnv` to wrap the deserialization codes. E.g.,
   *
   * {{{
   *   NettyRpcEnv.currentEnv.withValue(this) {
   *     your deserialization codes
   *   }
   * }}}
   */
  private[netty] val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
   * Similar to `currentEnv`, this variable references the client instance associated with an
   * RPC, in case it's needed to find out the remote address during deserialization.
   */
  private[netty] val currentClient = new DynamicVariable[TransportClient](null)

}

private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {

  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    var taskSentSer:SerializerInstance = null

    var useKryo:Boolean = true

    try {
      sparkConf.get("spark.taskSendSerializer")
      taskSentSer = new KryoSerializer(sparkConf).newInstance().asInstanceOf[KryoSerializerInstance]
    } catch {
      case e: Exception =>
        useKryo = false
        taskSentSer = new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    }

    val threadlocalTaskSentSer = new ThreadLocal[SerializerInstance]()
    threadlocalTaskSentSer.set(taskSentSer)

    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, threadlocalTaskSentSer, config.advertiseAddress,
        config.securityManager, useKryo)
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}

/**
 * The NettyRpcEnv version of RpcEndpointRef.
 *
 * This class behaves differently depending on where it's created. On the node that "owns" the
 * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
 *
 * On other machines that receive a serialized version of the reference, the behavior changes. The
 * instance will keep track of the TransportClient that sent the reference, so that messages
 * to the endpoint are sent over the client connection, instead of needing a new connection to
 * be opened.
 *
 * The RpcAddress of this ref can be null; what that means is that the ref can only be used through
 * a client connection, since the process hosting the endpoint is not listening for incoming
 * connections. These refs should not be shared with 3rd parties, since they will not be able to
 * send messages to the endpoint.
 *
 * @param conf Spark configuration.
 * @param endpointAddress The address where the endpoint is listening.
 * @param nettyEnv The RpcEnv associated with this ref.
 */
private[netty] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv)
  extends RpcEndpointRef(conf) with Serializable with Logging {

  @transient @volatile var client: TransportClient = _

  private val _address = if (endpointAddress.rpcAddress != null) endpointAddress else null
  private val _name = endpointAddress.name

  override def address: RpcAddress = if (_address != null) _address.rpcAddress else null

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def name: String = _name

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout, senderType: String): Future[T] = {
    nettyEnv.ask(RequestMessage(nettyEnv.address, this, message), timeout, senderType)
  }

  override def send(message: Any, senderType: String): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(RequestMessage(nettyEnv.address, this, message), senderType)
  }

  override def toString: String = s"NettyRpcEndpointRef(${_address})"

  def toURI: URI = new URI(_address.toString)

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => _address == other._address
    case _ => false
  }

  final override def hashCode(): Int = if (_address == null) 0 else _address.hashCode()
}

/**
 * The message that is sent from the sender to the receiver.
 */
case class RequestMessage(
    senderAddress: RpcAddress, receiver: NettyRpcEndpointRef, content: Any)

/**
 * A response that indicates some failure happens in the receiver side.
 */
private[netty] case class RpcFailure(e: Throwable)

/**
 * Dispatches incoming RPCs to registered endpoints.
 *
 * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
 * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
 * one that is not listening for incoming connections, but rather needs to be contacted via the
 * client socket).
 *
 * Events are sent on a per-connection basis, so if a client opens multiple connections to the
 * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
 * with different `RpcAddress` information).
 */
private[netty] class NettyRpcHandler(
    dispatcher: Dispatcher,
    nettyEnv: NettyRpcEnv,
    streamManager: StreamManager) extends RpcHandler with Logging {

  // A variable to track the remote RpcEnv addresses of all clients
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    val messageToDispatch = internalReceive(client, message)
    // Position and Limit are the same here
    // val network_log = org.apache.log4j.LogManager.getLogger("networkLogger")
    dispatcher.postRemoteMessage(messageToDispatch, callback, message.limit)
  }

  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    // Position and Limit are the same here
    // val network_log = org.apache.log4j.LogManager.getLogger("networkLogger")
    val name_size = nettyEnv.serialize(messageToDispatch.receiver.name).limit
    // network_log.trace(s"""TempLog: NettyRpcHandler message size ${message.limit},
    //  content ${nettyEnv.serialize(messageToDispatch.content).limit},
    //  address size ${nettyEnv.serialize(messageToDispatch.senderAddress).limit},
    //  receiver name size ${name_size},
    //  receiver size ${nettyEnv.serialize(messageToDispatch.receiver).limit}""")
    if (messageToDispatch.content.isInstanceOf[StatusUpdate] &&
          nettyEnv.conf.getBoolean("spark.SUBreakDown", false)) {
      val stateUpdate = messageToDispatch.content.asInstanceOf[StatusUpdate]
      val network_log = org.apache.log4j.LogManager.getLogger("networkLogger")
      network_log.info(
s"""TempLog: SU totalMessage ${message.limit}
TempLog: SU address ${nettyEnv.serialize(messageToDispatch.senderAddress).limit}
TempLog: SU receiver ${nettyEnv.serialize(messageToDispatch.receiver).limit}
TempLog: SU receiverName ${name_size}
TempLog: SU content ${nettyEnv.serialize(messageToDispatch.content).limit}
TempLog: SU exeId ${nettyEnv.serialize(stateUpdate.executorId).limit}
TempLog: SU taskId ${nettyEnv.serialize(stateUpdate.taskId).limit}
TempLog: SU state ${nettyEnv.serialize(stateUpdate.state).limit}
TempLog: SU data ${nettyEnv.serialize(stateUpdate.data).limit}""")
    }
    dispatcher.postOneWayMessage(messageToDispatch, message.limit)
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)

    var requestMessage: RequestMessage = null
    // Fetch the message type
    if (nettyEnv.useKryo) {
      val messageType = message.get(message.limit - 1)
      message.limit(message.limit - 1)
      if (messageType == nettyEnv.launchTaskSign) {
        requestMessage = nettyEnv.taskSentDeserialize[RequestMessage](client, message)
      } else {
        requestMessage = nettyEnv.deserialize[RequestMessage](client, message)
      }
    } else {
      requestMessage = nettyEnv.deserialize[RequestMessage](client, message)
    }

    if (requestMessage.senderAddress == null) {
      // Create a new message with the socket address of the client as the sender.
      RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
      // the listening address
      val remoteEnvAddress = requestMessage.senderAddress
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
  }

  override def getStreamManager: StreamManager = streamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      // If the remove RpcEnv listens to some address, we should also fire a
      // RemoteProcessConnectionError for the remote RpcEnv listening address
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      logError("Exception before connecting to the client", cause)
    }
  }

  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      nettyEnv.removeOutbox(clientAddr)
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      // If the remove RpcEnv listens to some address, we should also  fire a
      // RemoteProcessDisconnected for the remote RpcEnv listening address
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}
