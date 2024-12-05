package org.apache.spark.shuffle.redis

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.serializer.SerializerInstance

import _root_.redis.clients.jedis.Jedis
import scala.reflect.ClassTag

/**
  * A shuffler manager that support using local Redis store instead of Spark's BlockStore.
  */
private[spark] class RedisShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {


  /**
    * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
    */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, ConcurrentHashMap[Long, Int]]()

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new RedisShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](handle: ShuffleHandle,
                               mapId: Long,
                               context: TaskContext,
                               metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

    val numPartition =
      handle.asInstanceOf[RedisShuffleHandle[K, V, _]].dependency.partitioner.numPartitions
    val newMap = new ConcurrentHashMap[Long, Int]()
    val map = numMapsForShuffle.putIfAbsent(handle.shuffleId, newMap)
    if (map == null) newMap.put(mapId, numPartition)
    else map.put(mapId, numPartition)
    new RedisShuffleWriter(handle, mapId, context)
  }

  override def getReader[K, C](handle: ShuffleHandle,
                               startMapIndex: Int,
                               endMapIndex: Int,
                               startPartition: Int,
                               endPartition: Int,
                               context: TaskContext,
                               metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    new RedisShuffleReader[K, C](
      handle.asInstanceOf[RedisShuffleHandle[K, _, C]], startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // delete map output in Redis
    val jedis = new Jedis
    Option(numMapsForShuffle.remove(shuffleId)).foreach { maps =>
      maps.asScala.foreach { map =>
        val mapId = map._1
        val numPartition = map._2
        RedisShuffleManager.mapKeyIter(shuffleId, mapId, numPartition).foreach { key =>
          jedis.del(key)
        }
      }
    }
    jedis.close()
    true
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    throw new UnsupportedOperationException(
      "The RedisShuffleManager dose not return BlockResolver.")
  }

  override def stop(): Unit = {
    // do nothing
  }
}

private[spark] object RedisShuffleManager extends Logging {

  def mapKeyPrefix(shuffleId: Int, mapId: Long) =
    s"SHUFFLE_${shuffleId}_${mapId}_"

  def mapKey(shuffleId: Int, mapId: Long, partition: Int) =
    s"SHUFFLE_${shuffleId}_${mapId}_${partition}".getBytes()

  def mapKeyIter(shuffleId: Int, mapId: Long, numPartitions: Int): Iterator[Array[Byte]] = {
    (0 until numPartitions).toIterator.map { partition =>
      mapKey(shuffleId, mapId, partition)
    }
  }

  def serializePair[K: ClassTag, V: ClassTag](serializer: SerializerInstance,
                                              key: K, value: V): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val sstream = serializer.serializeStream(baos)
    sstream.writeKey(key)
    sstream.writeValue(value)
    sstream.flush()
    sstream.close()
    baos.toByteArray
  }

  def deserializePair[K, V](serializer: SerializerInstance, data: Array[Byte]): (K, V) = {
    val bais = new ByteArrayInputStream(data)
    val sstream = serializer.deserializeStream(bais)
    val key: K = sstream.readKey()
    val value: V = sstream.readValue()
    sstream.close()
    (key, value)
  }
}
