package org.apache.spark.shuffle.redis

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle


/**
  * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
  * Redis based shuffle.
  */
private[spark] class RedisShuffleHandle[K, V, C](
  shuffleId: Int,
  dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle(shuffleId, dependency) {
}
