package com.wavesplatform.network

import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

import com.google.common.base.Charsets
import com.google.common.collect.EvictingQueue
import com.google.common.primitives.{Bytes, Longs}
import com.twitter.chill.{KryoInstantiator, KryoPool}
import com.wavesplatform.settings.NetworkSettings
import com.wavesplatform.utils.createStore
import scorex.utils.ScorexLogging

import scala.util.Random

class PeerDatabaseImpl(settings: NetworkSettings) extends PeerDatabase with AutoCloseable with ScorexLogging {

  private val peersSuffix: Array[Byte] = ":peers".getBytes()
  private val blacklistedSuffix: Array[Byte] = ":blacklisted".getBytes()
  private val suspendedSuffix: Array[Byte] = ":suspended".getBytes()

  import PeerDatabaseImpl._

  private val table = createStore(Paths.get(settings.path.toString, "peers").toFile)

  private val unverifiedPeers = EvictingQueue.create[InetSocketAddress](settings.maxUnverifiedPeers)

  for (a <- settings.knownPeers.view.map(inetSocketAddress(_, 6863))) {
    // add peers from config with max timestamp so they never get evicted from the list of known peers
    doTouch(a, Long.MaxValue)
  }

  if (!settings.enableBlacklisting) {
    clearBlacklist()
  }

  override def addCandidate(socketAddress: InetSocketAddress): Unit = unverifiedPeers.synchronized {
    if (Option(table.get(makeKey(socketAddress, peersSuffix))).isEmpty && !unverifiedPeers.contains(socketAddress)) {
      log.trace(s"Adding candidate $socketAddress")
      unverifiedPeers.add(socketAddress)
    } else {
      log.trace(s"NOT adding candidate $socketAddress")
    }
  }

  private def doTouch(socketAddress: InetSocketAddress, timestamp: Long): Unit = unverifiedPeers.synchronized {
    unverifiedPeers.removeIf(_ == socketAddress)
    val it = table.iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey.endsWith(peersSuffix)) {
        val prevTs = Longs.fromByteArray(entry.getValue)
        val ts = Math.max(prevTs, timestamp)
        table.put(entry.getKey, Longs.toByteArray(ts))
      }
    }
  }

  override def touch(socketAddress: InetSocketAddress): Unit = doTouch(socketAddress, System.currentTimeMillis())

  override def blacklist(address: InetAddress, reason: String): Unit = {
    if (settings.enableBlacklisting) {
      unverifiedPeers.synchronized {
        unverifiedPeers.removeIf(_.getAddress == address)
        table.put(makeKey(address, blacklistedSuffix), encodeBlacklistValue(System.currentTimeMillis(), reason))
      }
    }
  }

  override def suspend(address: InetAddress): Unit = {
    unverifiedPeers.synchronized {
      unverifiedPeers.removeIf(_.getAddress == address)
      table.put(makeKey(address, suspendedSuffix), Longs.toByteArray(System.currentTimeMillis()))
    }
  }

  override def knownPeers: Map[InetSocketAddress, Long] = {
    removeObsoleteRecords(peersSuffix, settings.peersDataResidenceTime.toMillis)

    var result = Map.empty[InetSocketAddress, Long]

    val it = table.iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey.endsWith(peersSuffix)) {
        val address = decodeInetSocketAddress(entry.getKey)
        if (blacklistedHosts.contains(address.getAddress)) {
          val ts = Longs.fromByteArray(entry.getValue.take(LongSize))
          result = result.updated(address, ts)
        }
      }
    }
    result
  }

  override def blacklistedHosts: Set[InetAddress] = {
    removeObsoleteRecords(blacklistedSuffix, settings.blackListResidenceTime.toMillis)
    collectHosts(blacklistedSuffix)
  }

  override def suspendedHosts: Set[InetAddress] = {
    removeObsoleteRecords(suspendedSuffix, settings.suspensionResidenceTime.toMillis)
    collectHosts(suspendedSuffix)
  }

  override def detailedBlacklist: Map[InetAddress, (Long, String)] = {
    removeObsoleteRecords(blacklistedSuffix, settings.blackListResidenceTime.toMillis)
    collectBlacklistRecords()
  }

  override def detailedSuspended: Map[InetAddress, Long] = {
    removeObsoleteRecords(suspendedSuffix, settings.suspensionResidenceTime.toMillis)
    collectSuspendRecords()
  }

  override def randomPeer(excluded: Set[InetSocketAddress]): Option[InetSocketAddress] = unverifiedPeers.synchronized {
    //    log.trace(s"Excluding: $excluded")
    def excludeAddress(isa: InetSocketAddress) = excluded(isa) || blacklistedHosts(isa.getAddress) || suspendedHosts(isa.getAddress)

    // excluded only contains local addresses, our declared address, and external declared addresses we already have
    // connection to, so it's safe to filter out all matching candidates
    unverifiedPeers.removeIf(isa => excluded(isa))
    //    log.trace(s"Evicting queue: $unverifiedPeers")
    val unverified = Option(unverifiedPeers.peek()).filterNot(excludeAddress)
    val verified = Random.shuffle(knownPeers.keySet.diff(excluded).toSeq).headOption.filterNot(excludeAddress)

    //    log.trace(s"Unverified: $unverified; Verified: $verified")
    (unverified, verified) match {
      case (Some(_), v@Some(_)) => if (Random.nextBoolean()) Some(unverifiedPeers.poll()) else v
      case (Some(_), None) => Some(unverifiedPeers.poll())
      case (None, v@Some(_)) => v
      case _ => None
    }
  }

  private def removeObsoleteRecords(suffix: Array[Byte], maxAge: Long): Unit = {
    val earliestValidTs = System.currentTimeMillis() - maxAge

    val it = table.iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey.endsWith(suffix)) {
        val ts = Longs.fromByteArray(entry.getValue.take(LongSize))
        if (ts < earliestValidTs) table.delete(entry.getKey)
      }
    }
  }

  private def collectHosts(suffix: Array[Byte]): Set[InetAddress] = {
    val it = table.iterator()
    var result = Set.empty[InetAddress]

    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey.endsWith(suffix)) {
        val address = decodeInetAddress(entry.getKey)
        result = result + address
      }
    }
    result
  }

  private def collectBlacklistRecords(): Map[InetAddress, (Long, String)] = {
    var result = Map.empty[InetAddress, (Long, String)]

    val it = table.iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey.endsWith(blacklistedSuffix)) {
        val address = decodeInetAddress(entry.getKey)
        val pair = decodeBlacklistValue(entry.getValue)
        result = result.updated(address, pair)
      }
    }
    result
  }

  private def collectSuspendRecords(): Map[InetAddress, Long] = {
    var result = Map.empty[InetAddress, Long]
    val it = table.iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey.endsWith(suspendedSuffix)) {
        val address = decodeInetAddress(entry.getKey)
        val ts = Longs.fromByteArray(entry.getValue)
        result = result.updated(address, ts)
      }
    }
    result
  }

  def clearBlacklist(): Unit = {
    val it = table.iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey.endsWith(blacklistedSuffix)) {
        table.delete(entry.getKey)
      }
    }
  }

  override def close(): Unit = {
    table.close()
  }
}

object PeerDatabaseImpl {
  private val LongSize = 8
  private val PoolSize = 10
  private val kryo = KryoPool.withByteArrayOutputStream(PoolSize, new KryoInstantiator())

  def makeKey(address: InetAddress, suffix: Array[Byte]): Array[Byte] = {
    Bytes.concat(encodeInetAddress(address), suffix)
  }

  def makeKey(socketAddress: InetSocketAddress, suffix: Array[Byte]): Array[Byte] = {
    Bytes.concat(encodeInetSocketAddress(socketAddress), suffix)
  }

  def encodeBlacklistValue(ts: Long, reason: String): Array[Byte] = {
    Bytes.concat(Longs.toByteArray(ts), reason.getBytes(Charsets.UTF_8))
  }

  def decodeBlacklistValue(bytes: Array[Byte]): (Long, String) = {
    val ts = Longs.fromByteArray(bytes.take(LongSize))
    val msg = new String(bytes.takeRight(bytes.length - LongSize), StandardCharsets.UTF_8)
    (ts, msg)
  }

  def encodeInetAddress(value: InetAddress): Array[Byte] = kryo.toBytesWithClass(value)

  def decodeInetAddress(arr: Array[Byte]): InetAddress = kryo.fromBytes(arr, classOf[InetAddress])

  def encodeInetSocketAddress(value: InetSocketAddress): Array[Byte] = kryo.toBytesWithClass(value)

  def decodeInetSocketAddress(arr: Array[Byte]): InetSocketAddress = kryo.fromBytes(arr, classOf[InetSocketAddress])
}
