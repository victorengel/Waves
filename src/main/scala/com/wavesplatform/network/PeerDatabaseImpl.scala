package com.wavesplatform.network

import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets

import com.google.common.base.Charsets
import com.google.common.collect.EvictingQueue
import com.google.common.primitives.{Bytes, Longs}
import com.twitter.chill.{KryoInstantiator, KryoPool}
import com.wavesplatform.db.SubStorage
import com.wavesplatform.settings.NetworkSettings
import org.iq80.leveldb.DB
import scorex.utils.ScorexLogging

import scala.util.Random

class PeerDatabaseImpl(db: DB, settings: NetworkSettings) extends SubStorage(db, "peers") with PeerDatabase with ScorexLogging {

  private val peersPrefix: Array[Byte] = "peers".getBytes()
  private val blacklistedPrefix: Array[Byte] = "blacklisted".getBytes()
  private val suspendedPrefix: Array[Byte] = "suspended".getBytes()

  import PeerDatabaseImpl._

  private val unverifiedPeers = EvictingQueue.create[InetSocketAddress](settings.maxUnverifiedPeers)

  for (a <- settings.knownPeers.view.map(inetSocketAddress(_, 6863))) {
    // add peers from config with max timestamp so they never get evicted from the list of known peers
    doTouch(a, Long.MaxValue)
  }

  if (!settings.enableBlacklisting) {
    clearBlacklist()
  }

  override def addCandidate(socketAddress: InetSocketAddress): Unit = unverifiedPeers.synchronized {
    if (get(makeKey(peersPrefix, encodeInetSocketAddress(socketAddress))).isEmpty && !unverifiedPeers.contains(socketAddress)) {
      log.trace(s"Adding candidate $socketAddress")
      unverifiedPeers.add(socketAddress)
    } else {
      log.trace(s"NOT adding candidate $socketAddress")
    }
  }

  private def doTouch(socketAddress: InetSocketAddress, timestamp: Long): Unit = unverifiedPeers.synchronized {
    unverifiedPeers.removeIf(_ == socketAddress)
    map(peersPrefix, stripPrefix = false).foreach { e =>
      val prevTs = Longs.fromByteArray(e._2)
      val ts = Math.max(prevTs, timestamp)
      put(e._1, Longs.toByteArray(ts))
    }
  }

  override def touch(socketAddress: InetSocketAddress): Unit = doTouch(socketAddress, System.currentTimeMillis())

  override def blacklist(address: InetAddress, reason: String): Unit = {
    if (settings.enableBlacklisting) {
      unverifiedPeers.synchronized {
        unverifiedPeers.removeIf(_.getAddress == address)
        put(makeKey(blacklistedPrefix, encodeInetAddress(address)), encodeBlacklistValue(System.currentTimeMillis(), reason))
      }
    }
  }

  override def suspend(address: InetAddress): Unit = {
    unverifiedPeers.synchronized {
      unverifiedPeers.removeIf(_.getAddress == address)
      put(makeKey(suspendedPrefix, encodeInetAddress(address)), Longs.toByteArray(System.currentTimeMillis()))
    }
  }

  override def knownPeers: Map[InetSocketAddress, Long] = {
    removeObsoleteRecords(peersPrefix, settings.peersDataResidenceTime.toMillis)
    map(peersPrefix).map(e => decodeInetSocketAddress(e._1) -> Longs.fromByteArray(e._2))
  }

  override def blacklistedHosts: Set[InetAddress] = {
    removeObsoleteRecords(blacklistedPrefix, settings.blackListResidenceTime.toMillis)
    map(blacklistedPrefix).map(e => decodeInetAddress(e._1)).toSet
  }

  override def suspendedHosts: Set[InetAddress] = {
    removeObsoleteRecords(suspendedPrefix, settings.suspensionResidenceTime.toMillis)
    map(suspendedPrefix).map(e => decodeInetAddress(e._1)).toSet
  }

  override def detailedBlacklist: Map[InetAddress, (Long, String)] = {
    removeObsoleteRecords(blacklistedPrefix, settings.blackListResidenceTime.toMillis)
    map(blacklistedPrefix).map(e => decodeInetAddress(e._1) -> decodeBlacklistValue(e._2))
  }

  override def detailedSuspended: Map[InetAddress, Long] = {
    removeObsoleteRecords(suspendedPrefix, settings.suspensionResidenceTime.toMillis)
    map(suspendedPrefix).map(e => decodeInetAddress(e._1) -> Longs.fromByteArray(e._2))
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

  private def removeObsoleteRecords(prefix: Array[Byte], maxAge: Long): Unit = {
    val earliestValidTs = System.currentTimeMillis() - maxAge
    map(prefix, stripPrefix = false).foreach { e =>
      val ts = Longs.fromByteArray(e._2.take(LongSize))
      if (ts < earliestValidTs) delete(e._1)
    }
  }

  def clearBlacklist(): Unit = map(blacklistedPrefix, stripPrefix = false).foreach(e => delete(e._1))

}

object PeerDatabaseImpl {
  private val LongSize = 8
  private val PoolSize = 10
  private val kryo = KryoPool.withByteArrayOutputStream(PoolSize, new KryoInstantiator())

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
