package com.wavesplatform.network

import java.net.{InetAddress, InetSocketAddress}

import com.google.common.collect.EvictingQueue
import com.google.common.primitives.Longs
import com.wavesplatform.db._
import com.wavesplatform.settings.NetworkSettings
import com.wavesplatform.state2._
import io.netty.channel.Channel
import io.netty.channel.socket.nio.NioSocketChannel
import org.iq80.leveldb.DB
import scorex.utils.ScorexLogging

import scala.util.{Random, Try}

class PeerDatabaseImpl(db: DB, settings: NetworkSettings) extends SubStorage(db, "peers") with PeerDatabase with ScorexLogging {
  private val defaultTestNetPort = 6863

  private val peersPrefix: Array[Byte] = "peers".getBytes(Charset)
  private val peersIndexKey = makeKey("all-peers".getBytes(Charset), 0)
  private val blacklistedHostsPrefix: Array[Byte] = "blacklisted-hosts".getBytes(Charset)
  private val suspendedHostsPrefix: Array[Byte] = "suspended-hosts".getBytes(Charset)
  private val hostsIndexKey = makeKey("all-hosts".getBytes(Charset), 0)

  private val unverifiedPeers = EvictingQueue.create[InetSocketAddress](settings.maxUnverifiedPeers)

  for (a <- settings.knownPeers.view.map(inetSocketAddress(_, defaultTestNetPort))) {
    // add peers from config with max timestamp so they never get evicted from the list of known peers
    doTouch(a, Long.MaxValue)
  }

  if (!settings.enableBlacklisting) {
    clearBlacklist()
  }

  override def addCandidate(socketAddress: InetSocketAddress): Unit = unverifiedPeers.synchronized {
    if (get(makeKey(peersPrefix, InetSocketAddressCodec.encode(socketAddress))).isEmpty &&
      !unverifiedPeers.contains(socketAddress)) {
      log.trace(s"Adding candidate $socketAddress")
      unverifiedPeers.add(socketAddress)
    } else {
      log.trace(s"NOT adding candidate $socketAddress")
    }
  }

  override def touch(socketAddress: InetSocketAddress): Unit = doTouch(socketAddress, System.currentTimeMillis())

  override def blacklist(host: InetAddress, reason: String): Unit = {
    if (settings.enableBlacklisting) {
      unverifiedPeers.synchronized {
        unverifiedPeers.removeIf(_.getAddress == host)
        blacklistHost(host, System.currentTimeMillis(), reason)
      }
    }
  }

  override def suspend(host: InetAddress): Unit = {
    unverifiedPeers.synchronized {
      unverifiedPeers.removeIf(_.getAddress == host)
      suspendHost(host, System.currentTimeMillis())
    }
  }

  override def knownPeers: Map[InetSocketAddress, Long] = {
    removeObsoletePeers()
    allPeers().map { p =>
      val ts = getPeerTimestamp(p)
      p -> ts
    }.toMap.filterKeys(address => !blacklistedHosts.contains(address.getAddress))
  }

  override def blacklistedHosts: Set[InetAddress] = {
    removeObsoleteHosts()
    allHosts().filter(h => isBlacklisted(h)).toSet
  }

  override def suspendedHosts: Set[InetAddress] = {
    removeObsoleteHosts()
    allHosts().filter(h => isSuspended(h)).toSet
  }

  override def detailedBlacklist: Map[InetAddress, (Long, String)] = {
    removeObsoleteHosts()
    allHosts().foldLeft(Map.empty[InetAddress, (Long, String)]) { (r, h) =>
      val maybeDetails = getBlacklistingDetails(h)
      if (maybeDetails.isDefined) r.updated(h, maybeDetails.get) else r
    }
  }

  override def detailedSuspended: Map[InetAddress, Long] = {
    removeObsoleteHosts()
    allHosts().foldLeft(Map.empty[InetAddress, Long]) { (r, h) =>
      val maybeTime = getSuspensionTime(h)
      if (maybeTime.isDefined) r.updated(h, maybeTime.get) else r
    }
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

  override def clearBlacklist(): Unit =
    allHosts().filter(h => isBlacklisted(h)).foreach(h => removeBlacklistedHost(h))

  override def blacklistAndClose(channel: Channel, reason: String): Unit = {
    val address = channel.asInstanceOf[NioSocketChannel].remoteAddress().getAddress
    log.debug(s"Blacklisting ${id(channel)}: $reason")
    blacklist(address, reason)
    channel.close()
  }

  private def getPeerTimestamp(peer: InetSocketAddress): Long =
    get(makeKey(peersPrefix, InetSocketAddressCodec.encode(peer)))
      .flatMap(b => Try(Longs.fromByteArray(b)).toOption).getOrElse(0L)

  private def addOrUpdatePeer(peer: InetSocketAddress, ts: Long): Unit = {
    val updatedIndex = allPeers() :+ peer
    val batch = createBatch()
    put(makeKey(peersPrefix, InetSocketAddressCodec.encode(peer)), Longs.toByteArray(ts), batch)
    put(peersIndexKey, InetSocketAddressSeqCodec.encode(updatedIndex.distinct), batch)
    commit(batch)
  }

  private def removePeer(peer: InetSocketAddress): Unit = {
    val updatedIndex = allPeers().filterNot(p => p == peer).distinct
    val batch = createBatch()
    delete(makeKey(peersPrefix, InetSocketAddressCodec.encode(peer)), batch)
    put(peersIndexKey, InetSocketAddressSeqCodec.encode(updatedIndex))
    commit(batch)
  }

  private def allPeers(): Seq[InetSocketAddress] =
    get(peersIndexKey).map(InetSocketAddressSeqCodec.decode).map(_.explicitGet().value).getOrElse(Seq.empty)

  private def removeObsoletePeers(): Unit = {
    val valid = System.currentTimeMillis() - settings.peersDataResidenceTime.toMillis
    allPeers().foreach { p =>
      val ts = getPeerTimestamp(p)
      if (ts < valid) removePeer(p)
    }
  }

  private def doTouch(socketAddress: InetSocketAddress, timestamp: Long): Unit = unverifiedPeers.synchronized {
    unverifiedPeers.removeIf(_ == socketAddress)
    val ts = Math.max(getPeerTimestamp(socketAddress), timestamp)
    addOrUpdatePeer(socketAddress, ts)
  }

  private def allHosts(): Seq[InetAddress] =
    get(hostsIndexKey).map(InetAddressSeqCodec.decode).map(_.explicitGet().value).getOrElse(Seq.empty)


  private def isBlacklisted(host: InetAddress): Boolean =
    get(makeKey(blacklistedHostsPrefix, InetAddressCodec.encode(host))).isDefined

  private def isSuspended(host: InetAddress): Boolean =
    get(makeKey(suspendedHostsPrefix, InetAddressCodec.encode(host))).isDefined

  private def removeObsoleteHosts(): Unit = {
    val hosts = allHosts().toSet
    val validBlacklistTs = System.currentTimeMillis() - settings.blackListResidenceTime.toMillis
    val validSuspensionTs = System.currentTimeMillis() - settings.suspensionResidenceTime.toMillis
    val obsoleteSuspension = hosts.foldLeft(Set.empty[InetAddress]) { (r, h) =>
      if (getSuspensionTime(h).exists(_ < validSuspensionTs)) r + h else r
    }
    val obsoleteBlacklisting = hosts.foldLeft(Set.empty[InetAddress]) { (r, h) =>
      if (getBlacklistingDetails(h).exists(_._1 < validBlacklistTs)) r + h else r
    }
    val remove = obsoleteBlacklisting.intersect(obsoleteSuspension)

    val b = createBatch()
    obsoleteSuspension.foreach(h => delete(makeKey(suspendedHostsPrefix, InetAddressCodec.encode(h)), b))
    obsoleteBlacklisting.foreach(h => delete(makeKey(blacklistedHostsPrefix, InetAddressCodec.encode(h)), b))
    put(hostsIndexKey, InetAddressSeqCodec.encode(hosts.diff(remove).toSeq), b)
    commit(b)
  }

  private def blacklistHost(host: InetAddress, ts: Long, reason: String): Unit = {
    val hosts = allHosts() :+ host
    val b = createBatch()
    put(hostsIndexKey, InetAddressSeqCodec.encode(hosts.distinct), b)
    put(makeKey(blacklistedHostsPrefix, InetAddressCodec.encode(host)), BlackListValueCodec.encode((ts, reason)), b)
    commit(b)
  }

  private def suspendHost(host: InetAddress, ts: Long): Unit = {
    val hosts = allHosts() :+ host
    val b = createBatch()
    put(hostsIndexKey, InetAddressSeqCodec.encode(hosts.distinct), b)
    put(makeKey(suspendedHostsPrefix, InetAddressCodec.encode(host)), Longs.toByteArray(ts), b)
    commit(b)
  }

  private def getBlacklistingDetails(host: InetAddress): Option[(Long, String)] =
    get(makeKey(blacklistedHostsPrefix, InetAddressCodec.encode(host))).map(BlackListValueCodec.decode).map(_.explicitGet().value)

  private def getSuspensionTime(host: InetAddress): Option[Long] =
    get(makeKey(suspendedHostsPrefix, InetAddressCodec.encode(host))).flatMap(b => Try(Longs.fromByteArray(b)).toOption)

  private def removeBlacklistedHost(host: InetAddress): Unit = {
    val hosts = allHosts().filterNot(h => h == host)
    val b = createBatch()
    put(hostsIndexKey, InetAddressSeqCodec.encode(hosts.distinct), b)
    delete(makeKey(blacklistedHostsPrefix, InetAddressCodec.encode(host)), b)
    commit(b)
  }
}
