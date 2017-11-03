package com.wavesplatform.history

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.common.primitives.{Ints, Shorts}
import com.twitter.chill.{KryoInstantiator, KryoPool}
import com.wavesplatform.db.{PropertiesStorage, SubStorage, VersionedStorage}
import com.wavesplatform.features.FeatureProvider
import com.wavesplatform.settings.{FeaturesSettings, FunctionalitySettings}
import com.wavesplatform.state2.{BlockDiff, ByteStr}
import com.wavesplatform.utils._
import kamon.Kamon
import org.iq80.leveldb.DB
import scorex.block.Block
import scorex.transaction.History.BlockchainScore
import scorex.transaction.ValidationError.GenericError
import scorex.transaction._
import scorex.utils.ScorexLogging

import scala.util.Try

class HistoryWriterImpl private(db: DB, val synchronizationToken: ReentrantReadWriteLock,
                                functionalitySettings: FunctionalitySettings, featuresSettings: FeaturesSettings)
  extends SubStorage(db, "history") with PropertiesStorage with VersionedStorage with History with FeatureProvider with ScorexLogging {

  override protected val Version: Int = 1

  import HistoryWriterImpl._

  override val activationWindowSize: Int = functionalitySettings.featureCheckBlocksPeriod
  val MinVotesWithinWindowToActivateFeature: Int = functionalitySettings.blocksForFeatureActivation

  private val BlockAtHeightPrefix = "blocks".getBytes(Charset)
  private val SignatureAtHeightPrefix = "signatures".getBytes(Charset)
  private val HeightBySignaturePrefix = "heights".getBytes(Charset)
  private val ScoreAtHeightPrefix = "scores".getBytes(Charset)
  private val VotesAtHeightPrefix = "votes".getBytes(Charset)
  private val FeatureStatePrefix = "features".getBytes(Charset)

  private val HeightProperty = "height"

  private[HistoryWriterImpl] def isConsistent: Boolean = read { implicit l =>
    true
    // check if all maps have same size
    //    Set(blockBodyByHeight().size(), blockIdByHeight().size(), heightByBlockId().size(), scoreByHeight().size()).size == 1
  }

  private lazy val preAcceptedFeatures = functionalitySettings.preActivatedFeatures.mapValues(h => h - activationWindowSize)

  override def approvedFeatures(): Map[Short, Int] = read { implicit lock =>
    preAcceptedFeatures ++ map(FeatureStatePrefix).map(e => Shorts.fromByteArray(e._1) -> Ints.fromByteArray(e._2))
  }

  override def featureVotesCountWithinActivationWindow(height: Int): Map[Short, Int] = read { implicit lock =>
    val votingWindowOpening = FeatureProvider.votingWindowOpeningFromHeight(height, activationWindowSize)
    get(makeKey(VotesAtHeightPrefix, votingWindowOpening)).fold(Map.empty[Short, Int])(decodeVotesValue)
  }

  private def alterVotes(height: Int, votes: Set[Short], voteMod: Int): Unit = write { implicit lock =>
    val votingWindowOpening = FeatureProvider.votingWindowOpeningFromHeight(height, activationWindowSize)
    val votesWithinWindow = featureVotesCountWithinActivationWindow(height)
    val newVotes = votes.foldLeft(votesWithinWindow)((v, feature) => v + (feature -> (v.getOrElse(feature, 0) + voteMod)))
    put(makeKey(VotesAtHeightPrefix, votingWindowOpening), encodeVotesValue(newVotes))
  }

  def appendBlock(block: Block, acceptedFeatures: Set[Short])(consensusValidation: => Either[ValidationError, BlockDiff]): Either[ValidationError, BlockDiff] =
    write { implicit lock =>

      assert(block.signaturesValid().isRight)

      if ((height() == 0) || (this.lastBlock.get.uniqueId == block.reference)) consensusValidation.map { blockDiff =>
        val h = height() + 1
        val score = (if (height() == 0) BigInt(0) else this.score()) + block.blockScore
        put(makeKey(BlockAtHeightPrefix, h), block.bytes)
        put(makeKey(ScoreAtHeightPrefix, h), score.toByteArray)
        put(makeKey(SignatureAtHeightPrefix, h), block.uniqueId.arr)
        put(makeKey(HeightBySignaturePrefix, block.uniqueId.arr), Ints.toByteArray(h))
        putInt(HeightProperty, h)

        val presentFeatures = map(FeatureStatePrefix).keySet.map(Shorts.fromByteArray)
        val newFeatures = acceptedFeatures.diff(presentFeatures)
        newFeatures.foreach(f => put(makeKey(FeatureStatePrefix, Shorts.toByteArray(f)), Ints.toByteArray(h)))
        alterVotes(h, block.featureVotes, 1)

        blockHeightStats.record(h)
        blockSizeStats.record(block.bytes().length)
        transactionsInBlockStats.record(block.transactionData.size)

        log.trace(s"Full Block $block(id=${block.uniqueId} persisted")
        blockDiff
      }
      else {
        Left(GenericError(s"Parent ${block.reference} of block ${block.uniqueId} does not match last block ${this.lastBlock.map(_.uniqueId)}"))
      }
    }

  def discardBlock(): Option[Block] = write { implicit lock =>
    val h = height()

    alterVotes(h, blockAt(h).map(b => b.featureVotes).getOrElse(Set.empty), -1)

    val key = makeKey(BlockAtHeightPrefix, h)
    val maybeDiscardedBlock = get(key).flatMap(b => Block.parseBytes(b).toOption)

    delete(key)
    delete(makeKey(ScoreAtHeightPrefix, h))

    if (h % activationWindowSize == 0) {
      map(FeatureStatePrefix, stripPrefix = false).foreach { e =>
        if (Ints.fromByteArray(e._2) == h) delete(e._1)
      }
    }

    val signatureKey = makeKey(SignatureAtHeightPrefix, h)
    get(signatureKey).foreach(b => delete(makeKey(HeightBySignaturePrefix, b)))
    delete(signatureKey)

    maybeDiscardedBlock
  }

  override def lastBlockIds(howMany: Int): Seq[ByteStr] = read { implicit lock =>
    val startHeight = Math.max(1, height - howMany + 1)
    (startHeight to height).flatMap(getBlockSignature).reverse
  }

  override def height(): Int = read { implicit lock =>
    getInt(HeightProperty).getOrElse(0)
  }

  override def scoreOf(id: ByteStr): Option[BlockchainScore] = read { implicit lock =>
    val maybeHeight = heightOf(id)
    if (maybeHeight.isDefined) {
      val maybeScoreBytes = get(makeKey(ScoreAtHeightPrefix, maybeHeight.get))
      if (maybeScoreBytes.isDefined) Some(BigInt(maybeScoreBytes.get)) else None
    } else None
  }

  override def heightOf(blockSignature: ByteStr): Option[Int] = read { implicit lock =>
    get(makeKey(HeightBySignaturePrefix, blockSignature.arr)).map(Ints.fromByteArray)
  }

  override def blockBytes(height: Int): Option[Array[Byte]] = read { implicit lock =>
    get(makeKey(BlockAtHeightPrefix, height))
  }

  override def close(): Unit = db.close()

  override def lastBlockTimestamp(): Option[Long] = this.lastBlock.map(_.timestamp)

  override def lastBlockId(): Option[ByteStr] = this.lastBlock.map(_.signerData.signature)

  override def blockAt(height: Int): Option[Block] = blockBytes(height).map(Block.parseBytes(_).get)

  private def getBlockSignature(height: Int): Option[ByteStr] = get(makeKey(SignatureAtHeightPrefix, height)).map(ByteStr.apply)

}

object HistoryWriterImpl extends ScorexLogging {

  private val PoolSize = 10
  private val kryo = KryoPool.withByteArrayOutputStream(PoolSize, new KryoInstantiator())

  def encodeVotesValue(value: Map[Short, Int]): Array[Byte] = kryo.toBytesWithClass(value)

  def decodeVotesValue(arr: Array[Byte]): Map[Short, Int] = kryo.fromBytes(arr, classOf[Map[Short, Int]])

  def apply(db: DB, synchronizationToken: ReentrantReadWriteLock, functionalitySettings: FunctionalitySettings,
            featuresSettings: FeaturesSettings): Try[HistoryWriterImpl] =
    createWithVerification[HistoryWriterImpl](new HistoryWriterImpl(db, synchronizationToken, functionalitySettings, featuresSettings), h => h.isConsistent)

  private val blockHeightStats = Kamon.metrics.histogram("block-height")
  private val blockSizeStats = Kamon.metrics.histogram("block-size-bytes")
  private val transactionsInBlockStats = Kamon.metrics.histogram("transactions-in-block")
}
