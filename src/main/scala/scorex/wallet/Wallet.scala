package scorex.wallet

import com.google.common.primitives.{Bytes, Ints}
import com.wavesplatform.db.{ByteStrSeqCodec, PropertiesStorage, SubStorage}
import com.wavesplatform.settings.WalletSettings
import com.wavesplatform.state2._
import org.iq80.leveldb.DB
import scorex.account.{Address, PrivateKeyAccount}
import scorex.crypto.hash.SecureCryptographicHash
import scorex.transaction.ValidationError
import scorex.transaction.ValidationError.MissingSenderPrivateKey
import scorex.utils.{ScorexLogging, randomBytes}

class Wallet private(db: DB, password: Array[Char]) extends SubStorage(db, "wallet") with PropertiesStorage {

  private val NonceProperty = "nonce"
  private val SeedProperty = "seed"
  private val PrivateKeyPrefix = "sk".getBytes(Charset)
  private val indexKey = makeKey("sk-index".getBytes(Charset), 0)

  def seed: Option[Array[Byte]] = get(SeedProperty)

  def nonce(): Int = getInt(NonceProperty).getOrElse(-1)

  def privateKeyAccounts(): List[PrivateKeyAccount] = allAddresses().flatMap(a => getPrivateKeyAccount(a)).toList

  def generateNewAccount(): Option[PrivateKeyAccount] = synchronized {
    val nonce = incrementAndGetNonce()
    val account = Wallet.generateNewAccount(seed.get, nonce)
    val address = account.toAddress

    if (getPrivateKeyAccount(address).isEmpty) {
      putPrivateKeyAccount(account)
      Some(account)
    } else None
  }

  def generateNewAccounts(howMany: Int): Seq[PrivateKeyAccount] =
    (1 to howMany).flatMap(_ => generateNewAccount())

  def deleteAccount(account: PrivateKeyAccount): Boolean = synchronized {
    val address = account.toAddress
    if (getPrivateKeyAccount(address).isDefined) {
      deletePrivateKeyAccount(address)
      true
    } else false
  }

  def privateKeyAccount(address: Address): Either[ValidationError, PrivateKeyAccount] =
    getPrivateKeyAccount(address).toRight[ValidationError](MissingSenderPrivateKey)

  def addSeed(seed: Array[Byte]): Unit = if (get(SeedProperty).isEmpty) put(SeedProperty, seed)

  private def incrementAndGetNonce(): Int = synchronized {
    val nextNonce = nonce() + 1
    putInt(NonceProperty, nextNonce)
    nextNonce
  }

  private def getPrivateKeyAccount(address: Address): Option[PrivateKeyAccount] =
    get(makeKey(PrivateKeyPrefix, address.bytes.arr)).map(PrivateKeyAccount.apply)

  private def putPrivateKeyAccount(account: PrivateKeyAccount): Unit = {
    val addresses = allAddresses() :+ account.toAddress
    put(makeKey(PrivateKeyPrefix, account.toAddress.bytes.arr), account.seed)
    put(indexKey, ByteStrSeqCodec.encode(addresses.distinct.map(a => a.bytes)))
  }

  private def deletePrivateKeyAccount(address: Address): Unit = {
    delete(makeKey(PrivateKeyPrefix, address.bytes.arr))
    val addresses = allAddresses().filterNot(a => a == address).map(a => a.bytes).distinct
    put(indexKey, ByteStrSeqCodec.encode(addresses))
  }

  private def allAddresses(): Seq[Address] = {
    get(indexKey).map(ByteStrSeqCodec.decode).map(_.explicitGet().value).map { s =>
      s.map { bs =>
        Address.fromBytes(bs.arr).explicitGet()
      }
    }.getOrElse(Seq.empty)
  }
}

object Wallet extends ScorexLogging {

  implicit class WalletExtension(w: Wallet) {
    def findWallet(addressString: String): Either[ValidationError, PrivateKeyAccount] = for {
      acc <- Address.fromString(addressString)
      privateKeyAccount <- w.privateKeyAccount(acc)
    } yield privateKeyAccount

    def exportAccountSeed(account: Address): Either[ValidationError, Array[Byte]] = w.privateKeyAccount(account).map(_.seed)
  }

  def generateNewAccount(seed: Array[Byte], nonce: Int): PrivateKeyAccount = {
    val accountSeed = generateAccountSeed(seed, nonce)
    PrivateKeyAccount(accountSeed)
  }

  def generateAccountSeed(seed: Array[Byte], nonce: Int): Array[Byte] = SecureCryptographicHash(Bytes.concat(Ints.toByteArray(nonce), seed))

  def apply(db: DB, settings: WalletSettings): Wallet = {
    val wallet: Wallet = new Wallet(db, settings.password.toCharArray)

    if (wallet.seed.isEmpty) {
      val seed: ByteStr = settings.seed.getOrElse {
        val SeedSize = 64
        val randomSeed = ByteStr(randomBytes(SeedSize))
        log.info(s"You random generated seed is ${randomSeed.base58}")
        randomSeed
      }
      wallet.addSeed(seed.arr)
    }
    wallet
  }
}
