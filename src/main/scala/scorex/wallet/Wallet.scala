package scorex.wallet

import com.google.common.primitives.{Bytes, Ints}
import com.wavesplatform.db.{PropertiesStorage, SubStorage}
import com.wavesplatform.settings.WalletSettings
import com.wavesplatform.state2.ByteStr
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

  def seed: Option[Array[Byte]] = get(SeedProperty)

  def nonce(): Int = getInt(NonceProperty).getOrElse(0)

  def privateKeyAccounts(): List[PrivateKeyAccount] = map(PrivateKeyPrefix).values.map(PrivateKeyAccount.apply).toList

  def generateNewAccount(): Option[PrivateKeyAccount] = synchronized {
    val nonce = incrementAndGetNonce()
    val account = Wallet.generateNewAccount(seed.get, nonce)
    val address = account.address

    if (getPrivateKeyAccount(address).isEmpty) {
      putPrivateKeyAccount(account)
      Some(account)
    } else None
  }

  def generateNewAccounts(howMany: Int): Seq[PrivateKeyAccount] =
    (1 to howMany).flatMap(_ => generateNewAccount())

  def deleteAccount(account: PrivateKeyAccount): Boolean = synchronized {
    getPrivateKeyAccount(account.address).fold(false) { a =>
      delete(makeKey(PrivateKeyPrefix, a.address))
      true
    }
  }

  def privateKeyAccount(account: Address): Either[ValidationError, PrivateKeyAccount] =
    getPrivateKeyAccount(account.address).toRight[ValidationError](MissingSenderPrivateKey)

  def addSeed(seed: Array[Byte]): Unit = if (get(SeedProperty).isEmpty) put(SeedProperty, seed)

  private def incrementAndGetNonce(): Int = synchronized {
    val nextNonce = nonce() + 1
    putInt(NonceProperty, nextNonce)
    nextNonce
  }

  private def getPrivateKeyAccount(address: String): Option[PrivateKeyAccount] =
    get(makeKey(PrivateKeyPrefix, address)).map(PrivateKeyAccount.apply)

  private def putPrivateKeyAccount(account: PrivateKeyAccount): Unit =
    put(makeKey(PrivateKeyPrefix, account.address), account.seed)

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
