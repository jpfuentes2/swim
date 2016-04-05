import qualified Core
import           Types

import           Control.Concurrent.STM ( atomically )
import           Control.Concurrent.STM.TVar ( newTVarIO, writeTVar, modifyTVar, swapTVar, readTVar )
import           Data.Atomics.Counter ( newCounter )
import qualified Data.ByteString as BS (drop, pack, singleton, append)
import qualified Data.ByteString.Char8 as C8 (pack)
import qualified Data.Map.Strict as Map
import           Data.Word ( Word16, Word32, Word8 )
import           Test.Hspec

withStore :: (Store -> IO ()) -> IO ()
withStore f = Core.makeStore >>= f

makeMembers :: [Member]
makeMembers =
  let alive = Member { memberName = "alive", memberHost = "", memberAlive = IsAlive }
      suspect = Member { memberName = "suspect", memberHost = "", memberAlive = IsSuspect }
      dead = Member { memberName = "dead", memberHost = "", memberAlive = IsDead }
  in [alive, suspect, dead]

membersMap :: [Member] -> Map.Map String Member
membersMap ms =
  Map.fromList $ map (\m -> (memberName m, m)) ms

main :: IO ()
main = hspec $ do
  describe "encode and decode" $ do
    it "encodes & decodes" $ do
      let ping = Ping { seqNo = 1, node = "a" }
          decoded = Core.decode $ Core.encode ping

      decoded `shouldBe` Right ping

    it "decodes external messages" $ do
      let ping = Ping { seqNo = 1, node = "jax-ruby" }
          bs = BS.drop 0 $ C8.pack "��SeqNo�Node�jax-ruby"
          decoded = Core.decode bs

      print $ show bs
      decoded `shouldBe` Right ping

  describe "Core.removeDeadNodes" $
    it "removes dead members" $ withStore $ \s -> do
      _ <- atomically $ swapTVar (storeMembers s) $ membersMap makeMembers
      _ <- Core.removeDeadNodes s
      mems' <- atomically $ readTVar $ storeMembers s

      Map.notMember "dead" mems' `shouldBe` True
      Map.size mems' `shouldBe` 2

  describe "Core.kRandomNodes" $ do
    let ms = makeMembers

    it "takes no nodes if n is 0" $ do
      rand <- Core.kRandomNodes 0 ms ms
      length rand `shouldBe` 0

    it "filters non-alive nodes" $ do
      rand <- Core.kRandomNodes 3 [] ms
      length rand `shouldBe` 1
      head rand `shouldBe` head ms

    it "filters exclusion nodes" $ do
      rand <- Core.kRandomNodes 3 [head ms] ms
      length rand `shouldBe` 0

    it "shuffles" $ do
      let alive = map (\i -> Member { memberName = show i, memberHost = "", memberAlive = IsAlive} ) [0..n]
          n = 20
      rand <- Core.kRandomNodes n [] alive
      length rand `shouldBe` n
      rand `shouldNotBe` alive
