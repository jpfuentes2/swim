import qualified Core
import           Types

import           Control.Concurrent.STM ( atomically )
import           Control.Concurrent.STM.TVar ( newTVarIO, writeTVar, modifyTVar, swapTVar, readTVar )
import           Data.Atomics.Counter ( newCounter )
import qualified Data.ByteString as BS (ByteString(..), drop, pack, unpack, singleton, append)
import qualified Data.ByteString.Char8 as C8 (pack)
import           Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Network.UDP    as UDP ( Message(..), msgSender
                                                    , sourceSocket, sinkToSocket )
import           Data.Time.Clock             ( UTCTime(..), getCurrentTime )
import           Data.Time.Calendar             ( Day(ModifiedJulianDay) )
import qualified Data.Map.Strict as Map
import           Data.Foldable               ( foldl' )
import Data.Bits (shiftL, (.|.))
import           Data.Word ( Word16, Word32, Word8 )
import           Network.Socket.Internal ( SockAddr(SockAddrInet) )
import           Test.Hspec

fromOctets :: [Word8] -> Word32
fromOctets = foldl' accum 0
  where
    accum a o = (a `shiftL` 8) .|. fromIntegral o

withStore :: (Store -> IO ()) -> IO ()
withStore f = Core.makeStore >>= f

zeroTime = UTCTime (ModifiedJulianDay 0) 0

makeMembers :: [Member]
makeMembers =
  let alive = Member { memberName = "alive", memberHost = "", memberAlive = IsAlive, memberIncarnation = 0, memberLastChange = zeroTime }
      suspect = Member { memberName = "suspect", memberHost = "", memberAlive = IsSuspect, memberIncarnation = 0, memberLastChange = zeroTime }
      dead = Member { memberName = "dead", memberHost = "", memberAlive = IsDead, memberIncarnation = 0, memberLastChange = zeroTime }
  in [alive, suspect, dead]

membersMap :: [Member] -> Map.Map String Member
membersMap ms =
  Map.fromList $ map (\m -> (memberName m, m)) ms

encodeWire :: Message -> BS.ByteString
encodeWire m = BS.append (BS.singleton $ msgIndex m) $ Core.encode m

udpMsg' :: Event -> UDP.Message
udpMsg' e = UDP.Message { UDP.msgData = body, UDP.msgSender = sockAddr }
  where body = maybe (eventBody e) encodeWire $ eventMsg e

sockAddr = SockAddrInet 4002 $ fromOctets $ BS.unpack $ C8.pack "127.0.0.1"

main :: IO ()
main = hspec $ do
  describe "handleMessage" $ do
    context "when received Ping" $
      it "produces an Ack" $ withStore $ \s -> do
        let ping = Ping { seqNo = 0, node = "node" }
            event = Event { eventHost = From "sender", eventMsg = Just ping, eventBody = Core.encode ping }

        res <- CL.sourceList [udpMsg' event] $$ Core.handleMessage s $= CL.consume
        let events = map Core.fromMsg res
            e = head events

        length events `shouldBe` 1
        eventHost e `shouldBe` To (show sockAddr)
        eventMsg e `shouldBe` Just Ack { seqNo = seqNo ping, payload = [] }

    context "when received Ack" $ do
      it "it's ignored" $ withStore $ \s -> do
        let ack = Ack { seqNo = 0, payload = [] }
            event = Event { eventHost = From "sender", eventMsg = Just ack, eventBody = Core.encode ack }
        res <- CL.sourceList [udpMsg' event] $$ Core.handleMessage s $= CL.consume

        length res `shouldBe` 0

  describe "encode and decode" $ do
    it "encodes & decodes" $ do
      let ping = Ping { seqNo = 1, node = "a" }
          decoded = Core.decode $ Core.encode ping

      decoded `shouldBe` Right ping

    -- it "decodes external messages" $ do
    --   let ping = Ping { seqNo = 1, node = "jax-ruby" }
    --       bs = BS.drop 1 $ C8.pack "��SeqNo�Node�jax-ruby"
    --       decoded = Core.decode bs

    --   decoded `shouldBe` Right ping

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
      let alive = map (\i -> Member { memberName = show i, memberHost = "", memberAlive = IsAlive, memberIncarnation = 1, memberLastChange = zeroTime } ) [0..n]
          n = 20
      rand <- Core.kRandomNodes n [] alive
      length rand `shouldBe` n
      rand `shouldNotBe` alive
