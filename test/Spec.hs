{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE LambdaCase #-}

import           Control.Concurrent.STM ( atomically )
import           Data.Bits (shiftL, (.|.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import           Control.Concurrent.STM.TVar (swapTVar, readTVar)
import           Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Combinators    as CC
import qualified Data.Conduit.Network.UDP    as UDP
import           Data.Monoid ( (<>) )
import           Data.Foldable ( foldl' )
import           Data.List (find, sort)
import           Control.Monad.Identity
import           Control.Monad.IO.Class      (MonadIO (liftIO))
import qualified Data.Map.Strict as Map
import           Data.Time.Calendar ( Day(ModifiedJulianDay) )
import           Data.Time.Clock ( UTCTime(..), getCurrentTime )
import           Data.Word ( Word16, Word32, Word8 )
import           Network.Socket.Internal ( SockAddr(SockAddrInet), PortNumber )
import           Data.Serialize (decode, encode, get)
import qualified Data.List.NonEmpty          as NEL
import           Test.Hspec

import qualified Core
import           Types
import           Util

withStore :: (Store -> IO ()) -> IO ()
withStore f = configure >>= \ case
  Left e -> liftIO $ fail $ "configure failed" <> show e
  Right store -> f store

zeroTime :: UTCTime
zeroTime = UTCTime (ModifiedJulianDay 0) 0

fromOctets :: [Word8] -> Word32
fromOctets = foldl' accum 0
  where
    accum a o = (a `shiftL` 8) .|. fromIntegral o

sockAddr :: PortNumber -> SockAddr
sockAddr _port = SockAddrInet _port $ fromOctets $ BS.unpack $ C8.pack "127.0.0.1"

makeMembers :: [Member]
makeMembers =
  let seeds = [  ("alive", IsAlive, zeroTime, sockAddr 4001)
               , ("suspect", IsSuspect, zeroTime, sockAddr 4002)
               , ("dead", IsDead, zeroTime, sockAddr 4003)]
  in map (\(name, status, timeChanged, addr) ->
             Member { memberName = name
                    , memberHost = "127.0.0.1"
                    , memberAlive = status
                    , memberIncarnation = 0
                    , memberLastChange = timeChanged
                    , memberHostNew = addr }) seeds

alive = head makeMembers

membersMap :: [Member] -> Map.Map String Member
membersMap ms =
  Map.fromList $ map (\m -> (memberName m, m)) ms

currentIncarnation :: Store -> IO Int
currentIncarnation Store{..} = atomically $ readTVar storeIncarnation
-- sockAddr = SockAddrInet 4002 $ fromOctets $ BS.unpack $ C8.pack "127.0.0.1"

envelope :: [Message] -> Envelope
envelope msgs = Envelope $ NEL.fromList msgs

backAndForthForeverAndEver msgs = decode (encode $ envelope msgs)

main :: IO ()
main = hspec $ do
  let addr = sockAddr 4000
--   describe "handleMessage" $ do
--     context "when received Ping" $
--       it "produces an Ack" $ withStore $ \s -> do
--         let ping = Ping { seqNo = 0, node = "node" }
--             event = Event { eventHost = From "sender", eventMsg = Just ping, eventBody = Core.encode ping }

--         res <- CL.sourceList [udpMsg' event] $$ Core.handleMessage s $= CL.consume
--         let events = map Core.fromMsg res
--             e = head events

--         length events `shouldBe` 1
--         eventHost e `shouldBe` To (show sockAddr)
--         eventMsg e `shouldBe` Just Ack { seqNo = seqNo ping, payload = [] }

--     context "when received Ack" $ do
--       it "it's ignored" $ withStore $ \s -> do
--         let ack = Ack { seqNo = 0, payload = [] }
--             event = Event { eventHost = From "sender", eventMsg = Just ack, eventBody = Core.encode ack }
--         res <- CL.sourceList [udpMsg' event] $$ Core.handleMessage s $= CL.consume

--         length res `shouldBe` 0

  describe "wire protocol" $ do
    it "encodes & decodes" $ do
      let ping = Ping { seqNo = 1, node = "a" }
          indirectPing = IndirectPing { seqNo = 1, target = 1, port = 4000, node = "a" }

      backAndForthForeverAndEver [ping] `shouldBe` Right (envelope [ping])
      backAndForthForeverAndEver [indirectPing] `shouldBe` Right (envelope [indirectPing])

    it "encodes & decodes envelope/compound" $ do
      let ping1 = Ping { seqNo = 1, node = "a" }
          ack1 = Ack { seqNo = 2, payload = [] }
          ping2 = Ping { seqNo = 3, node = "b" }
          ack2 = Ack { seqNo = 4, payload = [] }
          msgs = [ping1, ack1, ping2, ack2]

      backAndForthForeverAndEver msgs `shouldBe` Right (envelope msgs)

  describe "Core.removeDeadNodes" $
    it "removes dead members" $ withStore $ \s@Store{..} -> do
      _ <- atomically $ do
        void $ swapTVar storeMembers $ membersMap makeMembers
        void $ Core.removeDeadNodes s
      mems' <- atomically $ readTVar storeMembers

      Map.notMember "dead" mems' `shouldBe` True
      Map.size mems' `shouldBe` 2

  describe "Core.membersAndSelf" $
    it "gives self fst and everyone else snd" $ withStore $ \s@Store{..} -> do
      let mems = makeMembers

      _ <- atomically $ swapTVar storeMembers $ membersMap (mems <> [storeSelf])
      (self', mems') <- atomically $ Core.membersAndSelf s

      storeSelf `shouldBe` self'
      find (== storeSelf) mems' `shouldBe` Nothing
      sort mems `shouldBe` sort mems'

  describe "Core.kRandomNodesExcludingSelf" $
    it "excludes self" $ withStore $ \s@Store{..} -> do
      _ <- atomically $ swapTVar storeMembers $ membersMap (makeMembers <> [storeSelf])
      mems <- Core.kRandomNodesExcludingSelf (numToGossip storeCfg) s

      find (== storeSelf) mems `shouldBe` Nothing
      length mems `shouldBe` 3

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
      let alives = map (const alive) [0..n]
          n = 200

      rand <- Core.kRandomNodes n [] alives
      length rand `shouldBe` n
      rand `shouldNotBe` alives

  describe "Core.handleUDPMessage FIXME" $ do
    let ping = Ping 1 "myself"
        ack = Ack 1 []
        send s msg = do
          let udpMsg = UDP.Message (encode msg) (sockAddr 4000)
          CL.sourceList [udpMsg] $$ Core.handleUDPMessage s =$= CC.sinkList
        invokesAckHandler = undefined

    it "gets Ping, sends Ack" $ withStore $ \s@Store{..} -> do
      gossip <- send s ping

      head gossip `shouldBe` Direct (Ack 1 []) (sockAddr 4000)

  describe "Core.handleUDPMessage" $ do
    let ping = Ping 1 "myself"
        ack = Ack 1 []
        indirectPing (SockAddrInet target port) = IndirectPing 1 (fromIntegral target) (fromIntegral port) "other"
        send s msg = do
          let udpMsg = UDP.Message (encode (Envelope $ NEL.fromList [msg])) addr
          CL.sourceList [udpMsg] $$ Core.handleUDPMessage s =$= CC.sinkList
        invokesAckHandler = undefined

    it "gets Ping for us, responds with Ack" $ withStore $ \s@Store{..} -> do
      gossip <- send s ping

      gossip `shouldBe` [Direct (Ack 1 []) addr]

    it "gets Ping for someone else, ignores msg" $ withStore $ \s@Store{..} -> do
      gossip <- send s $ Ping 1 "unknown-node"

      gossip `shouldBe` []

    it "gets Ack, invokes ackHandler" $ withStore $ \s@Store{..} -> do
      gossip <- send s ack

      gossip `shouldBe` []
      -- invokesAckHandler $ seqNo ack

    it "gets IndirectPing, sends Ping" $ withStore $ \s@Store{..} -> do
      let indirectPing' = indirectPing addr
          udpMsg = UDP.Message (encode (Envelope $ NEL.fromList [indirectPing'])) addr
      beforeInc <- currentIncarnation s
      gossip <- send s indirectPing'
      afterInc <- currentIncarnation s

      beforeInc + 1 `shouldBe` afterInc
      gossip `shouldBe` [Direct (Ping 1 (node indirectPing')) addr]
      -- invokesAckHandler $ seqNo ack
