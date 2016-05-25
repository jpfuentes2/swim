{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Core where

import           Control.Concurrent (threadDelay, forkIO)
import           Control.Concurrent.Async (race, race_)
import           Control.Concurrent.STM (STM(..), atomically)
import           Control.Concurrent.STM.TVar
import           Control.Exception.Base (bracket)
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Control.Monad.Identity
import           Control.Monad.Trans.Either (EitherT(..), left, right, hoistEither, runEitherT)
import qualified Data.ByteString as BS (ByteString, null, append, concat, drop, empty)
import qualified Data.ByteString.Builder as BSB (word16BE, toLazyByteString)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import           Data.Conduit
import qualified Data.Conduit.Combinators as CC (map, omapE, print, stdout, sinkNull)
import           Data.Conduit.Network (runTCPServer, appSource, appSink, serverSettings, appSockAddr)
import           Data.Conduit.Network.UDP (sinkToSocket, sourceSocket)
import qualified Data.Conduit.Network.UDP as UDP (Message (..))
import           Data.Conduit.TMChan (TMChan(..), sourceTMChan, writeTMChan, newTMChanIO, sinkTMChan, readTMChan)
import           Data.Either (Either (..))
import           Data.Either.Combinators (mapLeft)
import           Data.Foldable (find)
import qualified Data.Map.Strict             as Map (elems, empty, filter,
                                                     lookup, insert)
import           Data.Maybe (fromMaybe)
import           Data.Monoid ((<>))
import           Data.Streaming.Network (getSocketUDP, getSocketTCP)
import           Data.Time.Clock (UTCTime (..), getCurrentTime)
import           Data.Word (Word16, Word32, Word8)
import           Network.Socket as NS
import           Network.Socket.Internal     (HostAddress, SockAddr (SockAddrInet))
import           System.Posix.Signals        (Handler (Catch), installHandler,
                                              sigUSR1)
import           Types
import           Util

isAlive :: Member -> Bool
isAlive = (== IsAlive) . memberAlive

isDead :: Member -> Bool
isDead = (== IsDead) . memberAlive

notAlive :: Member -> Bool
notAlive = not . isAlive

atomicIncr :: Num a => TVar a -> IO a
atomicIncr tvar = atomically incr
  where incr = do
          a' <- readTVar tvar
          let a'' = a' + 1
          writeTVar tvar a'' >> return a''

nextSeqNo :: Store -> IO Int
nextSeqNo = atomicIncr . storeSeqNo

nextIncarnation :: Store -> IO Int
nextIncarnation = atomicIncr . storeIncarnation

-- ensures nextIncarnation is >= Int
nextIncarnation' :: Store -> Int -> STM Int
nextIncarnation' s n = do
  inc <- readTVar (storeIncarnation s)
  let inc' = succ inc
  if n >= inc' then
     nextIncarnation' s n
  else
     writeTVar (storeIncarnation s) inc' >> return inc'

removeDeadNodes :: Store -> STM ()
removeDeadNodes s =
  modifyTVar' (storeMembers s) $ Map.filter (not . isDead)

-- gives kRandomNodes excluding our own host from the list
kRandomNodesExcludingSelf :: Config -> Store -> IO [Member]
kRandomNodesExcludingSelf cfg s = do
  nodes <- atomically $ readTVar $ storeMembers s
  let self = storeSelf s
  kRandomNodes (cfgGossipNodes cfg) [] (filter (/= self) $ Map.elems nodes)

-- select up to k random nodes, excluding a given
-- node and any non-alive nodes. It is possible that less than k nodes are returned.
kRandomNodes :: Int -> [Member] -> [Member] -> IO [Member]
kRandomNodes n excludes ms = take n <$> shuffle (filter f ms)
  where
    f m = notElem m excludes && IsAlive == memberAlive m

-- gossip/schedule
waitForAckOf :: AckChan -> IO ()
waitForAckOf (AckChan chan seqNo') =
  sourceTMChan chan $$ ackOf (fromIntegral seqNo') =$ CC.sinkNull >> return ()
  where
    ackOf s = awaitForever $ \ackSeqNo ->
      unless (s == ackSeqNo) $ ackOf s

members :: Store -> STM [Member]
members s = Map.elems <$> readTVar (storeMembers s)

membersAndSelf :: Store -> STM (Member, [Member])
membersAndSelf s = members s >>= (\ms -> return (self, filter (/= self) ms))
  where
    self = storeSelf s

handleAck :: Store -> Word32 -> IO ()
handleAck store seqNo' = atomically $ writeTMChan (storeAckHandler store) seqNo'

-- we're not using TCP for anything other than initial state sync
-- so we only handle pushPull / ping
handleTCPMessage :: Store -> SockAddr -> Conduit BS.ByteString IO BS.ByteString
handleTCPMessage store sockAddr = awaitForever $ \req ->
  case process req of
    Left _ -> return () -- FIXME: log it trace/debug?
    Right out -> yield out

  where process req = do
          (bs, msgType) <- decodeMsgType req
          msg <- case msgType of
                      CompoundMsg -> undefined -- FIXME: recursively handle messages
                      _ -> decode bs
          case msg of
            PushPull inc node' deadFrom' -> Left undefined
            Ping seqNo' node' -> Left undefined
            _ -> Left (("Unhandled msgType " <> show msg) :: Error) -- FIXME: error mgmt

-- FIXME: handle compound messages
handleUDPMessage :: Store -> Conduit UDP.Message IO (Message, SockAddr)
handleUDPMessage store = awaitForever $ \req ->
  case process req of
    Left _ ->
      return () -- FIXME: log it

    -- invoke ack handler for the sequence
    Right (Ack seqNo' _) -> do
      liftIO $ handleAck store seqNo'
      return ()

    -- respond with Ack if the ping was meant for us
    Right (Ping seqNo' node')
      | node' == memberName (storeSelf store) ->
        yield (Ack {seqNo = seqNo', payload = []}, UDP.msgSender req)
      | otherwise ->
        return ()

    -- send a ping to the requested target
    -- and create ack handler which relays ack from target to original requester
    Right (IndirectPing seqNo' target' port' node') -> do
      next <- liftIO $ nextIncarnation store
      yield (Ping { seqNo = fromIntegral next
                  , node = node' }
            , SockAddrInet (fromIntegral port') target')
      return ()

  where process (UDP.Message rawBytes _) = do
          (msgBytes, msgType) <- decodeMsgType rawBytes
          case msgType of
               CompoundMsg -> undefined -- FIXME: recursively handle messages
               _ -> decode msgBytes

-- disseminate receives messages for broadcasting to other members
-- messages other than Ping/IndirectPing/Ack are enqueued for piggy-backing
-- while ping/indirect-ping/ack are immediately sent
disseminate :: Store -> Conduit Gossip IO UDP.Message
disseminate store = awaitForever $ \(Gossip msg addr) ->

  -- FIXME: again, I need GADT or fix my message type so I can match using |
  -- send ping/indirect-ping/ack immediately and enqueue everything else
  case msg of
    Ping{..} -> send msg addr
    Ack{..} -> send msg addr
    IndirectPing{..} -> send msg addr
    _ -> enqueue msg addr

  where send msg addr =
          yield $ UDP.Message (encode msg) addr

        -- FIXME: need to add priority queue and then have send pull from that to create compound msg
        enqueue msg addr =
          return ()

main :: IO ()
main = do
  (config, store, self) <- configure >>= either error return
  _ <- installHandler sigUSR1 (Catch $ dumpStore store) Nothing

  -- sendAndReceiveState
  -- _ <- forkIO $ runTCPServer (serverSettings 4000 "127.0.0.1") $ \client ->
  --        appSource client $$ handleTCPMessage store (appSockAddr client) $= appSink client

  gossip <- newTMChanIO

  withSocket (bindUDP "127.0.0.1" 4000) $ \sock -> do
    -- use concurrently or mapM_ ?
    _ <- failureDetector config store gossip
    race_ (disseminate' store gossip sock)
          (listen store gossip sock)

  where disseminate' s chan sock =
          sourceTMChan chan $$ disseminate s $= sinkToSocket sock

        listen s chan sock =
          sourceSocket sock 65335 $$ mapOutput toGossip (handleUDPMessage s) $= sinkTMChan chan False
