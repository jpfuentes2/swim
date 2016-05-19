{-# LANGUAGE OverloadedStrings #-}

module Core where

import           Control.Concurrent (threadDelay, forkIO)
import           Control.Concurrent.Async (race)
import           Control.Concurrent.STM (STM(..), atomically)
import           Control.Concurrent.STM.TVar
import           Control.Exception.Base (bracket)
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Control.Monad.Identity
import           Control.Monad.Trans.Class (lift)
import qualified Data.ByteString as BS (ByteString, null, append, concat, drop, empty)
import qualified Data.ByteString.Builder as BSB (word16BE, toLazyByteString)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import           Data.Conduit
import qualified Data.Conduit.Combinators as CC (map, omapE, print, stdout, sinkNull)
import           Data.Conduit.Network (runTCPServer, appSource, appSink, serverSettings, appSockAddr)
import           Data.Conduit.Network (sinkSocket)
import qualified Data.Conduit.Network.UDP    as UDP (Message (..), msgSender,
                                                     sinkToSocket, sourceSocket)
import           Data.Conduit.TMChan (TMChan(..), sourceTMChan, writeTMChan, newTMChanIO, sinkTMChan)
import           Data.Either (Either (..))
import           Data.Either.Combinators (mapLeft)
import           Data.Foldable (find)
import qualified Data.Map.Strict             as Map (elems, empty, filter,
                                                     lookup, insert)
import           Data.Maybe (fromMaybe)
import           Data.Monoid ((<>))
import           Data.Streaming.Network (getSocketUDP, getSocketTCP)
import           Data.Time.Clock (UTCTime (..), getCurrentTime)
import           Network.Socket as NS
import           Network.Socket.Internal     (HostAddress,
                                              SockAddr (SockAddrInet))
import           System.Posix.Signals        (Handler (Catch), installHandler,
                                              sigUSR1)
import           Data.Word (Word16, Word32, Word8)
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

aliveMembers :: [Member] -> [Member]
aliveMembers = filter isAlive

removeDeadNodes :: Store -> IO ()
removeDeadNodes s =
  atomically $ modifyTVar (storeMembers s) $ Map.filter (not . isDead)

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

-- should only handle: user, pushPull, ping
-- doesn't use acks obviously
handleTCPMessage :: Store -> SockAddr -> Conduit BS.ByteString IO BS.ByteString
handleTCPMessage store sockAddr = awaitForever $ \bs ->
    let (msgData, msgSender) = (bs, sockAddr)
        -- n = BS.unpack $ BS.take 1 msgData
        raw = BS.drop 1 msgData
        msg = either (const Nothing) Just $ decode raw
    in
        case msg of
            Just (Ack seq payload) -> return ()
            Just (Alive incarnation n addr port vsn) -> return ()
            Just (Ping seqNo node) -> return ()

            -- failed to parse
            Nothing -> return ()

            -- not implemented
            _ -> return ()

getSocketUDP' :: String -> Int -> IO Socket
getSocketUDP' host port = do
    (sock, info) <- getSocketUDP host port
    -- reuse since hashicorp/memberlist seems to want us to use same port
    setSocketOption sock ReuseAddr 1
    NS.bind sock (addrAddress info) >> return sock

makeStore :: Member -> IO Store
makeStore self = do
    mems <- newTVarIO Map.empty
    -- num <- newTVarIO 0
    events <- newTVarIO []
    seqNo <- newTVarIO 0
    inc <- newTVarIO 0
    ackHandler <- newTMChanIO
    let store = Store { storeSeqNo = seqNo
                      , storeIncarnation = inc
                      , storeMembers = mems
                      , storeSelf = self
                      , storeAckHandler = ackHandler
                      -- , storeNumMembers = num
                      }
    return store

-- gossip/schedule
waitForAckOf :: AckChan -> IO ()
waitForAckOf (AckChan chan seqNo') =
  sourceTMChan chan $$ ackOf (fromIntegral seqNo') =$ CC.sinkNull >> return ()
  where
    ackOf s = awaitForever $ \ackSeqNo ->
      if s == ackSeqNo then return () else ackOf s

members :: Store -> STM [Member]
members s = Map.elems <$> readTVar (storeMembers s)

membersAndSelf :: Store -> STM (Member, [Member])
membersAndSelf s = members s >>= (\ms -> return (self, filter (/= self) ms))
  where
    self = storeSelf s

-- ping -> respond with ack
-- ack -> invoke ack handler (a single chan to a map of handlers?)
-- compound -> recursively call self -- may need to switch to ConduitM
-- indirect -> ping the remote host & *create* ack handler
handleUDPMessage :: Store -> Conduit UDP.Message IO (Message, SockAddr)
handleUDPMessage store = awaitForever $ \r ->
    let (msgData, msgSender) = (UDP.msgData r, UDP.msgSender r)
        -- n = BS.unpack $ BS.take 1 msgData
        raw = BS.drop 1 msgData
        msg = either (const Nothing) Just $ decode raw
    in
        case msg of
            Just (Ack _seqNo _) -> do
              liftIO $ atomically $ writeTMChan (storeAckHandler store) _seqNo
              return ()

            Just (Ping _seqNo _node)
              | _node == memberName (storeSelf store) ->
                yield (Ack {seqNo = _seqNo, payload = []}, msgSender)
              | otherwise ->
                return ()

            Just (IndirectPing seqNo fromAddr node) -> do
              return ()

            -- Just (Dead incarnation node from) -> do
            --   found <- atomically $ do
            --     mems <- readTVar $ storeMembers store

            -- failed to parse
            Nothing -> return ()

            -- not implemented
            _ -> return ()

-- disseminator takes care of gossiping messages from both
-- acks received from the ackHandler chan and broadcast requests
-- to a member using our UDP socket
disseminator :: Store -> Socket -> TMChan (Message, SockAddr) -> IO ()
disseminator store socket chan = undefined
  -- UDP.sinkToSocket udpSocket

dumpStore :: Store -> IO ()
dumpStore s = do
  (seqNo, inc, membs, self) <- atomically $ do
    seqNo <- readTVar $ storeSeqNo s
    inc <- readTVar $ storeIncarnation s
    membs <- readTVar $ storeMembers s
    return (seqNo, inc, membs, storeSelf s)

  print $ "(seqNo, inc) " <> show (seqNo, inc)
  print $ "members: " <> show membs
  print $ "self: " <> show self

blah = do
    now <- getCurrentTime
    let self = Member { memberName = "myself"
                      , memberHost = "localhost"
                      , memberAlive = IsAlive
                      , memberIncarnation = 0
                      , memberLastChange = now
                      }
    store <- makeStore self
    _ <- installHandler sigUSR1 (Catch $ dumpStore store) Nothing
    gossipChan <- newTMChanIO

    forkIO $
      runTCPServer (serverSettings 4000 "127.0.0.1") $ \client ->
        appSource client $$ handleTCPMessage store (appSockAddr client) =$ appSink client

    -- sendAndReceiveState
    withSocket (getSocketUDP' "127.0.0.1" 4000) $ \udpSocket -> do
      forkIO $ disseminator store udpSocket gossipChan
      UDP.sourceSocket udpSocket 65335 $$ handleUDPMessage store $= sinkTMChan gossipChan False
