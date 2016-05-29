{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

module Core where

import           Control.Concurrent (forkIO)
import           Control.Concurrent.Async (race_, race)
import           Control.Concurrent.STM (STM, atomically)
import           Control.Concurrent.STM.TVar
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Control.Monad.Identity
import qualified Data.ByteString as BS
import           Data.Conduit (Conduit, ConduitM, Producer, ($$), (=$=), awaitForever, yield)
import           Data.Conduit.Cereal (conduitGet)
import           Data.Conduit.List (sourceList)
import qualified Data.Conduit.Combinators as CC
import           Data.Conduit.Network (runTCPServer, appSource, appSink, serverSettings, appSockAddr)
import           Data.Conduit.Network.UDP (sinkToSocket)
import qualified Data.Conduit.Network.UDP as UDP
import           Data.Conduit.TMChan (TMChan, sourceTMChan, writeTMChan, newTMChanIO, sinkTMChan)
import           Data.Foldable (find)
import qualified Data.List.NonEmpty as NEL
import qualified Data.Map.Strict as Map
import           Data.Monoid ((<>))
import           Data.Serialize (decode, encode, get)
import           Data.Time.Clock (getCurrentTime)
import           Data.Word (Word16, Word32)
import           Network.Socket as NS
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
  modifyTVar (storeMembers s) $ Map.filter (not . isDead)

-- gives kRandomNodes excluding our own host from the list
kRandomNodesExcludingSelf :: Int -> Store -> IO [Member]
kRandomNodesExcludingSelf numNodes s = do
  nodes <- atomically $ readTVar $ storeMembers s
  let self = storeSelf s
  kRandomNodes numNodes [] (filter (/= self) $ Map.elems nodes)

-- select up to k random nodes, excluding a given
-- node and any non-alive nodes. It is possible that less than k nodes are returned.
kRandomNodes :: Int -> [Member] -> [Member] -> IO [Member]
kRandomNodes n excludes ms = take n <$> shuffle (filter f ms)
  where
    f m = notElem m excludes && IsAlive == memberAlive m

-- gossip/schedule
waitForAckOf :: AckChan -> IO ()
waitForAckOf (AckChan chan seqNo') =
  sourceTMChan chan $$ ackOf (fromIntegral seqNo') =$= CC.sinkNull >> return ()
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
handleTCPMessage _store _sockAddr =
  conduitGet get =$= CC.map unEnvelope =$= CC.concat =$= CC.mapM process
  where
    process :: Message -> IO BS.ByteString
    process = \ case
      Ping _ _             -> fail "FIXME Ping"
      -- PushPull _ _ _       -> fail "FIXME PushPull"
      unexpected           -> fail $ "unexpected TCP message " <> show unexpected

handleUDPMessage :: Store -> Conduit UDP.Message IO Gossip
handleUDPMessage store =
  CC.map decodeUdp =$= handleDecodeErrors =$= CC.concat =$= CC.mapM (uncurry process) =$= CC.concat
  where
    decodeUdp :: UDP.Message -> Either Error [(SockAddr, Message)]
    decodeUdp udpMsg = map (UDP.msgSender udpMsg,) . NEL.toList . unEnvelope <$> decode (UDP.msgData udpMsg)

    handleDecodeErrors :: Conduit (Either Error a) IO a
    handleDecodeErrors = awaitForever $ either fail yield

    process :: SockAddr -> Message -> IO [Gossip]
    process sender = \ case
      -- invoke ack handler for the sequence
      Ack seqNo' _ -> do
        liftIO $ handleAck store seqNo'
        return []

      -- respond with Ack if the ping was meant for us
      Ping seqNo' node'
        | node' == memberName (storeSelf store) ->
          return [Gossip Ack {seqNo = seqNo', payload = []} sender]
        | otherwise ->
          return []

      -- send a ping to the requested target
      -- and create ack handler which relays ack from target to original requester
      IndirectPing _seqNo' target' port' node' -> do
        next <- liftIO $ nextIncarnation store
        return [ Gossip Ping { seqNo = fromIntegral next, node = node' } $
                        SockAddrInet (fromIntegral port') target' ]

      _ -> return []

-- disseminate receives messages for gossiping to other members
-- ping/indirect-ping
-- messages other than Ping/IndirectPing/Ack are enqueued for piggy-backing
-- while ping/indirect-ping/ack are immediately sent
disseminate :: Store -> Conduit Gossip IO UDP.Message
disseminate _store = awaitForever $ \(Gossip msg addr) ->
  -- send ping/indirect-ping/ack immediately and enqueue everything else
  case msg of
    Ping{..} -> gossip msg addr
    Ack{..} -> gossip msg addr
    IndirectPing{..} -> gossip msg addr
    _ -> enqueue msg addr

  where gossip msg addr =
          yield $ UDP.Message (encode msg) addr

        -- FIXME: need to add priority queue and then have send pull from that to create compound msg
        enqueue _msg _addr =
          return ()

-- TODO: need a timer to mark this node as dead after suspect timeout
-- TODO: solve the expression problem of our Message type: we want Suspect msg here
suspectOrDeadNode' :: Store -> Message -> MemberName -> Int -> Liveness -> IO (Maybe Message)
suspectOrDeadNode' _ _ _ _ IsAlive = undefined
suspectOrDeadNode' s msg name i suspectOrDead = do
  (self, membs) <- atomically $ membersAndSelf s

  case find ((== name) . memberName) membs of
    -- we don't know this node. ignore.
    Nothing -> return Nothing

    -- ignore old incarnation or failed livenessCheck
    Just m | i < memberIncarnation m || livenessCheck m -> return Nothing

    -- no cluster, we're not suspect/dead. refute it.
    Just m | name == memberName self -> do
               i' <- atomically $ do
                 nextInc <- nextIncarnation' s $ memberIncarnation m
                 let m' = m { memberIncarnation = nextInc }
                 saveMember m' >> return nextInc

               -- return so we don't mark ourselves suspect/dead
               -- FIXME: hardcoding!
               return $ Just $ Alive i' name (1 :: Word32) (1 :: Word16) []

    -- broadcast suspect/dead msg
    Just m -> do
      getCurrentTime >>= \now ->
        atomically $ do
          let m' = m { memberIncarnation = i
                   , memberAlive = suspectOrDead
                   , memberLastChange = now }
          saveMember m'

      return $ Just msg

  where
    livenessCheck m = case suspectOrDead of
      IsAlive -> undefined
      IsSuspect -> memberAlive m /= IsAlive
      IsDead -> memberAlive m == IsDead

    -- name = case msg of
    saveMember m =
      modifyTVar' (storeMembers s) $ Map.insert (memberName m) m

suspectNode :: Store -> Message -> IO (Maybe Message)
suspectNode s msg@(Suspect i name) = suspectOrDeadNode' s msg name i IsSuspect
suspectNode _ _ = undefined

deadNode :: Store -> Message -> IO (Maybe Message)
deadNode s msg@(Dead i name _) = suspectOrDeadNode' s msg name i IsDead
deadNode _ _ = undefined

type Timeout = ()
type AckWaiter = IO (Either Timeout SeqNo)

invokeAckHandler :: Store -> SeqNo -> IO ()
invokeAckHandler = undefined

pollAckHandlerFor :: Store -> SeqNo -> STM
pollAckHandlerFor store seqNo =
  readTMChan $ storeAckHandler store >>
  seqNo ack == seqNo

failureDetector :: Config -> Store -> Conduit () IO Gossip
failureDetector cfg@Config{..} store = loop
  where loop = do
          _ <- after $ seconds gossipInterval
          _members <- kRandomNodesExcludingSelf numToGossip store
          newSeqNo <- nextSeqNo store

          -- let ackWaiter = race (after gossipInterval) (atomically $ )

          -- sourceList _members $$ probeNode newSeqNo ackWaiter $=
          return loop

        probeNode :: SeqNo -> AckWaiter -> Member -> Conduit () IO Gossip
        probeNode newSeqNo ackWaiter m = do
          yield $ Ping (fromIntegral _seqNo) $ show m
          ack <- ackWaiter

          -- send IndirectPing to k random nodes if we didn't receive ack
          either (invokeAckHandler store >> return) $ do
            randomMembers <- liftIO $ kRandomNodesExcludingSelf numToGossip store
            mapM_ sendIndirectPing randomMembers

        sendIndirectPing :: SeqNo -> AckWaiter -> Member -> Conduit () IO Gossip
        sendIndirectPing newSeqNo ackWaiter m@Member{..} = do
          yield IndirectPing { seqNo = newSeqNo
                             , target = fst memberHost
                             , port = snd memberHost
                             , node = show m
                             }
          ack <- ackWaiter
          either (invokeAckHandler store >> return) suspectNode'

        -- broadcast possible suspect msg
        suspectNode' :: Member -> Conduit () IO Gossip
        suspectNode' Member{..} = do
          suspect <- liftIO $ suspectNode store $ Suspect memberIncarnation memberName
          maybe $ return () $ yield Gossip suspect memberHostNew

-- type Producer (m :: * -> *) o = forall i. ConduitM i o m ()
-- handleUDPMessage :: Store -> Conduit UDP.Message IO Gossip
failureDetector' :: Config -> Store -> Conduit () IO Gossip
failureDetector' cfg store = undefined
--  failureDetector cfg store =$= sinkTMChan gossip False

main :: IO ()
main = do
  (cfg, store, _) <- configure >>= either error return
  _ <- installHandler sigUSR1 (Catch $ dumpStore store) Nothing
  gossip <- newTMChanIO

  withSocket (bindUDP "127.0.0.1" 4000) $ \sock ->
    let tcpServer =
          runTCPServer (serverSettings 4000 "127.0.0.1") $ \client ->
            appSource client $$ handleTCPMessage store (appSockAddr client) =$= appSink client
        udpReceiver =
          UDP.sourceSocket sock 65535 $$ handleUDPMessage store =$= sinkTMChan gossip False
        disseminate' =
          sourceTMChan gossip $$ disseminate store =$= sinkToSocket sock
    in tcpServer `race_` udpReceiver `race_` disseminate' `race_` failureDetector'
