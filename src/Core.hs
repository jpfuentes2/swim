{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types        #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Core where

import           Control.Concurrent (forkIO)
import           Control.Concurrent.Async (race, race_)
import           Control.Concurrent.STM (STM, atomically)
import           Control.Concurrent.STM.TVar (TVar, readTVar, writeTVar, modifyTVar')
import           Control.Monad (forever)
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Control.Monad.Identity (unless, void)
import           Control.Monad.Trans.Class (lift)
import           Data.Conduit (Conduit, Source, awaitForever, yield, ($$), (=$=))
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.Network.UDP as UDP
import           Data.Conduit.TMChan (newTMChanIO, sinkTMChan, sourceTMChan, writeTMChan)
import           Data.Foldable (find)
import qualified Data.List.NonEmpty as NEL
import qualified Data.Map.Strict as Map
import           Data.Serialize (decode, encode)
import           Data.Time.Clock (UTCTime (..), getCurrentTime)
import qualified Network.Socket as NS
import           System.Posix.Signals (Handler (Catch), installHandler, sigUSR1)

import           Types
import           Util

isAlive :: Member -> Bool
isAlive = (== IsAliveC) . memberAlive

isDead :: Member -> Bool
isDead = (== IsDeadC) . memberAlive

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
nextIncarnation' store@Store{..} n = do
  inc <- readTVar storeIncarnation
  let inc' = succ inc
  if n >= inc' then
     nextIncarnation' store n
  else
     writeTVar storeIncarnation inc' >> return inc'

removeDeadNodes :: Store -> STM ()
removeDeadNodes Store{..} =
  modifyTVar' storeMembers $ Map.filter (not . isDead)

kRandomMembers :: Store -> Int -> [Member] -> IO [Member]
kRandomMembers store n excludes = do
  ms <- atomically $ members store
  take n <$> shuffle (filter f ms)
  where
    f m = notElem m excludes && isAlive m

members :: Store -> STM [Member]
members Store{..} = Map.elems <$> readTVar storeMembers

handleUDPMessage :: Store -> Conduit UDP.Message IO Gossip
handleUDPMessage store@Store{..} =
  CC.map decodeUdp =$= handleDecodeErrors =$= CC.concat =$= CC.mapM (uncurry process) =$= CC.concat
  where
    decodeUdp :: UDP.Message -> Either Error [(NS.SockAddr, Message)]
    decodeUdp udpMsg = map (UDP.msgSender udpMsg,) . NEL.toList . unEnvelope <$> decode (UDP.msgData udpMsg)

    handleDecodeErrors :: Conduit (Either Error a) IO a
    handleDecodeErrors = awaitForever $ either fail yield

    process :: NS.SockAddr -> Message -> IO [Gossip]
    process sender msg = case msg of
      -- invoke ack handler for the sequence
      Ack seqNo' _ -> do
        _ <- liftIO $ getCurrentTime >>= (\t -> atomically $ invokeAckHandler store (seqNo',t))
        return []

      -- respond with Ack if the ping was meant for us
      Ping seqNo' node'
        | node' == memberName storeSelf ->
          return [Direct Ack {seqNo = seqNo', payload = []} sender]
        | otherwise ->
          return []

      -- send a ping to the requested target
      -- and create ack handler which relays ack from target to original requester
      IndirectPing _seqNo' target' port' node' ->
        nextIncarnation store >>= \next ->
          return [ Direct Ping { seqNo = fromIntegral next, node = node' } $
                          NS.SockAddrInet (fromIntegral port') target' ]

      Suspect{..} ->
        maybeBroadcast $ suspectNode store msg

      Dead{..} ->
        maybeBroadcast $ deadNode store msg

      Alive{..} ->
        maybeBroadcast $ aliveNode store msg

    maybeBroadcast :: IO (Maybe Message) -> IO [Gossip]
    maybeBroadcast msg =
      maybe [] (\m -> [Broadcast m]) <$> msg

-- disseminate receives messages for gossiping to other members
-- ping/indirect-ping
-- messages other than Ping/IndirectPing/Ack are enqueued for piggy-backing
-- while ping/indirect-ping/ack are immediately sent
disseminate :: Store -> Conduit Gossip IO UDP.Message
disseminate _store = awaitForever $ \ case
  -- send ping/indirect-ping/ack immediately and enqueue everything else
  Direct msg addr -> gossip msg addr
  Broadcast msg -> enqueue msg

  where gossip msg addr =
          yield $ UDP.Message (encode msg) addr

        -- FIXME: add priority queue and then have send pull from that to create compound msg
        enqueue _msg =
          return ()


-- FIXME: need a timer to mark this node as dead after suspect timeout
suspectOrDeadNode' :: NotAlive n => Store -> Message -> MemberName -> Int -> Liveness' n -> IO (Maybe Message)
suspectOrDeadNode' s@Store{..} msg name i suspectOrDead = do
  ms <- atomically $ members s
  case find ((== name) . memberName) ms of
    -- we don't know this node. ignore.
    Nothing ->
      return Nothing

    -- ignore old incarnation or failed livenessCheck
    Just m | i < memberIncarnation m || livenessCheck m ->
      return Nothing

    -- no cluster, we're not suspect/dead. refute it.
    Just m | name == memberName storeSelf -> do
      i' <- atomically $ do
        nextInc <- nextIncarnation' s $ memberIncarnation m
        let m' = m { memberIncarnation = nextInc }
        saveMember m' >> return nextInc

      -- return so we don't mark ourselves suspect/dead
      let (NS.SockAddrInet port host) = memberHostNew storeSelf
      return $ Just Alive { incarnation = i'
                          , node = name
                          , addr = host
                          , port = fromIntegral port }

    -- broadcast suspect/dead msg
    Just m -> do
      getCurrentTime >>= \now ->
        atomically $ do
          let m' = m { memberIncarnation = i
                     , memberAlive = case suspectOrDead of
                         IsDead -> IsDeadC
                         IsSuspect -> IsSuspectC
                     , memberLastChange = now }
          saveMember m'

      return $ Just msg

  where
    livenessCheck Member{..} = case suspectOrDead of
      IsSuspect -> memberAlive /= IsAliveC
      IsDead -> memberAlive == IsDeadC

    saveMember m@Member{..} =
      modifyTVar' storeMembers $ Map.insert memberName m

suspectNode :: Store -> Message -> IO (Maybe Message)
suspectNode s msg@(Suspect i name) = suspectOrDeadNode' s msg name i IsSuspect
suspectNode _ _ = undefined

deadNode :: Store -> Message -> IO (Maybe Message)
deadNode s msg@(Dead i name _) = suspectOrDeadNode' s msg name i IsDead
deadNode _ _ = undefined

aliveNode :: Store -> Message -> IO (Maybe Message)
aliveNode store@Store{..} msg@(Alive _ node' _ _) = do
  ms <- atomically $ members store
  now <- getCurrentTime
  _ <- maybe (addNewMember now msg) pure $ find ((== node') . memberName) ms
  -- FIXME: READ THE PAPER
  void $ fail "READ THE PAPER"
  return Nothing

  where addNewMember :: UTCTime -> Message -> IO Member
        addNewMember now (Alive i' _ addr' port') = do
          let member = Member { memberName = node'
                              , memberHost = ""
                              , memberHostNew = NS.SockAddrInet (fromIntegral port') addr'
                              , memberAlive = IsAliveC
                              , memberIncarnation = i'
                              , memberLastChange = now
                              }
          atomically $ modifyTVar' storeMembers $ Map.insert node' member
          pure member

aliveNode _ _ = undefined

invokeAckHandler :: Store -> (SeqNo, UTCTime) -> STM ()
invokeAckHandler Store{..} = writeTMChan storeAckHandler

waitForAckOf :: Store -> SeqNo -> IO ()
waitForAckOf Store{..} _seqNo =
  sourceTMChan storeAckHandler $$ ackOf _seqNo =$= CC.sinkNull >> return ()
  where
    ackOf s = awaitForever $ \(ackSeqNo, _) ->
      unless (s == ackSeqNo) $ ackOf s

-- failureDetector

-- FIXME: move from random to robust scheme
failureDetector :: Store -> Source IO Gossip
failureDetector store@Store{..} = do
  gossip <- liftIO newTMChanIO
  void $ liftIO $ forkIO $ forever $ do
    void $ after $ milliseconds (gossipInterval storeCfg)
    currSeqNo <- fromIntegral <$> nextSeqNo store
    ms <- kRandomMembers store (numToGossip storeCfg) []
    mapM_ (\m -> probeNode' store currSeqNo m $$ sinkTMChan gossip False) ms
  sourceTMChan gossip

probeNode' :: Store -> SeqNo -> Member -> Source IO Gossip
probeNode' store@Store{..} currSeqNo m = do
  -- ping this node via direct gossip
  yield $ Direct (Ping currSeqNo $ memberName m) (memberHostNew m)
  unlessAck $ do
    -- indirectly ping this node using N proxies via direct gossip
    ms <- liftIO $ kRandomMembers store (numToGossip storeCfg) []
    CC.yieldMany $ map (Direct indirectPing . memberHostNew) ms
    unlessAck $ do
      -- run suspicion
      suspect <- lift $ suspectNode store $ Suspect (memberIncarnation m) (memberName m)
      maybe (return ()) (yield . Broadcast) suspect

  where unlessAck :: Source IO Gossip -> Source IO Gossip
        unlessAck f = do
          wait <- liftIO $ race (timeout $ milliseconds $ gossipInterval storeCfg)
                                (waitForAckOf store currSeqNo)
          either (const $ return ()) (const f) wait

        indirectPing :: Message
        indirectPing =
          let NS.SockAddrInet port host = memberHostNew m
          in  IndirectPing { seqNo = currSeqNo
                           , target = host
                           , port = fromIntegral port
                           , node = show m
                           }
-- failureDetector

main :: IO ()
main = do
  store <- configure >>= either error return
  _ <- installHandler sigUSR1 (Catch $ dumpStore store) Nothing
  let gossip = storeGossip store

  withSocket (bindUDP "127.0.0.1" 4000) $ \sock ->
    let udpReceiver =
          UDP.sourceSocket sock 65535 $$ handleUDPMessage store =$= sinkTMChan gossip False

        failureDetector' =
          failureDetector store $$ sinkTMChan gossip False

        disseminate' =
          sourceTMChan gossip $$ disseminate store =$= UDP.sinkToSocket sock
    in udpReceiver `race_` disseminate' `race_` failureDetector'
