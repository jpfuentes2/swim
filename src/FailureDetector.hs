{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TupleSections     #-}

module FailureDetector where

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race)
import           Control.Concurrent.STM (atomically)
import           Control.Concurrent.STM.TVar
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Control.Monad.Identity (void)
import           Data.Conduit
import           Data.Foldable (find)
import qualified Data.Map.Strict as Map (elems, insert)
import           Data.Time.Clock (getCurrentTime)
import           Data.Word (Word16, Word32)

import           Core
import           Types

-- TODO: need a timer to mark this node as dead after suspect timeout
-- TODO: solve the expression problem of our Message type: we want Suspect msg here
suspectOrDeadNode' :: Store -> Message -> String -> Int -> Liveness -> IO (Maybe Message)
suspectOrDeadNode' _ _ _ _ IsAlive = undefined
suspectOrDeadNode' s msg name i suspectOrDead = do
  (self, members) <- atomically $ membersAndSelf s

  case find ((== name) . memberName) members of
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
               return $ Just $ Alive i' name (1 :: Word32) (1 :: Word16) []

    -- broadcast suspect/dead msg
    Just m -> do
      now <- getCurrentTime
      _ <- atomically $ do
        let m' = m { memberIncarnation = i
                   , memberAlive = suspectOrDead
                   , memberLastChange = now }
        void $ saveMember m'

      return $ Just msg

  where
    livenessCheck m = case suspectOrDead of
      IsAlive -> undefined
      IsSuspect -> memberAlive m /= IsAlive
      IsDead -> memberAlive m == IsDead

    -- name = case msg of
    saveMember m =
      modifyTVar (storeMembers s) $ Map.insert (memberName m) m

suspectNode :: Store -> Message -> IO (Maybe Message)
suspectNode s msg@(Suspect i name) = suspectOrDeadNode' s msg name i IsSuspect
suspectNode _ _ = undefined

deadNode :: Store -> Message -> IO (Maybe Message)
deadNode s msg@(Dead i name _) = suspectOrDeadNode' s msg name i IsDead
deadNode _ _ = undefined

probeNode :: Config -> Store -> Member -> AckChan -> ConduitM AckResponse Message IO ()
probeNode cfg s m ackChan = do
  seqNo' <- liftIO $ nextSeqNo s
  yield Ping { seqNo = fromIntegral seqNo', node = show m }
  ack <- liftIO $ race (after $ seconds 5) (waitForAckOf seqNo' ackChan)

  case ack of
    -- received Ack so we're happy!
    Right _ -> return ()

    -- send IndirectPing to kRandomNodes
    Left _ -> do
      let indirectPing mem = IndirectPing { seqNo = fromIntegral seqNo', fromAddr = 1 :: Word32, node = "wat" }
      randomNodes <- liftIO $ kRandomNodesExcludingSelf cfg s
      mapM_ (yield . indirectPing) randomNodes
      ack <- liftIO $ race (after $ seconds 5) (waitForAckOf seqNo' ackChan)

      case ack of
        -- broadcast possible suspect msg
        Left _ -> do
          suspect <- liftIO $ suspectNode s $ Suspect (memberIncarnation m) (memberName m)
          maybe (return ()) (void . yield) suspect

        Right _ -> return ()

failureDetector :: Config -> Store -> IO ()
failureDetector cfg s =
    loop
  where
    d = 5
    loop = do
        _ <- threadDelay d >> getCurrentTime
        nodes <- atomically $ readTVar $ storeMembers s

        -- exclude self from list of nodes to probe
        randomNodes <- kRandomNodes (cfgGossipNodes cfg) [storeSelf s] (Map.elems nodes)

        -- TODO: plug in probeNode
        loop
