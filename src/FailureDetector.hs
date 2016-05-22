{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module FailureDetector where

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race)
import           Control.Concurrent.STM (atomically)
import           Control.Concurrent.STM.TVar
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Data.Conduit
import           Data.Foldable (find)
import qualified Data.Map.Strict as Map (elems, insert)
import           Data.Time.Clock (getCurrentTime)
import           Data.Word (Word16, Word32)

import           Core
import           Types
import           Util

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
        saveMember m'

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

type Timeout = ()
type SeqNo = Word32
type AckWaiter = IO (Either Timeout SeqNo)

-- TODO: time to put self member into Store rather than Config
probeNode :: Config ->
             Store ->
             Member ->
             SeqNo ->
             AckWaiter ->
             ConduitM AckResponse Message IO ()
probeNode cfg s m seqNo' ackWaiter = do
  yield Ping { seqNo = fromIntegral seqNo', node = show m }
  ack <- liftIO ackWaiter

  case ack of
    Right _ -> return ()

    -- send IndirectPing to kRandomNodes
    Left _ -> do
      let indirectPing mem = IndirectPing { seqNo = fromIntegral seqNo', fromAddr = 1 :: Word32, node = "wat" }
      randomNodes <- liftIO $ kRandomNodesExcludingSelf cfg s
      mapM_ (yield . indirectPing) randomNodes
      ack <- liftIO ackWaiter

      case ack of
        Right _ -> return ()

        -- broadcast possible suspect msg
        Left _ -> do
          suspect <- liftIO $ suspectNode s $ Suspect (memberIncarnation m) (memberName m)
          maybe (return ()) yield suspect

failureDetector :: Config -> Store -> IO ()
failureDetector cfg s =
    loop
  where
    loop = do
        _ <- after $ seconds 5
        members <- kRandomNodesExcludingSelf cfg s

        -- mapM_ (\node -> do
          -- make a seqNo'
          -- make a chan for seqNo'
          -- let ackWaiter = race (after $ seconds 5) $ waitForAckOf chan
        --                 chan <- 
        --                 probeNode cfg s node  randomNodes

        -- TODO: plug in probeNode
        loop
