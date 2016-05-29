{-# LANGUAGE OverloadedStrings #-}

module Util where

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.STM (atomically)
import           Control.Concurrent.STM.TVar (newTVarIO, readTVar)
import           Control.Exception.Base (bracket)
import           Control.Monad.Trans (liftIO)
import           Control.Monad.Trans.Either (EitherT(..), right, hoistEither, runEitherT)
import           Data.Conduit.TMChan (newTMChanIO)
import qualified Data.List.NonEmpty as NEL (fromList)
import qualified Data.Map.Strict as Map (empty)
import           Data.Monoid ((<>))
import           Data.Streaming.Network (getSocketUDP)
import           Data.Time.Calendar
import           Data.Time.Clock (UTCTime(..), getCurrentTime)
import           Network.Socket (Socket, SockAddr(SockAddrInet), close, setSocketOption, SocketOption(ReuseAddr), bind, addrAddress)
import           System.Random (getStdRandom, randomR)
import           Types

seconds :: Int -> Microseconds
seconds = (1000000 *)

milliseconds :: Int -> Microseconds
milliseconds = (1000 *)

after :: Microseconds -> IO UTCTime
after mics = do
  threadDelay mics
  getCurrentTime

-- FIXME: O(N^2) Fisher-Yates shuffle. it's okay b/c our lists are small for now
shuffle :: [a] -> IO [a]
shuffle [] = return []
shuffle as = do
  rand <- getStdRandom $ randomR (0, length as - 1) -- [0, n)
  let (l, a:r) = splitAt rand as
  (a:) <$> shuffle (l <> r)

parseConfig :: Either Error Config
parseConfig = Right Config { bindHost = "udp://127.0.0.1:4002"
                           , joinHosts = NEL.fromList [ "udp://127.0.0.1:4000" ]
                           , udpBufferSize = 65336
                           , numToGossip = 10
                           , gossipInterval = milliseconds 200
                           }

withSocket :: IO Socket -> (Socket -> IO a) -> IO a
withSocket s = bracket s close

-- opens a socket, sets SO_REUSEADDR, and the binds it
bindUDP :: String -> Int -> IO Socket
bindUDP host p = do
  (sock, info) <- getSocketUDP host p
  -- reuse since hashicorp/memberlist seems to want us to use same port
  setSocketOption sock ReuseAddr 1
  bind sock (addrAddress info) >> return sock

-- FIXME: move to show instance
dumpStore :: Store -> IO ()
dumpStore s = do
  (_seqNo, i, ms, self) <- atomically $ do
    _seqNo <- readTVar $ storeSeqNo s
    i <- readTVar $ storeIncarnation s
    ms <- readTVar $ storeMembers s
    return (_seqNo, i, ms, storeSelf s)

  print $ "(seqNo, inc) " <> show (_seqNo, i)
  print $ "members: " <> show ms
  print $ "self: " <> show self

makeStore :: Member -> IO Store
makeStore self = do
  mems <- newTVarIO Map.empty
  -- num <- newTVarIO 0
  -- events <- newTVarIO []
  sqNo <- newTVarIO 0
  inc <- newTVarIO 0
  ackHandler <- newTMChanIO
  let store = Store { storeSeqNo = sqNo
                    , storeIncarnation = inc
                    , storeMembers = mems
                    , storeSelf = self
                    , storeAckHandler = ackHandler
                    -- , storeNumMembers = num
                    }
  return store

makeSelf :: Config -> Member
makeSelf _ =
  Member { memberName = "myself"
         , memberHost = "localhost"
         , memberHostNew = SockAddrInet 123 4000
         , memberAlive = IsAlive
         , memberIncarnation = 0
         , memberLastChange = UTCTime (ModifiedJulianDay 0) 0
         }

configure :: IO (Either Error (Config, Store, Member))
configure = runEitherT $ do
  cfg <- hoistEither parseConfig
  self <- right (makeSelf cfg)
  store <- liftIO $ makeStore self
  return (cfg, store, self)
