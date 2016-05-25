{-# LANGUAGE OverloadedStrings #-}

module Util where

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.STM (atomically)
import           Control.Concurrent.STM.TVar (newTVarIO, readTVar)
import           Control.Exception.Base (bracket)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB (word16BE, toLazyByteString)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import           Data.Conduit.TMChan (newTMChanIO)
import qualified Data.List.NonEmpty as NEL (fromList)
import qualified Data.Map.Strict as Map (empty)
import           Data.MessagePack.Aeson (packAeson, unpackAeson)
import           Data.Monoid ((<>))
import           Data.Serialize.Put (runPut, putWord8, putWord16be, putByteString)
import           Data.Streaming.Network (getSocketUDP)
import           Data.Time.Clock (UTCTime(..), getCurrentTime)
import           Data.Word (Word16, Word32, Word8)
import           Network.Socket (Socket, close, setSocketOption, SocketOption(ReuseAddr), bind, addrAddress)
import           System.Random (getStdRandom, randomR)
import           Types

type Microseconds = Int

seconds :: Int -> Microseconds
seconds = (1000000 *)

after :: Microseconds -> IO UTCTime
after mics = do
  threadDelay mics
  getCurrentTime

-- O(N^2) Fisher-Yates shuffle. it's okay our lists are small for now
shuffle :: [a] -> IO [a]
shuffle [] = return []
shuffle as = do
  rand <- getStdRandom $ randomR (0, length as - 1) -- [0, n)
  let (left, a:right) = splitAt rand as
  (a:) <$> shuffle (left <> right)

parseConfig :: Either Error Config
parseConfig = Right Config { bindHost = "udp://127.0.0.1:4002"
                           , joinHost = "udp://127.0.0.1:4000"
                           , configJoinHosts = NEL.fromList [ "udp://127.0.0.1:4000" ]
                           , configUDPBufferSize = 65336
                           , cfgGossipNodes = 10
                           }

withSocket :: IO Socket -> (Socket -> IO a) -> IO a
withSocket s = bracket s close

-- opens a socket, sets SO_REUSEADDR, and the binds it
bindUDP :: String -> Int -> IO Socket
bindUDP host port = do
  (sock, info) <- getSocketUDP host port
  -- reuse since hashicorp/memberlist seems to want us to use same port
  setSocketOption sock ReuseAddr 1
  bind sock (addrAddress info) >> return sock

dumpStore :: Store -> IO ()
dumpStore s = do
  (_seqNo, inc, membs, self) <- atomically $ do
    _seqNo <- readTVar $ storeSeqNo s
    inc <- readTVar $ storeIncarnation s
    membs <- readTVar $ storeMembers s
    return (seqNo, inc, membs, storeSelf s)

  --print $ "(seqNo, inc) " <> show (_seqNo, inc)
  print $ "members: " <> show membs
  print $ "self: " <> show self

decodeMsgType :: BS.ByteString -> Either Error (MsgType, BS.ByteString)
decodeMsgType =
  runGet $ do
    typ <- fromIntegral <$> getWord8
    left <- remaining
    return (toEnum typ, getByteString left)

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
