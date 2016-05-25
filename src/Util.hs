{-# LANGUAGE OverloadedStrings #-}

module Util where

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.STM (atomically)
import           Control.Concurrent.STM.TVar (newTVarIO, readTVar)
import           Control.Exception.Base (bracket)
import           Control.Monad.Trans (liftIO, lift)
import           Control.Monad.Trans.Either (EitherT(..), left, right, hoistEither, runEitherT)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB (word16BE, toLazyByteString)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import           Data.Conduit.TMChan (newTMChanIO)
import qualified Data.List.NonEmpty as NEL (fromList)
import qualified Data.Map.Strict as Map (empty)
import           Data.MessagePack.Aeson (packAeson, unpackAeson)
import           Data.Monoid ((<>))
import           Data.Streaming.Network (getSocketUDP)
import           Data.Time.Calendar
import           Data.Time.Clock (UTCTime(..), getCurrentTime)
import           Data.Word (Word16, Word32, Word8)
import           Network.Socket (Socket, close, setSocketOption, SocketOption(ReuseAddr), bind, addrAddress)
import           Network.Socket.Internal (HostAddress, SockAddr (SockAddrInet))
import           System.Random (getStdRandom, randomR)

import           Types

toWord8 :: Int -> Word8
toWord8 n = fromIntegral n :: Word8

toWord16 :: Int -> Word16
toWord16 n = fromIntegral n :: Word16

toWord32 :: Int -> Word32
toWord32 n = fromIntegral n :: Word32

encode :: Message -> BS.ByteString
encode = toStrict . packAeson

-- TODO: reduce list traversal please
-- | msg type | num msgs | len of each msg |
-- |---------------------------------------|
-- |                body                   |
encodeCompound :: [Message] -> BS.ByteString
encodeCompound msgs =
  let (msgIdx, numMsgs) = (toWord8 $ fromEnum CompoundMsg, toWord8 $ length msgs)
      encoded = map encode msgs
      msgLengths = foldl (\b msg -> b <> BSB.word16BE (toWord16 $ BS.length msg)) mempty encoded
      header = BS.pack [msgIdx, numMsgs] <> toStrict (BSB.toLazyByteString msgLengths)
  in BS.append header $ BS.concat encoded

decode :: BS.ByteString -> Either Error Message
decode bs = maybe err Right (unpackAeson $ fromStrict bs)
  where err = Left $ "Could not parse " <> show bs

decodeCompound :: BS.ByteString -> Either Error [Message]
decodeCompound bs = do
  (numMsgs, rest) <- maybe (Left "missing compound length byte") Right $ BS.uncons bs
  _ <- if BS.length rest < fromIntegral numMsgs * 2 then
         Left "compound message is truncated"
       else
         Right ()
  -- let lengths =

  Right []

type Second = Int

seconds :: Int -> Second
seconds = (1000000 *)

after :: Second -> IO UTCTime
after = return getCurrentTime . threadDelay

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

decodeMsgType :: BS.ByteString -> Either Error (BS.ByteString, MsgType)
decodeMsgType bs = do
  n <- if BS.null bs then Left "empty msg" else Right $ BS.head bs
  -- FIXME: make me safe
  Right (BS.drop 1 bs, toEnum (fromIntegral n :: Int))

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

-- FIXME: delete me once we've solved expression problem
toGossip :: (Message, SockAddr) -> Gossip
toGossip (msg, addr) = Gossip msg addr
