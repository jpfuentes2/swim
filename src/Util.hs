{-# LANGUAGE OverloadedStrings #-}

module Util where

import           Control.Concurrent (threadDelay)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB (word16BE, toLazyByteString)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import           Data.MessagePack.Aeson (packAeson, unpackAeson)
import           Data.Monoid ((<>))
import           Data.Time.Clock (UTCTime(..), getCurrentTime)
import           Data.Word (Word16, Word8)
import           System.Random (getStdRandom, randomR)
import           Types

toWord8 :: Int -> Word8
toWord8 n = fromIntegral n :: Word8

toWord16 :: Int -> Word16
toWord16 n = fromIntegral n :: Word16

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
seconds i = i * 1000000

after :: Int -> IO UTCTime
after = return getCurrentTime . threadDelay

-- Fisher-Yates shuffle
shuffle :: [a] -> IO [a]
shuffle [] = return []
shuffle as = do
  rand <- getStdRandom $ randomR (0, length as - 1) -- [0, n)
  let (left, a:right) = splitAt rand as
  (a:) <$> shuffle (left <> right)

parseConfig :: Either Error Config
parseConfig = Right Config { bindHost = "udp://127.0.0.1:4002"
                           , joinHost = "udp://127.0.0.1:4000"
                           , configJoinHosts = NonEmpty.fromList [ "udp://127.0.0.1:4000" ]
                           , configUDPBufferSize = 65336
                           , cfgGossipNodes = 10
                           }

withSocket :: IO Socket -> (Socket -> IO a) -> IO a
withSocket s = bracket s close
