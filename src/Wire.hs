{-# LANGUAGE OverloadedStrings #-}

module Wire where

import qualified Data.ByteString             as BS (ByteString, null, append, concat, drop, empty,
                                                    take, pack, unpack, length, uncons)
import qualified Data.ByteString.Builder as BSB (word16BE, toLazyByteString)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import           Data.MessagePack.Aeson (packAeson, unpackAeson)
import           Data.Monoid ((<>))
import           Data.Word (Word16, Word32, Word8)
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
