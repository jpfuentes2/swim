{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards #-}

module Types where

import           Control.Concurrent.STM.TVar
import           Data.Aeson.Types
import           Data.ByteString ( ByteString )
import qualified Data.Conduit.Network.UDP as UDP ( Message(..) )
import           Data.Conduit.TMChan (TMChan(..))
import           Data.Foldable (asum)
import           Data.List.NonEmpty ( NonEmpty(..) )
import qualified Data.List.NonEmpty as NEL
import qualified Data.Map.Strict as Map
import           Data.Monoid ( (<>) )
import           Data.Serialize (Serialize, encode, putWord8, putWord16be, putByteString)
import           Data.Time.Clock ( UTCTime(..) )
import           Data.Traversable (mapM)
import           Data.Word (Word16, Word32, Word8)
import           Network.Socket.Internal (SockAddr)

type Error = String

-- replace with SockAddr
type Host = String

data Config = Config { bindHost :: String
                     , joinHost :: String
                     , configJoinHosts :: NonEmpty String
                     , configUDPBufferSize :: Int
                     , cfgGossipNodes :: Int
                     } deriving (Show, Eq)

-- Member
data Member = Member { memberName        :: String
                     , memberHost        :: Host
                     , memberHostNew     :: SockAddr
                     , memberAlive       :: Liveness
                     , memberIncarnation :: Int
                     , memberLastChange  :: UTCTime
                     -- , memberMeta :: ByteString
                     }
    deriving (Show, Eq)

instance Ord Member where
  compare a b = compare (memberName a) (memberName b)

data EventHost = To String | From String deriving (Show, Eq)

data Store = Store { storeSeqNo :: TVar Int
                   , storeIncarnation :: TVar Int
                   -- , storeNumMembers :: TVar Int -- estimate, aka known unknown, of members
                   , storeMembers :: TVar (Map.Map String Member) -- known known of members
                   , storeSelf :: Member
                   , storeAckHandler :: AckHandler
                   -- , storeHandlers :: TVar( Map.Map Word32 )
                   }

-- The state of a Member
data Liveness = IsAlive | IsSuspect | IsDead
    deriving (Eq, Show, Read)

-- |Wrapper of a series of 'Message's which are transmitted together.
-- If a single message, then encoded alone, otherwise encoded as a compound message
newtype Envelope = Envelope (NonEmpty Message)

-- FIXME? this protocol is totally weird - includes a message type which is ignored if it's
-- not the compound message type

instance Serialize Envelope where
  put (Envelope (msg :| [])) = putWord8 (fromIntegral . fromEnum . typeOf $ msg) >> put msg
  put (Envelope (NEL.toList -> msgs)) = do
    putWord8 . fromIntegral . fromEnum $ CompoundMsg
    putWord8 . fromIntegral . length $ msgs
    let encodedMsgs = map encode msgs
    mapM_ (putWord16be . fromIntegral . BS.length) encodedMsgs
    mapM_ putByteString msgs

  get = do
    typ <- fromIntegral <$> getWord8
    -- FIXME? could use safe's toEnumMay or similar from errors
    unless (typ >= 0 && typ < fromEnum maxBound) $
      fail $ "invalid message type " <> show typ
    case toEnum typ of
      CompoundMsg -> do
        numMsgs <- fromIntegral <$> getWord8
        unlessM ((>= (numMsgs * 2)) . remaining) $
          fail "compound message is truncated"
          NEL.nonEmpty . map fromIntegral <$> replicateM numMsgs getWord16be >>= \ case
            Just lengths -> Envelope <$> mapM (`isolate` get) lengths
            Nothing -> fail "compound mesage with zero messages"
      _ -> Envelope . (:| []) <$> get

-- Messages our server understands
data Message = Ping { seqNo :: Word32
                    , node  :: String
                    }
             | IndirectPing { seqNo :: Word32
                            , target  :: Word32
                            , port  :: Word16
                            , node  :: String
                            }
             | Ack { seqNo   :: Word32
                   , payload :: [Word8]
                   }
             | Suspect { incarnation :: Int
                       , node        :: String
                       -- , from        :: String
                       }
             | Alive { incarnation :: Int
                     , node        :: String
                     , fromAddr        :: Word32
                     , port        :: Word16
                     , version         :: [Word8]
                     }
             | Dead { incarnation :: Int
                    , node        :: String
                    , deadFrom        :: String
                    }
             | PushPull { incarnation :: Int
                    , node        :: String
                    , deadFrom        :: String
                    }
    deriving (Eq, Show)

instance Serialize Message where
  put = putLazyByteString . packAeson
  get = do
    lbs <- getLazyByteString =<< remaining
    maybe (fail . ("Could not parse " <>) . show) return . unpackAeson $ lbs

data InternalMessage = Gossip [Member] | Nada

newtype AckResponse = AckResponse Word32

data AckChan = AckChan (TMChan Word32) Word32

type AckHandler = TMChan Word32

data MsgType = PingMsg
             | IndirectPingMsg
             | AckMsg
             | SuspectMsg
             | AliveMsg
             | DeadMsg
             | PushPullMsg
             | CompoundMsg
             deriving (Bounded, Eq, Show, Enum)

msgIndex :: Num a => Message -> a
msgIndex m = case m of
  Ping{..} -> fromIntegral $ fromEnum PingMsg
  IndirectPing{..} -> fromIntegral $ fromEnum IndirectPingMsg
  Ack{..} -> fromIntegral $ fromEnum AckMsg
  Suspect{..} -> fromIntegral $ fromEnum SuspectMsg
  Alive{..} -> fromIntegral $ fromEnum AliveMsg
  Dead{..} -> fromIntegral $ fromEnum DeadMsg
  PushPull{..} -> fromIntegral $ fromEnum PushPullMsg
  Compound _ -> fromIntegral $ fromEnum CompoundMsg

instance FromJSON Message where
  parseJSON = withObject "message" $ \o -> asum [
    Ping <$> o .: "SeqNo" <*> o .: "Node",
    IndirectPing <$> o .: "SeqNo" <*> o .: "Target" <*> o .: "Port" <*> o .: "Node",
    Ack <$> o .: "SeqNo" <*> o .: "Payload",
    Suspect <$> o .: "Incarnation" <*> o .: "Node",
    Alive <$> o .: "Incarnation" <*> o .: "Node" <*> o .: "FromAddr" <*> o .: "Port" <*> o .: "Version",
    Dead <$> o .: "Incarnation" <*> o .: "Node" <*> o .: "DeadFrom"]

instance ToJSON Message where
  toJSON Ping{..} = object [
    "SeqNo" .= seqNo,
    "Node"  .= node ]

  toJSON IndirectPing{..} = object [
    "SeqNo" .= seqNo,
    "Target" .= target,
    "Port"  .= port,
    "Node"  .= node ]

  toJSON Ack{..} = object [
    "SeqNo" .= seqNo,
    "Payload"  .= payload ]

  toJSON Suspect{..} = object [
    "Incarnation" .= incarnation,
    "Node"  .= node ]

  toJSON Alive{..} = object [
    "Incarnation" .= incarnation,
    "Node"  .= node,
    "FromAddr"  .= fromAddr,
    "Port"  .= port ]

  toJSON Dead{..} = object [
    "Incarnation" .= incarnation,
    "Node"  .= node,
    "DeadFrom"  .= deadFrom ]

instance Show UDP.Message where
  show (UDP.Message msgData msgSender) =
    "got msg: " <> show msgData <> " from: " <> show msgSender
