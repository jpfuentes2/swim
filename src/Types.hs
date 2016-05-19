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
import qualified Data.Map.Strict as Map
import           Data.Monoid ( (<>) )
import           Data.Time.Clock ( UTCTime(..) )
import           Data.Word (Word16, Word32, Word8)

type Error = String

-- replace with SockAddr
type Host = String

data Config = Config { bindHost :: String
                     , joinHost :: String
                     , configJoinHosts :: NonEmpty String
                     , configUDPBufferSize :: Int
                     , cfgGossipNodes :: Int
                     }

-- Member
data Member = Member { memberName        :: String
                     , memberHost        :: Host
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
                   }

-- The state of a Member
data Liveness = IsAlive | IsSuspect | IsDead
    deriving (Eq, Show, Read)

-- Messages our server understands
data Message = Ping { seqNo :: Word32
                    , node  :: String
                    }
             | IndirectPing { seqNo :: Word32
                            , fromAddr  :: Word32
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
             | Compound ByteString
    deriving (Eq, Show)

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
             deriving (Eq, Show, Enum)

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
    IndirectPing <$> o .: "SeqNo" <*> o .: "FromAddr" <*> o .: "Node",
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
    "FromAddr"  .= fromAddr,
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
