{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards #-}

module Types where

import           Control.Concurrent.STM.TVar
import           Data.Aeson (FromJSON, ToJSON)
import           Data.Aeson.Types
import           Data.Atomics.Counter ( AtomicCounter )
import           Data.ByteString ( ByteString )
import qualified Data.Conduit.Network.UDP    as UDP ( Message(..) )
import           Data.Foldable (asum)
import           Data.List.NonEmpty ( NonEmpty(..) )
import qualified Data.Map.Strict as Map
import           Data.Monoid                 ( (<>) )
import           Data.Time.Clock ( UTCTime(..) )
import           Data.Word ( Word16, Word32, Word8 )
import           GHC.Generics
import           Network.Socket ( HostAddress )

type Error = String

-- replace with SockAddr
type Host = String

-- type Client = Client { addr :: HostAddress }

data Config = Config { bindHost :: String
                     , joinHost :: String
                     , configJoinHosts :: NonEmpty String
                     , configUDPBufferSize :: Int
                     }

-- Member
data Member = Member { memberName :: String
                     , memberHost :: Host
                     , memberAlive :: Liveness
                     -- , memberMeta :: ByteString
                     } deriving (Show, Eq)

data EventHost = To String | From String deriving (Show)

data Event = Event { eventHost :: EventHost
                   , eventMsg :: Maybe Message
                   , eventBody :: ByteString
                   } deriving (Show)

data Store = Store { storeSeqNo :: AtomicCounter
                   , storeIncarnation :: AtomicCounter
                   -- , storeNumMembers :: TVar Int -- estimate, aka known unknown, of members
                   , storeMembers :: TVar (Map.Map String Member) -- known known of members
                   , storeEvents :: TVar [Event] -- event log
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
             | Failed { remoteAddr :: String
                      , error        :: Error
                      , message        :: String
                    }
    deriving (Eq, Show)

instance FromJSON Message where
  parseJSON = withObject "message" $ \o -> asum [
    Ping <$> o .: "SeqNo" <*> o .: "Node",
    IndirectPing <$> o .: "SeqNo" <*> o .: "FromAddr" <*> o .: "Node",
    Ack <$> o .: "SeqNo" <*> o .: "Payload",
    Suspect <$> o .: "Incarnation" <*> o .: "Node",
    Alive <$> o .: "Incarnation" <*> o .: "Node" <*> o .: "FromAddr" <*> o .: "Port" <*> o .: "Version",
    Dead <$> o .: "Incarnation" <*> o .: "Node" <*> o .: "DeadFrom",
    Failed <$> o .: "RemoteAddr" <*> o .: "Error" <*> o .: "Message" ]

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

  toJSON Failed{..} = object [
    "RemoteAddr" .= remoteAddr,
    "Error"  .= error,
    "Message"  .= message ]

instance Show UDP.Message where
    show (UDP.Message msgData msgSender) =
        "got msg: " <> show msgData <> " from: " <> show msgSender
