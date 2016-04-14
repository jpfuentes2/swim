{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TupleSections     #-}

module Core where

import qualified Data.Aeson as Aeson (encode)
import           Data.MessagePack.Aeson (packAeson, unpackAeson)

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race)
import           Control.Concurrent.STM (STM(..), atomically)
import           Control.Concurrent.STM.TVar
import           Control.Exception.Base (bracket)
import           Control.Monad.Identity
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Control.Monad.Trans.Class (lift)
import qualified Data.ByteString             as BS (ByteString, drop, empty,
                                                    take, unpack)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import           Data.Conduit
import qualified Data.Conduit.Combinators as CC (map, omapE, print, stdout, sinkNull)
import           Data.Conduit.Network (sinkSocket)
import           Data.Conduit.TMChan (TMChan(..), sourceTMChan)
import qualified Data.Conduit.Network.UDP    as UDP (Message (..), msgSender,
                                                     sinkToSocket, sourceSocket)
import           Data.Maybe (fromMaybe)
import           Data.Either (Either (..))
import           Data.Either.Combinators (mapLeft)
import           Data.Foldable (find)
import qualified Data.List.NonEmpty as NonEmpty (fromList)
import qualified Data.Map.Strict             as Map (elems, empty, filter,
                                                     lookup, insert)
import           Data.Word ( Word16, Word32, Word8 )
import           Data.Monoid ((<>))
import           Data.Streaming.Network (getSocketUDP)
import           Data.Time.Clock (UTCTime (..), getCurrentTime)
import           Network.Socket as NS
import           Network.Socket.Internal     (HostAddress,
                                              SockAddr (SockAddrInet))
import           System.Posix.Signals        (Handler (Catch), installHandler,
                                              sigUSR1)
import           System.Random (getStdRandom, randomR)
import           Types

type Second = Int

seconds :: Int -> Second
seconds i = i * 1000000

encode :: Message -> BS.ByteString
encode = toStrict . packAeson

isAlive :: Member -> Bool
isAlive = (== IsAlive) . memberAlive

isDead :: Member -> Bool
isDead = (== IsDead) . memberAlive

notAlive :: Member -> Bool
notAlive = not . isAlive

decode :: BS.ByteString -> Either Error Message
decode bs = maybe (Left $ "Could not parse " <> show bs) Right unpacked
  where
    unpacked = unpackAeson $ fromStrict bs

parseConfig :: Either Error Config
parseConfig = Right Config { bindHost = "udp://127.0.0.1:4002"
                           , joinHost = "udp://127.0.0.1:4000"
                           , configJoinHosts = NonEmpty.fromList [ "udp://127.0.0.1:4000" ]
                           , configUDPBufferSize = 65336
                           , cfgGossipNodes = 10
                           }

atomicIncr :: Num a => TVar a -> IO a
atomicIncr tvar = atomically incr
  where incr = do
          a' <- readTVar tvar
          let a'' = a' + 1
          writeTVar tvar a'' >> return a''

nextSeqNo :: Store -> IO Int
nextSeqNo = atomicIncr . storeSeqNo

nextIncarnation :: Store -> IO Int
nextIncarnation = atomicIncr . storeIncarnation

-- ensures nextIncarnation is >= Int
nextIncarnation' :: Store -> Int -> STM Int
nextIncarnation' s n = do
  inc <- readTVar (storeIncarnation s)
  let inc' = succ inc
  if n >= inc' then
     nextIncarnation' s n
  else
     writeTVar (storeIncarnation s) inc' >> return inc'

aliveMembers :: [Member] -> [Member]
aliveMembers = filter ((== IsAlive) . memberAlive)

removeDeadNodes :: Store -> IO ()
removeDeadNodes s =
  atomically $ void $ modifyTVar (storeMembers s) $ Map.filter ((/= IsDead) . memberAlive)

-- gives kRandomNodes excluding our own host from the list
kRandomNodesExcludingSelf :: Config -> Store -> IO [Member]
kRandomNodesExcludingSelf cfg s = do
  nodes <- atomically $ readTVar $ storeMembers s
  kRandomNodes (cfgGossipNodes cfg) [] (Map.elems nodes)

-- select up to k random nodes, excluding a given
-- node and any non-alive nodes. It is possible that less than k nodes are returned.
kRandomNodes :: Int -> [Member] -> [Member] -> IO [Member]
kRandomNodes n excludes ms = take n <$> shuffle (filter f ms)
  where
    f m = notElem m excludes && IsAlive == memberAlive m

shuffle :: [a] -> IO [a]
shuffle [] = return []
shuffle xs = do
    randomPosition <- getStdRandom (randomR (0, length xs - 1))
    let (left, a : right) = splitAt randomPosition xs
    fmap (a :) (shuffle (left ++ right))

after :: Int -> IO UTCTime
after d = threadDelay d >> getCurrentTime

fromMsg :: UDP.Message -> Event
fromMsg raw = Event { eventHost = To $ show $ UDP.msgSender raw
                    , eventMsg = either (const Nothing) Just $ decode $ UDP.msgData raw
                    , eventBody = UDP.msgData raw
                    }

toMsg :: Event -> NS.SockAddr -> UDP.Message
toMsg e addr = UDP.Message { UDP.msgData = eventBody e, UDP.msgSender = addr }

handleMessage :: Store -> Conduit UDP.Message IO UDP.Message
handleMessage store = awaitForever $ \r ->
    let (msgData, msgSender) = (UDP.msgData r, UDP.msgSender r)
        -- n = BS.unpack $ BS.take 1 msgData
        raw = BS.drop 1 msgData
        inc = Event { eventHost = From $ show msgSender
                    , eventMsg = either (const Nothing) Just $ decode raw
                    , eventBody = raw
                    }
        msg = eventMsg inc
    in
        case msg of
            Just (Ack seq payload) -> respond inc Nothing
            Just (Alive incarnation n addr port vsn) -> respond inc Nothing

            Just (Ping seqNo node) -> do
                let ack = Ack { seqNo = seqNo, payload = [] }
                    out = Event { eventHost = From $ show $ UDP.msgSender r
                                , eventMsg = Just ack
                                , eventBody = encode ack
                                }
                    udpMsg = toMsg out $ UDP.msgSender r

                respond inc $ Just (out, udpMsg)

            -- Just (Dead incarnation node from) -> do
            --   found <- atomically $ do
            --     mems <- readTVar $ storeMembers store

            -- failed to parse
            Nothing -> respond inc Nothing

            -- not implemented
            _ -> respond inc Nothing
  where
    -- record the in/out events and send a response
    events = storeEvents store
    recordEvents es = liftIO . atomically $ modifyTVar events (es <>)

    respond inc (Just (out, udp)) = recordEvents [ inc, out ] >> yield udp
    respond inc Nothing = void $ recordEvents [ inc ]

getSocketUDP' :: String -> Int -> IO Socket
getSocketUDP' host port = do
    (sock, info) <- getSocketUDP host port
    let addr = addrAddress info
    -- reuse since hashicorp/memberlist seems to want us to use same port
    setSocketOption sock ReuseAddr 1
    NS.bind sock addr
    return sock

makeStore :: Member -> IO Store
makeStore self = do
    mems <- newTVarIO Map.empty
    -- num <- newTVarIO 0
    events <- newTVarIO []
    seqNo <- newTVarIO 0
    inc <- newTVarIO 0
    let store = Store { storeSeqNo = seqNo
                      , storeIncarnation = inc
                      , storeMembers = mems
                      , storeEvents = events
                      , storeSelf = self
                      -- , storeNumMembers = num
                      }
    return store

withSocket :: IO Socket -> (Socket -> IO a) -> IO a
withSocket s = bracket s close

dumpEvents :: Store -> IO ()
dumpEvents s = do
    events <- atomically $ readTVar $ storeEvents s
    mapM_ print events

-- gossip/schedule
waitForAckOf :: Int -> AckChan -> IO ()
waitForAckOf seqNo' ackChan =
  sourceTMChan ackChan $$ ackOf (fromIntegral seqNo') =$ CC.sinkNull >> return ()
  where
    ackOf s = awaitForever $ \ackSeqNo ->
      if s == ackSeqNo then return () else ackOf s

membersAndSelf :: Store -> STM (Member, [Member])
membersAndSelf s = do
  ms <- readTVar $ storeMembers s
  return (storeSelf s, Map.elems ms)

blah = do
    now <- getCurrentTime
    let self = Member { memberName = "myself"
                      , memberHost = "localhost"
                      , memberAlive = IsAlive
                      , memberIncarnation = 0
                      , memberLastChange = now
                      }

    store <- makeStore self
    -- sendAndReceiveState
    withSocket (getSocketUDP' "127.0.0.1" 4000) $ \udpSocket -> do
      installHandler sigUSR1 (Catch $ dumpEvents store) Nothing
      UDP.sourceSocket udpSocket 65336 $$ handleMessage store $= UDP.sinkToSocket udpSocket
