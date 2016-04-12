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

-- TODO: need a timer to mark this node as dead after suspect timeout
-- TODO: solve the expression problem of our Message type: we want Suspect msg here
suspectOrDeadNode' :: Store -> Message -> String -> Int -> Liveness -> IO (Maybe Message)
suspectOrDeadNode' s msg _ _ IsAlive = undefined
suspectOrDeadNode' s msg name incarnation suspectOrDead = do
  (self, members) <- atomically $ membersAndSelf s

  case find ((== name) . memberName) members of
    -- we don't know this node. ignore.
    Nothing -> return Nothing

    -- ignore old incarnation or non-alive
    Just m | incarnation < memberIncarnation m || livenessCheck m -> return Nothing

    -- no cluster, we're not suspect/dead. refute it.
    Just m | name == memberName self -> do
               i' <- atomically $ do
                 nextInc <- nextIncarnation' s $ memberIncarnation m
                 let m' = m { memberIncarnation = nextInc }
                 saveMember m' >> return nextInc

               -- return so we don't mark ourselves suspect/dead
               return $ Just Alive { incarnation = i'
                                   , node = name
                                   , fromAddr = 1 :: Word32 -- (memberHost m)
                                   , port = 1 :: Word16
                                   , version = [] }

    -- broadcast suspect/dead msg
    Just m -> do
      now <- getCurrentTime
      _ <- atomically $ do
        let m' = m { memberIncarnation = incarnation
                   , memberAlive = suspectOrDead
                   , memberLastChange = now }
        void $ saveMember m'

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
suspectNode s _ = undefined

deadNode :: Store -> Message -> IO (Maybe Message)
deadNode s msg@(Dead i name _) = suspectOrDeadNode' s msg name i IsDead
deadNode s _ = undefined

probeNode :: Config -> Store -> Member -> AckChan -> ConduitM AckResponse Message IO ()
probeNode cfg s m ackChan = do
  seqNo' <- liftIO $ nextSeqNo s
  yield Ping { seqNo = fromIntegral seqNo', node = show m }
  ack <- liftIO $ race (after $ seconds 5) (waitForAckOf seqNo' ackChan)

  case ack of
    -- received Ack so we're happy!
    Right _ -> return ()

    -- send IndirectPing to kRandomNodes
    Left _ -> do
      let indirectPing mem = IndirectPing { seqNo = fromIntegral seqNo', fromAddr = 1 :: Word32, node = "wat" }
      randomNodes <- liftIO $ kRandomNodesExcludingSelf cfg s
      mapM_ (yield . indirectPing) randomNodes
      ack <- liftIO $ race (after $ seconds 5) (waitForAckOf seqNo' ackChan)
      case ack of
        -- broadcast possible suspect msg
        Left _ -> do
          suspect <- liftIO $ suspectNode s $ Suspect (memberIncarnation m) (memberName m)
          maybe (return ()) (void . yield) suspect

        Right _ -> return ()

failureDetector :: Config -> Store -> IO ()
failureDetector cfg s =
    loop
  where
    d = 5
    loop = do
        _ <- threadDelay d >> getCurrentTime
        nodes <- atomically $ readTVar $ storeMembers s

        -- exclude self from list of nodes to probe
        randomNodes <- kRandomNodes (cfgGossipNodes cfg) [storeSelf s] (Map.elems nodes)

        -- TODO: plug in probeNode
        loop

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
