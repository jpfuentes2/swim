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
removeDeadNodes s = atomically $ do
    mems <- readTVar $ storeMembers s
    _ <- swapTVar (storeMembers s) $ Map.filter ((/= IsDead) . memberAlive) mems
    return ()

-- gives kRandomNodes excluding our own host from the list
kRandomNodesExcludingSelf :: Config -> Store -> IO [Member]
kRandomNodesExcludingSelf cfg s = do
  nodes <- liftIO $ atomically $ readTVar $ storeMembers s
  liftIO $ kRandomNodes (cfgGossipNodes cfg) [] (Map.elems nodes)

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

every' :: Int -> Producer IO UTCTime
every' d = loop
  where
    loop = do
        t <- liftIO $ threadDelay d >> getCurrentTime
        yield t >> loop

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

makeStore :: IO Store
makeStore = do
    mems <- newTVarIO Map.empty
    -- num <- newTVarIO 0
    events <- newTVarIO []
    seqNo <- newTVarIO 0
    inc <- newTVarIO 0
    let store = Store { storeSeqNo = seqNo
                      , storeIncarnation = inc
                      , storeMembers = mems
                      , storeEvents = events
                      -- , storeNumMembers = num
                      }
    return store

withSocket :: IO Socket -> (Socket -> IO a) -> IO a
withSocket s = bracket s close

dumpEvents :: Store -> IO ()
dumpEvents s = do
    events <- atomically $ readTVar $ storeEvents s
    mapM_ print events

-- TODO: Message s/b Dead -- this ADT has caused me many problems and this is not ex
deadNode :: Store -> Message -> IO ()
deadNode s (Dead incarnation node from) = atomically $ do
  members <- readTVar $ storeMembers s
  let currMember = Map.lookup node members
  currInc <- readTVar $ storeIncarnation s

  let x = case currMember of
        Just m ->
          if ignore m then
            return ()
          else
            return ()
        Nothing -> return ()
  x
  where ignore m = memberName m == node || incarnation < memberIncarnation m || memberAlive m == IsDead

-- gossip/schedule
waitForAckOf :: Int -> AckChan -> IO ()
waitForAckOf seqNo' ackChan =
  sourceTMChan ackChan $$ ackOf (fromIntegral seqNo') =$ CC.sinkNull >> return ()
  where
    ackOf s = awaitForever $ \ackSeqNo ->
      if s == ackSeqNo then return () else ackOf s

membersAndSelf :: Store -> STM (Member, [Member])
membersAndSelf s =
  readTVar $ storeMembers s >>= (\ms -> return (storeSelf s, Map.elems ms))

-- TODO: solve the expression problem of our Message type: we want Suspect msg here
suspectNode :: Store -> Int -> String -> IO (Maybe Message)
suspectNode s i name = atomically $ do
  (self, members) <- membersAndSelf s
  currInc <- readTVar $ storeIncarnation s

  case find ((== name) . memberName) members of
    -- ignore old incarnation or non-alive
    Just m | currInc < i || notAlive m -> return Nothing

    -- no cluster, we're not suspect. refute it.
    Just m | name == memberName self -> do
               -- update member state for new Incarnation
               i' <- nextIncarnation' s $ memberIncarnation m
               let m' = m { memberIncarnation = i' }
               _ <- saveMember m'

               -- return so we don't mark ourselves suspect
               return $ Just Alive { incarnation = i'
                                   , node = name
                                   , fromAddr = 1 :: Word32 -- (memberHost m)
                                   , port = 1 :: Word16
                                   , version = [] }
    -- broadcast
    Just m -> do
      markSuspect m
      return $ Just Suspect { incarnation = i, node = name }

    -- we don't know you, yo
    Nothing -> return Nothing

  where
    saveMember m = modifyTVar (Map.insert (memberName m) m) (storeMembers s)

    markSuspect m = undefined
      -- let memberIncarnation = 
      -- writeTVar (storeIncarnation s)
      --          let updateMember m alive = m { memberAlive = alive }
      --          updateMember m 
      --          m = { }

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
      _ <- mapLeft (const $ suspectNode s (memberIncarnation m) (memberName m)) ack
      return ()

failureDetector :: Config -> Store -> IO ()
failureDetector cfg s =
    loop
  where
    d = 5
    loop = do
        _ <- threadDelay d >> getCurrentTime
        nodes <- atomically $ readTVar $ storeMembers s
        -- TODO: exclude ourselves via cfg
        randomNodes <- kRandomNodes (cfgGossipNodes cfg) [] (Map.elems nodes)
        --mapM_ (const $ probeNode s) randomNodes
        -- probe probeNodes
        -- yield Ping { seqNo = 1, node = "wat" }
        loop

blah = do
    store <- makeStore
    -- sendAndReceiveState
    withSocket (getSocketUDP' "127.0.0.1" 4000) $ \udpSocket -> do
      installHandler sigUSR1 (Catch $ dumpEvents store) Nothing
      UDP.sourceSocket udpSocket 65336 $$ handleMessage store $= UDP.sinkToSocket udpSocket
