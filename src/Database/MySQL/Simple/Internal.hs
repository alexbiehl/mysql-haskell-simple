{-# LANGUAGE MultiWayIf #-}
module Database.MySQL.Simple.Internal where

import           Control.Exception
import           Control.Monad
import           Data.IORef
import qualified Data.Vector as V
import           Database.MySQL.Base
import           Database.MySQL.Connection
import           Database.MySQL.Protocol.MySQLValue
import           System.IO.Streams
import qualified System.IO.Streams as Stream


-- this is a straight copy from 'mysql-haskell with some tweaks around passing params
-- as vector.
queryVectorInternal :: MySQLConn -> StmtID -> V.Vector MySQLValue -> BitMap -> IO (V.Vector ColumnDef, InputStream (V.Vector MySQLValue))
queryVectorInternal conn@(MySQLConn is os _ consumed) stid params nullmap = do
    guardUnconsumed conn
    writeCommand (COM_STMT_EXECUTE stid (V.toList params) nullmap) os
    p <- readPacket is
    if isERR p
      then decodeFromPacket p >>= throwIO . ERRException
      else do
        len <- getFromPacket getLenEncInt p
        fields <- V.replicateM len $ (decodeFromPacket <=< readPacket) is
        _ <- readPacket is -- eof packet, we don't verify this though
        writeIORef consumed False
        rows <- Stream.makeInputStream $ do
            q <- readPacket is
            if  | isOK  q  -> Just <$> getFromPacket (getBinaryRowVector fields len) q
                | isEOF q  -> writeIORef consumed True >> return Nothing
                | isERR q  -> decodeFromPacket q >>= throwIO . ERRException
                | otherwise -> throwIO (UnexpectedPacket q)
        return (fields, rows)
