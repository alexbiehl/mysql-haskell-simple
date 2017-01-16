{-# LANGUAGE PatternGuards #-}
module Database.MySQL.Simple.Session (
    Session
  , SessionError(..)
  , runSession

  , Query
  , statement
  , query
  ) where

import           Database.MySQL.Simple.Encoder
import           Database.MySQL.Simple.Decoder
import           Database.MySQL.Simple.Internal

import           Control.Arrow
import qualified Control.Category as Category
import           Control.Category hiding ((.))
import           Control.Exception
import           Control.Monad.IO.Class
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LByteString
import           Data.HashTable.IO (BasicHashTable)
import qualified Data.HashTable.IO as HashTable
import           Data.Profunctor
import           Data.Typeable
import           Database.MySQL.Base (MySQLConn)
import qualified Database.MySQL.Base as MySQL

type StatementRegistry = BasicHashTable ByteString Statement

type Statement = MySQL.StmtID

newtype Session a =
  Session { unSession :: MySQLConn -> StatementRegistry -> IO a }

instance Functor Session where
  fmap f (Session m) = Session $ \mysqlConn registry ->
    fmap f (m mysqlConn registry)

instance Applicative Session where
  pure a = Session $ \_ _ -> pure a

  Session f <*> Session m = Session $ \mysqlConn registry ->
    f mysqlConn registry <*> m mysqlConn registry

instance Monad Session where
  Session m >>= f = Session $ \mysqlConn registry -> do
    a <- m mysqlConn registry
    unSession (f a) mysqlConn registry

instance MonadIO Session where
  liftIO m = Session $ \_ _ -> m

prepareStatement :: MySQLConn -> ByteString -> StatementRegistry -> IO Statement
prepareStatement mysqlConn qry registry = do
  mstmt <- HashTable.lookup registry qry
  case mstmt of
    Just stmt -> return stmt
    Nothing   -> do
      stmt <- MySQL.prepareStmt
              mysqlConn
              (MySQL.Query (LByteString.fromStrict qry))
      HashTable.insert registry qry stmt
      return stmt

newtype Query a b = Query (a -> Session b)

instance Category Query where
  id =
    Query $ \a -> pure a

  (Query m) . (Query n) =
    Query $ \a -> n a >>= \b -> m b

instance Arrow Query where
  arr f =
    Query (return . f)

  first (Query f) =
    Query (\ ~(b, d) -> f b >>= \c -> return (c, d))

  second (Query f) =
    Query (\ ~(d, b) -> f b >>= \c -> return (d, c))

instance Functor (Query a) where
  fmap f (Query m) =
    Query $ \a -> fmap f (m a)

instance Profunctor Query where
  dimap f g (Query m) =
    Query $ \a -> fmap g (m (f a))

  lmap f (Query m) =
    Query $ \a -> m (f a)

  rmap f (Query m) =
    Query $ \a -> fmap f (m a)

statement :: ByteString -> Params a -> Result b -> Query a b
statement qry prm result = Query $ \a -> Session $ \mysqlConn registry -> do
  let (values, nullmap) = runParams prm a
  stmt         <- prepareStatement mysqlConn qry registry
  (defs, rows) <- queryVectorInternal mysqlConn stmt values nullmap
  res          <- runResult result defs rows
  case res of
    Right b  -> return b
    Left err -> throwIO err

query :: a -> Query a b -> Session b
query a (Query run) = run a

data SessionError = SessQueryErr QueryError
                  | SessMySQLErr SomeException
                  deriving (Show)

instance Exception SessionError

runSession :: MySQLConn -> Session a -> IO (Either SessionError a)
runSession mysqlConn session = do
  registry <- HashTable.new
  res <- try $ unSession session mysqlConn registry
  case res of
    Right a -> return (Right a)
    Left err@(SomeException exc)
      | Just e@QueryError{} <- cast exc              -> return $ Left (SessQueryErr e)
      | Just MySQL.NetworkException{} <- cast exc    -> return $ Left (SessMySQLErr err)
      | Just MySQL.UnconsumedResultSet{} <- cast exc -> return $ Left (SessMySQLErr err)
      | Just MySQL.ERRException{} <- cast exc        -> return $ Left (SessMySQLErr err)
      | Just MySQL.WrongParamsCount{} <- cast exc    -> return $ Left (SessMySQLErr err)
      | otherwise                                    -> throwIO err
