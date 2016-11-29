{-# LANGUAGE RankNTypes #-}
module Database.MySQL.Simple.Decoder where

import           Control.Exception
import           Control.Monad.ST
import           Data.Bifunctor
import           Data.ByteString                    (ByteString)
import           Data.Functor.Contravariant
import           Data.Int
import           Data.Text                          (Text)
import           Data.Time
import           Data.Vector                        (Vector)
import qualified Data.Vector                        as V
import qualified Data.Vector.Mutable                as MV
import           Data.Word
import           Database.MySQL.Protocol.MySQLValue (MySQLValue (..))
import qualified System.IO.Streams                  as Streams
import qualified System.IO.Streams.Combinators      as Streams

data DecodingError = InvalidValue
                   | InvalidRow
                   deriving (Show)

type DecodeResult a = Either DecodingError a

data Row a =
  Row { rowCols   :: !Int
      , rowDecode :: forall r. (a -> Int -> DecodeResult r)
                  -> (DecodingError -> DecodeResult r)
                  -> Vector MySQLValue
                  -> Int
                  -> DecodeResult r
      }

instance Functor Row where
  fmap f (Row n m) = Row n $ \succ_ fail_ v i ->
    m (succ_ . f) fail_ v i
  {-# INLINE fmap #-}

instance Applicative Row where
  pure a = Row 0 $ \succ_ _fail _v i -> succ_ a i
  {-# INLINE pure #-}

  Row n fm <*> Row m am = Row (n + m) $ \succ_ fail_ v i ->
    fm (\f j -> am (\a k -> succ_ (f a) k) fail_ v j) fail_ v i
  {-# INLINE (<*>) #-}

newtype Value a =
  Value { runValue :: forall r. (a -> DecodeResult r)
                   -> (DecodingError -> DecodeResult r)
                   -> MySQLValue
                   -> DecodeResult r
        }

instance Functor Value where
  fmap f (Value m) = Value $ \succ_ fail_ v ->
    m (succ_ . f) fail_ v
  {-# INLINE fmap #-}

data QueryError = QueryError DecodingError
                deriving (Show)

instance Exception QueryError

newtype Result a =
  Result { runResult :: Streams.InputStream (Vector MySQLValue)
                     -> IO (Either QueryError a)
         }

instance Functor Result where
  fmap f (Result g) = Result $ \s -> do r <- g s
                                        return (fmap f r)

col :: Value a -> Row a
col val = Row 1 $ \succ_ fail_ v i ->
  runValue val (\a -> succ_ a (i + 1)) fail_ (V.unsafeIndex v i)

runRow :: Row a -> Vector MySQLValue -> Either QueryError a
runRow (Row cols decode) v
  | V.length v == cols =
    bimap (\e -> QueryError e) (\x -> x) $
      decode (\a _ -> Right a) (\e -> Left e) v 0
  | otherwise =
    Left (QueryError InvalidRow)

maybeRow :: Row a -> Result (Maybe a)
maybeRow row = Result $ \is -> do
  mr <- Streams.read is
  case mr of
    Just r -> case runRow row r of
      Right a -> return $! Right $! Just $! a
      Left e  -> return $ Left e
    Nothing -> return (Right Nothing)

-- for result methods we first new more efficient accessors in mysql-haskell
rowsVector :: Row a -> Result (Vector a)
rowsVector row = undefined

rowsList :: Row a -> Result [a]
rowsList row = foldrRows (:) [] row

foldlRows :: (a -> b -> a) -> a -> Row b -> Result a
foldlRows f z row = Result $ \is -> loop is z
  where
    loop is s = do
      ma <- Streams.read is
      case ma of
        Just a -> case runRow row a of
          Right a -> loop is $! (f s a)
          Left e  -> return $ Left e
        Nothing -> return (Right s)

foldrRows :: (b -> a -> a) -> a -> Row b -> Result a
foldrRows f z row = Result $ \is -> undefined

nullable :: Value a -> Value (Maybe a)
nullable (Value val) = Value $ \succ_ fail_ v ->
  case v of
    MySQLNull -> succ_ Nothing
    _         -> val (succ_ . Just) fail_ v
{-# INLINE nullable #-}

int8 :: Value Int8
int8 = Value $ \succ_ fail_ mv ->
                 case mv of
                   MySQLInt8 i -> succ_ i
                   _           -> fail_ InvalidValue


word8 :: Value Word8
word8 = Value $ \succ_ fail_ mv ->
                  case mv of
                    MySQLInt8U i -> succ_ i
                    _            -> fail_ InvalidValue



int16 :: Value Int16
int16 = Value $ \succ_ fail_ mv ->
                  case mv of
                    MySQLInt8  i -> succ_ (fromIntegral i)
                    MySQLInt16 i -> succ_ i
                    _            -> fail_ InvalidValue


word16 :: Value Word16
word16 = Value $ \succ_ fail_ mv ->
                   case mv of
                     MySQLInt8U i  -> succ_ (fromIntegral i)
                     MySQLInt16U i -> succ_ i
                     _             -> fail_ InvalidValue


int32 :: Value Int32
int32 = Value $ \succ_ fail_ mv ->
                  case mv of
                    MySQLInt8 i  -> succ_ (fromIntegral i)
                    MySQLInt16 i -> succ_ (fromIntegral i)
                    MySQLInt32 i -> succ_ i
                    _            -> fail_ InvalidValue


word32 :: Value Word32
word32 = Value $ \succ_ fail_ mv ->
                   case mv of
                     MySQLInt8U i  -> succ_ (fromIntegral i)
                     MySQLInt16U i -> succ_ (fromIntegral i)
                     MySQLInt32U i -> succ_ i
                     _             -> fail_ InvalidValue


int64 :: Value Int64
int64 = Value $ \succ_ fail_ mv ->
                  case mv of
                    MySQLInt8 i  -> succ_ (fromIntegral i)
                    MySQLInt16 i -> succ_ (fromIntegral i)
                    MySQLInt32 i -> succ_ (fromIntegral i)
                    MySQLInt64 i -> succ_ i
                    _            -> fail_ InvalidValue


word64 :: Value Word64
word64 = Value $ \succ_ fail_ mv ->
                   case mv of
                     MySQLInt8U i  -> succ_ (fromIntegral i)
                     MySQLInt16U i -> succ_ (fromIntegral i)
                     MySQLInt32U i -> succ_ (fromIntegral i)
                     MySQLInt64U i -> succ_ i
                     _             -> fail_ InvalidValue


float :: Value Float
float = Value $ \succ_ fail_ mv ->
                  case mv of
                    MySQLFloat i -> succ_ i
                    _            -> fail_ InvalidValue


double :: Value Double
double = Value $ \succ_ fail_ mv ->
                   case mv of
                     MySQLDouble i -> succ_ i
                     _             -> fail_ InvalidValue


text :: Value Text
text = Value $ \succ_ fail_ mv ->
                 case mv of
                   MySQLText t -> succ_ t
                   _           -> fail_ InvalidValue


bytestring :: Value ByteString
bytestring = Value $ \succ_ fail_ mv ->
                       case mv of
                         MySQLBytes bs -> succ_ bs
                         _             -> fail_ InvalidValue

date :: Value Day
date = Value $ \succ_ fail_ mv ->
                 case mv of
                   MySQLDate d -> succ_ d
                   _           -> fail_ InvalidValue

time :: Value TimeOfDay
time = Value $ \succ_ fail_ mv ->
                case mv of
                  MySQLTime _ t -> succ_ t
                  _             -> fail_ InvalidValue

timestamp :: Value LocalTime
timestamp = Value $ \succ_ fail_ mv ->
                      case mv of
                        MySQLTimeStamp t -> succ_ t
                        _                -> fail_ InvalidValue

datetime :: Value LocalTime
datetime = Value $ \succ_ fail_ mv ->
                     case mv of
                       MySQLDateTime dt -> succ_ dt
                       _                -> fail_ InvalidValue

