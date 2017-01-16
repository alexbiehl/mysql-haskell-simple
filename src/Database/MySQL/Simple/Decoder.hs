{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes   #-}
module Database.MySQL.Simple.Decoder (
    Result
  , runResult

  , Row
  , runRow
  , maybeRow
  , rowsVector
  , foldlRows

  , Value
  , column
  , nullable

  , DecodingError(..)
  , QueryError(..)

  , int8
  , word8
  , int16
  , word16
  , int32
  , word32
  , int64
  , word64
  , float
  , double
  , date
  , time
  , timestamp
  , datetime
  , bytestring
  , text


  , test1
  , testRes
  ) where

import           Control.Exception
import           Data.ByteString                    (ByteString)
import           Data.Int
import           Data.Text                          (Text)
import           Data.Time
import           Data.Vector                        (Vector, MVector)
import qualified Data.Vector                        as Vector
import           Data.Vector.Mutable                (IOVector)
import qualified Data.Vector.Mutable                as MVector
import           Data.Word
import           Database.MySQL.Protocol.ColumnDef  (ColumnDef)
import           Database.MySQL.Protocol.MySQLValue (MySQLValue (..))
import qualified System.IO.Streams                  as Streams

data DecodingError = InvalidValue
                   | InvalidRow
                   deriving (Show)

data Row a =
  Row { rowCols    :: !Int
      , _rowDecode :: forall r. (a -> Int -> r)
                   -> (DecodingError -> r)
                   -> Vector MySQLValue
                   -> Int
                   -> r
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
  Value { runValue :: forall r. (a -> r)
                   -> (DecodingError -> r)
                   -> MySQLValue
                   -> r
        }

instance Functor Value where
  fmap f (Value m) = Value $ \succ_ fail_ v ->
    m (succ_ . f) fail_ v
  {-# INLINE fmap #-}

data QueryError = QueryError DecodingError
                deriving (Show)

instance Exception QueryError

newtype Result a =
  Result { runResult :: Vector ColumnDef
                     -> Streams.InputStream (Vector MySQLValue)
                     -> IO (Either QueryError a)
         }

instance Functor Result where
  fmap f (Result g) = Result $ \defs is ->
    fmap f <$> g defs is
  {-# INLINE fmap #-}

column :: Value a -> Row a
column valueDecoder = Row 1 $ \succ_ fail_ values i -> do
  runValue 
    valueDecoder
    (\a -> succ_ a i)
    fail_
    (Vector.unsafeIndex values i)
{-# INLINE column #-}

runRow :: forall a r. Row a
       -> Vector MySQLValue
       -> (a -> r)
       -> (QueryError -> r)
       -> r
runRow (Row columns decodeRow) values succ_ fail_
  | Vector.length values == columns = do
      decodeRow
        (\a _ -> succ_ a)
        (fail_ . QueryError)
        values
        0
  | otherwise =
      fail_ (QueryError InvalidRow)
{-# INLINE runRow #-}

maybeRow :: Row a -> Result (Maybe a)
maybeRow rowDecoder = Result $ \defs is -> do
  case rowCols rowDecoder == Vector.length defs of
    False -> return $ Left (QueryError InvalidRow)
    True -> do 
      mrow <- Streams.read is
      case mrow of
        Just row  -> do
          runRow
            rowDecoder
            row
            (return . Right . Just)
            (return . Left)
        Nothing -> return (Right Nothing)
{-# INLINE maybeRow #-}

-- for result methods we first new more efficient accessors in mysql-haskell
rowsVector :: Row a -> Result (Vector a)
rowsVector rowDecoder =
  rowsVectorSized dEFAULT_BUFSIZE rowDecoder
  where
    dEFAULT_BUFSIZE = 64
{-# INLINE rowsVector #-}

rowsVectorSized :: Int -> Row a -> Result (Vector a)
rowsVectorSized initialSize rowDecoder = Result $ \defs is -> do
  case rowCols rowDecoder == Vector.length defs of
    False -> return $ Left (QueryError InvalidRow)
    True -> do
      v <- MVector.unsafeNew initialSize
      loop is 0 v
  where
    loop !is !i !v = do
      mrow <- Streams.read is
      case mrow of
        Just row -> do
          runRow
            rowDecoder
            row
            (\a -> loop is (i + 1) =<< append v i a)
            (return . Left)
        Nothing -> do
          v' <- Vector.unsafeFreeze $ MVector.unsafeSlice 0 i v
          return $! (Right v')

    append :: IOVector a
           -> Int
           -> a
           -> IO (IOVector a)
    append v i x
      | i < MVector.length v = do
          MVector.unsafeWrite v i x
          return v
      | otherwise = do
          v' <- enlarge v
          MVector.unsafeWrite v' i x
          return v'

    enlarge :: IOVector a
            -> IO (IOVector a)
    enlarge v =
      MVector.unsafeGrow v (max (MVector.length v) 1)
{-# INLINE rowsVectorSized #-}

foldlRows :: (a -> b -> a) -> a -> Row b -> Result a
foldlRows step zero rowDecoder = Result $ \defs is -> do
  case rowCols rowDecoder == Vector.length defs of
    False -> return $ Left (QueryError InvalidRow)
    True -> loop is zero
  where
    loop !is !s = do
      mrow <- Streams.read is
      case mrow of
        Just row -> do
          runRow
            rowDecoder
            row
            (\a -> loop is $! step s a)
            (\e -> return (Left e))
        Nothing -> return (Right s)
{-# INLINE foldlRows #-}

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

data User = User { ua :: !Int32, ub :: !Int32, uc :: !Int32, ud :: !(Maybe Text) }

test1 :: Row User
test1 =
  User <$> column int32 <*> column int32 <*> column int32 <*> column (nullable text)
{-# INLINE test1 #-}

testRes :: Result (Vector User)
testRes = rowsVector test1 -- foldlRows (\count _user -> count + 1) 0 test1
{-# INLINE testRes #-}
