module Database.MySQL.Simple.Encoder where

import           Data.Bits
import           Data.ByteString                    (ByteString)
import qualified Data.ByteString.Internal           as ByteString
import           Data.Functor.Contravariant
import           Data.Int
import           Data.Semigroup
import           Data.Text                          (Text)
import           Data.Time
import qualified Data.Vector                        as V
import           Data.Vector.Mutable                (IOVector)
import qualified Data.Vector.Mutable                as MV
import           Data.Word
import           Database.MySQL.Protocol.MySQLValue (BitMap (..),
                                                     MySQLValue (..))
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.Storable
import           System.IO.Unsafe

data Param a = Param !Int (Value a)

instance Contravariant Param where
  contramap f (Param n g) = Param n (contramap f g)

instance Semigroup (Param a) where
  p <> q = appendParam p q
  {-# INLINE (<>) #-}

newtype Value a =
  Value { runValue :: a
                   -> Int
                   -> Ptr Word8
                   -> IOVector MySQLValue
                   -> IO ()
        }

instance Contravariant Value where
  contramap f (Value g) = Value (g . f)

appendParam :: Param a -> Param a -> Param a
appendParam (Param n1 f) (Param n2 g) =
  Param (n1 + n2) (Value $ \a i bitmap params -> do
    runValue f a i bitmap params
    runValue g a (i + 1) bitmap params)

value :: Value a -> Param a
value v = Param 1 v

runParam :: Param a -> a -> (V.Vector MySQLValue, BitMap)
runParam (Param n f) a = unsafeDupablePerformIO $ do
  let bitmapSize = n + 7 `unsafeShiftR` 3
  params <- MV.unsafeNew n
  fop    <- mallocForeignPtrBytes bitmapSize
  withForeignPtr fop $ \op -> do
    runValue f a 0 op params
  params' <- V.unsafeFreeze params
  let bitmap = BitMap (ByteString.fromForeignPtr fop 0 bitmapSize)
  return (params', bitmap)


nullable :: Value a -> Value (Maybe a)
nullable val = Value $ \ma i bitmap params -> do
  case ma of
    Just a  -> runValue val a i bitmap params
    Nothing -> do
      MV.unsafeWrite params i MySQLNull
      let
        op :: Ptr Word8
        op = bitmap `plusPtr` (i `unsafeShiftR` 3)
      b <- peek op
      poke op (b `setBit` i .&. 3)

mkValue :: (a -> MySQLValue) -> Value a
mkValue f = Value $ \a i _bitmap params -> do
  MV.unsafeWrite params i (f a)
{-# INLINE mkValue #-}

int8 :: Value Int8
int8 = mkValue MySQLInt8

word8 :: Value Word8
word8 = mkValue MySQLInt8U

int16 :: Value Int16
int16 = mkValue MySQLInt16

word16 :: Value Word16
word16 = mkValue MySQLInt16U

int32 :: Value Int32
int32 = mkValue MySQLInt32

word32 :: Value Word32
word32 = mkValue MySQLInt32U

int64 :: Value Int64
int64 = mkValue MySQLInt64

word64 :: Value Word64
word64 = mkValue MySQLInt64U

float :: Value Float
float = mkValue MySQLFloat

double :: Value Double
double = mkValue MySQLDouble

date :: Value Day
date = mkValue MySQLDate

timestamp :: Value LocalTime
timestamp = mkValue MySQLTimeStamp

datetime :: Value LocalTime
datetime = mkValue MySQLDateTime

bytestring :: Value ByteString
bytestring = mkValue MySQLBytes

text :: Value Text
text = mkValue MySQLText


data User = User { ua :: !Int32, ub :: !Int32, uc :: !Int32, ud :: !(Maybe Text) }

test1 :: Param User
test1 =
     value (contramap ua int32)
  <> value (contramap ub int32)
  <> value (contramap uc int32)
  <> value (contramap ud (nullable text))
