{-# LANGUAGE RankNTypes #-}
module Database.MySQL.Simple.Encoder where

import           Control.Monad.ST
import           Data.ByteString                    (ByteString)
import           Data.Functor.Contravariant
import           Data.Int
import           Data.Monoid                        ((<>))
import           Data.Text                          (Text)
import           Data.Time
import qualified Data.Vector                        as V
import           Data.Vector.Mutable                (STVector)
import qualified Data.Vector.Mutable                as MV
import           Data.Word
import           Database.MySQL.Protocol.MySQLValue (MySQLValue (..))

data Param a = Param !Int (Value a)

instance Contravariant Param where
  contramap f (Param n g) = Param n (contramap f g)

instance Monoid (Param a) where
  mempty      = unitParam
  mappend p q = appendParam p q

newtype Value a =
  Value { runValue :: forall s. a
                   -> Int
                   -> STVector s MySQLValue
                   -> ST s Int
        }

instance Contravariant Value where
  contramap f (Value g) = Value (g . f)

unitParam :: Param a
unitParam = Param 0 $ Value (\_ i _ -> return i)

appendParam :: Param a -> Param a -> Param a
appendParam (Param n1 f) (Param n2 g) =
  Param (n1 + n2) (Value $ \a i v -> do
    j <- runValue f a i v
    runValue g a j v)

value :: Value a -> Param a
value v = Param 1 v

runParam :: Param a -> a -> V.Vector MySQLValue
runParam (Param n f) a = V.create $ do
  v <- MV.unsafeNew n
  _ <- runValue f a 0 v
  return v

nullable :: Value a -> Value (Maybe a)
nullable val = Value $ \ma i v -> do
  case ma of
    Just a  -> runValue val a i v
    Nothing -> do MV.unsafeWrite v i MySQLNull
                  return (i + 1)

mkValue :: (a -> MySQLValue) -> Value a
mkValue f = Value $ \a i v -> do
  MV.unsafeWrite v i (f a)
  return (i + 1)
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

bytestring :: Value ByteString
bytestring = mkValue MySQLBytes

text :: Value Text
text = mkValue MySQLText


data User = User { ua :: !Int32, ub :: !Int32, uc :: !Int32, ud :: !Text }

test1 :: Param User
test1 =
     value (contramap ua int32)
  <> value (contramap ub int32)
  <> value (contramap uc int32)
  <> value (contramap ud text)
