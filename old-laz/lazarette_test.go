package lazarette

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/byuoitav/lazarette/cache"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" + "!@#$%^&*()-_=+;|/\\{}"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func open(tb testing.TB) *lazarette {
	cash, err := Open(os.TempDir())
	if err != nil {
		tb.Fatal(err)
	}

	laz := cash.(*lazarette)
	laz.Clean()

	return laz
}

func randKey(tb testing.TB, maxLength int) string {
	for {
		b := make([]byte, seededRand.Intn(maxLength))
		for i := range b {
			b[i] = charset[seededRand.Intn(len(charset))]
		}

		if len(string(b)) > 0 {
			return string(b)
		}
	}
}

func randVal(tb testing.TB, maxLength int) cache.Value {
	buf := make([]byte, seededRand.Intn(maxLength))
	_, err := seededRand.Read(buf)
	if err != nil {
		tb.Fatal(err)
	}

	return cache.Value{
		Timestamp: ptypes.TimestampNow(),
		Data:      buf,
	}
}

func checkValueEqual(tb testing.TB, key string, expected, actual *cache.Value) {
	if !proto.Equal(expected, actual) {
		tb.Fatalf("values don't match for key %q:\n\texpected: %s\n\tactual: %s\n", key, expected.String(), actual.String())
	}
}

func setAndCheck(tb testing.TB, laz *lazarette, key string, val cache.Value) {
	err := laz.Set(key, val)
	if err != nil {
		tb.Fatalf("failed to set %q: %v. buf was 0x%x", key, err, val)
	}

	nval, err := laz.Get(key)
	if err != nil {
		tb.Fatalf("failed to get %q: %v", key, err)
	}

	checkValueEqual(tb, key, &val, &nval)
}

func TestSetAndGet(t *testing.T) {
	laz := open(t)

	key := randKey(t, 50)
	val := randVal(t, 300)

	setAndCheck(t, laz, key, val)

	laz.Close()
}

func TestSettingTheSameKey(t *testing.T) {
	laz := open(t)

	key := randKey(t, 50)
	val := randVal(t, 300)

	for i := 0; i < 10; i++ {
		setAndCheck(t, laz, key, val)
		val = randVal(t, 300)
	}

	laz.Close()
}

func BenchmarkSet(b *testing.B) {
	laz := open(b)

	// generate keys/values
	var keys []string
	var vals []cache.Value

	for i := 0; i < 1000; i++ {
		keys = append(keys, randKey(b, 50))
	}

	for i := 0; i < 1000; i++ {
		vals = append(vals, randVal(b, 300))
	}

	b.Run("UniqueKeys", func(bb *testing.B) {
		for i := 0; i < bb.N; i++ {
			key := keys[i]
			val := vals[0]
			val.Timestamp = ptypes.TimestampNow()

			setAndCheck(bb, laz, key, val)
		}
	})

	b.Run("UniqueVals", func(bb *testing.B) {
		for i := 0; i < bb.N; i++ {
			key := keys[0]
			val := vals[i]
			val.Timestamp = ptypes.TimestampNow()

			setAndCheck(bb, laz, key, val)
		}
	})

	b.Run("UniqueKeysAndVals", func(bb *testing.B) {
		for i := 0; i < bb.N; i++ {
			key := keys[i]
			val := vals[i]
			val.Timestamp = ptypes.TimestampNow()

			setAndCheck(bb, laz, key, val)
		}
	})

	laz.Close()
}
