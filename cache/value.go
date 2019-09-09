package cache

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

const (
	linefeed = 0x0a
)

// Value .
type Value struct {
	Timestamp time.Time
	Data      []byte
}

// MarshalBinary .
func (v *Value) MarshalBinary() ([]byte, error) {
	switch {
	case v.Timestamp.IsZero():
		return []byte{}, fmt.Errorf("timestamp must be set")
	case v.Data == nil:
		return []byte{}, fmt.Errorf("data must not be nil")
	}

	timestamp, err := v.Timestamp.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("unable to parse value: %v", err)
	}

	buf := bytes.Buffer{}
	buf.Write(timestamp)
	buf.WriteByte(linefeed)
	buf.Write(v.Data)

	return buf.Bytes(), nil
}

// UnmarshalBinary .
func (v *Value) UnmarshalBinary(data []byte) error {
	buf := data
	if len(buf) == 0 {
		return errors.New("no data")
	}

	// get timestamp bytes
	idx := bytes.Index(buf, []byte{linefeed})
	if idx < 0 {
		return errors.New("invalid data: missing timestamp")
	}

	t := &time.Time{}
	err := t.UnmarshalBinary(buf[:idx])
	if err != nil {
		return fmt.Errorf("unable to parse timestamp: %v", err)
	}

	*v = Value{}
	v.Timestamp = *t
	v.Data = make([]byte, len(buf[idx+1:]))
	copy(v.Data, buf[idx+1:])

	return nil
}
