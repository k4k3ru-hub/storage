// compaction_codec_test.go
package dataset

import (
	"context"
	"io"
	"testing"
)

type codecOnly struct{}

func (codecOnly) Encode(context.Context, io.Writer, []int) error {
	return nil
}

func (codecOnly) Decode(context.Context, ReadSource, int64) ([]int, error) {
	return nil, nil
}

func TestCodecDoesNotRequireCompactionMethods(t *testing.T) {
	var codec Codec[int] = codecOnly{}
	if _, ok := codec.(CompactionCodec[int]); ok {
		t.Fatal("Codec unexpectedly implements CompactionCodec")
	}
}
