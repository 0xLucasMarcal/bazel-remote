package blake3verifier

import (
	"encoding/hex"
	"fmt"
	"io"

	"lukechampine.com/blake3"
)

type blake3verifier struct {
	hasher              *blake3.Hasher
	expectedSize        int64
	expectedHash        string
	actualSize          int64
	multiWriter         io.Writer
	originalWriteCloser io.WriteCloser
}

func New(expectedHash string, expectedSize int64, writeCloser io.WriteCloser) *blake3verifier {
	h := blake3.New(32, nil)

	return &blake3verifier{
		hasher:              h,
		expectedHash:        expectedHash,
		expectedSize:        expectedSize,
		multiWriter:         io.MultiWriter(h, writeCloser),
		originalWriteCloser: writeCloser,
	}
}

func (b *blake3verifier) Write(p []byte) (int, error) {
	n, err := b.multiWriter.Write(p)
	if n > 0 {
		b.actualSize += int64(n)
	}

	return n, err
}

func (b *blake3verifier) Close() error {
	if b.actualSize != b.expectedSize {
		return fmt.Errorf("error: expected %d bytes, got %d", b.expectedSize, b.actualSize)
	}

	actualHash := hex.EncodeToString(b.hasher.Sum(nil))
	if actualHash != b.expectedHash {
		return fmt.Errorf("error: expected hash %s, got %s", b.expectedHash, actualHash)
	}

	return b.originalWriteCloser.Close()
}
