package importhttp

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// zstdEncoding is the HTTP content coding of a compressed body (RFC 8878)
const zstdEncoding = "zstd"

// zstdWindowBytes is the window of the sender's encoder. A part repeats its
// record IDs and provider hashes within a short distance, so a small window
// loses little and keeps each encoder at about 2 MiB.
const zstdWindowBytes = 1 << 20

// zstdMaxWindowBytes is the largest window that the client's decoder
// accepts, and so about the most memory that one decoder holds. It is above
// zstdWindowBytes, so that a client reads a sender whose window grows by
// that much.
const zstdMaxWindowBytes = 8 << 20

// newEncoder returns an encoder that writes one frame to w. It encodes in
// the caller's writes, with no goroutines of its own. Closing it ends the
// frame.
func newEncoder(w io.Writer) (*zstd.Encoder, error) {
	return zstd.NewWriter(w, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(zstdWindowBytes))
}

// newDecoder returns a decoder that reads r. It decodes in the caller's
// reads, with no goroutines of its own. The caller closes it when done.
// A frame with a window over zstdMaxWindowBytes fails its first read with
// zstd.ErrWindowSizeExceeded.
func newDecoder(r io.Reader) (*zstd.Decoder, error) {
	return zstd.NewReader(r, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(zstdMaxWindowBytes))
}
