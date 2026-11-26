// Package importhttp carries an indexer import's parts over HTTP: a sender
// serves a Source's parts, and a client reads them as a remote Source.
//
// A part's response body is its entries, each a uvarint key length, the
// key, a uvarint value length and the value, in the kind's wire encoding.
// Keys are never empty, so a key length of 0 ends the entries. A trailer
// follows it and ends the body:
//   - a status byte: complete, partial, transient error or permanent error;
//   - the number of entries as a uvarint;
//   - for a partial part, the Resume cursor; for an error, its message; each
//     as a uvarint length and the bytes. A complete part has no more.
//
// A body that ends before its trailer, or whose count differs from the
// entries read, was cut off, and the part is retried.
package importhttp

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"slices"

	"github.com/ipni/go-indexer-core/importer"
)

// Trailer status bytes
const (
	statusComplete  byte = 0
	statusPartial   byte = 1
	statusTransient byte = 2
	statusPermanent byte = 3
)

// Limits on the fields of a body. A body that breaks them is corrupt, and
// the reader refuses a length over its limit before it allocates anything
// for it. The sender refuses to send an entry over them, as a permanent
// error that names its key.
const (
	// maxKeyBytes is the most bytes of a key. Every kind's key is far
	// shorter: a 20-byte record ID, a multihash, or a CID or peer ID as a
	// string.
	maxKeyBytes = 4 << 10
	// maxValueBytes is the most bytes of a value. The largest values are multihashes', 20 bytes for
	// each context the multihash is provided in: 64 MiB holds more than 3
	// million contexts.
	maxValueBytes = 64 << 20
	// maxTrailerFieldBytes is the most bytes of a trailer's field: a Resume
	// cursor, which is a key followed by one byte, or an error message,
	// which the sender cuts to this length.
	maxTrailerFieldBytes = maxKeyBytes + 1
)

// errCutOff is the error of a body that does not end with a valid trailer
var errCutOff = errors.New("the part's body is cut off")

// writeBody writes a part's body to w: the entries that entries yields,
// then the trailer. It stops at the first error that entries yields: a
// *importer.PartialError ends the body as partial with its Resume, an error
// marked with importer.PermanentError as a permanent error, and any other
// error as a transient error, each with the number of entries written. An
// entry with an empty key ends it as a permanent error.
//
// It makes many small writes, so w is normally buffered. It returns the
// trailer's status, the number of entries written, and an error only of w;
// the body then has no trailer.
func writeBody(
	w io.Writer,
	entries iter.Seq2[importer.Entry, error],
) (
	status byte,
	count uint64,
	err error,
) {
	// lenBuf holds a length varint of the current entry
	lenBuf := make([]byte, 0, binary.MaxVarintLen64)
	for e, err := range entries {
		if err == nil {
			err = checkEntry(e)
		}
		if err != nil {
			status, err := writeTrailer(w, count, err)
			return status, count, err
		}

		lenBuf = binary.AppendUvarint(lenBuf[:0], uint64(len(e.Key)))
		if err := writeAll(w, lenBuf, e.Key); err != nil {
			return 0, count, err
		}
		lenBuf = binary.AppendUvarint(lenBuf[:0], uint64(len(e.Value)))
		if err := writeAll(w, lenBuf, e.Value); err != nil {
			return 0, count, err
		}
		count++
	}
	status, err = writeTrailer(w, count, nil)
	return status, count, err
}

// checkEntry returns a permanent error when e cannot be sent: its key is
// empty, or its key or value is over its limit
func checkEntry(e importer.Entry) error {
	switch {
	case len(e.Key) == 0:
		return importer.PermanentError(errors.New("the source sent an empty key"))
	case len(e.Key) > maxKeyBytes:
		return importer.PermanentError(fmt.Errorf("key %x… has %d bytes, over the limit of %d", e.Key[:64], len(e.Key), maxKeyBytes))
	case len(e.Value) > maxValueBytes:
		return importer.PermanentError(fmt.Errorf("the value of key %x has %d bytes, over the limit of %d", e.Key, len(e.Value), maxValueBytes))
	}
	return nil
}

// writeTrailer writes the key length 0 and the trailer for a part that
// ended with err after count entries; a nil err is a complete part. It
// returns the trailer's status and the error of w.
func writeTrailer(w io.Writer, count uint64, err error) (byte, error) {
	status, field := statusComplete, []byte(nil)
	var partial *importer.PartialError
	switch {
	case err == nil:
	case errors.As(err, &partial):
		status, field = statusPartial, partial.Resume
	case importer.IsPermanent(err):
		status, field = statusPermanent, []byte(err.Error())
	default:
		status, field = statusTransient, []byte(err.Error())
	}

	if len(field) > maxTrailerFieldBytes {
		if status == statusPartial {
			// A source's Resume follows a key it sent, so this is its bug
			status, field = statusPermanent, fmt.Appendf(nil, "a Resume cursor of %d bytes, over the limit of %d", len(field), maxTrailerFieldBytes)
		} else {
			field = field[:maxTrailerFieldBytes]
		}
	}

	trailer := binary.AppendUvarint([]byte{0, status}, count)
	if status != statusComplete {
		trailer = binary.AppendUvarint(trailer, uint64(len(field)))
	}
	return status, writeAll(w, trailer, field)
}

// writeAll writes each of bs to w
func writeAll(w io.Writer, bs ...[]byte) error {
	for _, b := range bs {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

// readBody yields the entries of a part's body that r reads, then ends as
// the trailer says:
//   - a complete part ends after its last entry, with no error;
//   - a partial part ends with a *importer.PartialError with the Resume
//     cursor that the sender wrote;
//   - a permanent error ends with an error marked with
//     importer.PermanentError, and a transient error with an unmarked
//     error, each with the sender's message.
//
// A body that ends before its trailer, has a count that differs from the
// entries read, or has bytes after its trailer ends with a transient error,
// as does a failed read. An entry's slices are valid until the next
// iteration. It reads r through a bufio.Reader unless r is one.
func readBody(r io.Reader) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		br, ok := r.(*bufio.Reader)
		if !ok {
			br = bufio.NewReader(r)
		}
		if err := readEntries(br, yield); err != nil {
			yield(importer.Entry{}, err)
		}
	}
}

// readEntries yields the entries of a body that br reads, and returns how the
// part ended, as readBody describes: nil for a complete part, and also when
// yield stops the loop.
func readEntries(br *bufio.Reader, yield func(importer.Entry, error) bool) error {
	var count uint64
	// buf holds the key and value of the current entry, shared between
	// iterations to reduce allocations
	var buf, key, value []byte
	for {
		keyLen, err := binary.ReadUvarint(br)
		if err != nil {
			return readError(err)
		}
		if keyLen == 0 {
			break
		}
		if buf, key, err = readField(buf[:0], br, keyLen, maxKeyBytes); err != nil {
			return err
		}

		valueLen, err := binary.ReadUvarint(br)
		if err != nil {
			return readError(err)
		}
		if buf, value, err = readField(buf, br, valueLen, maxValueBytes); err != nil {
			return err
		}

		count++
		if !yield(importer.Entry{Key: key, Value: value}, nil) {
			return nil
		}
	}

	return readTrailer(br, count)
}

// readTrailer reads the trailer of a body after count entries, and returns
// how the part ended: nil for a complete part
func readTrailer(br *bufio.Reader, count uint64) error {
	status, err := br.ReadByte()
	if err != nil {
		return readError(err)
	}
	sent, err := binary.ReadUvarint(br)
	if err != nil {
		return readError(err)
	}
	if sent != count {
		return fmt.Errorf("%w: the sender wrote %d entries and %d were read", errCutOff, sent, count)
	}

	var field []byte
	switch status {
	case statusComplete:
	case statusPartial, statusTransient, statusPermanent:
		n, err := binary.ReadUvarint(br)
		if err != nil {
			return readError(err)
		}
		if _, field, err = readField(nil, br, n, maxTrailerFieldBytes); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unknown trailer status %d", errCutOff, status)
	}

	// The trailer ends the body
	if _, err := br.ReadByte(); err != io.EOF {
		if err != nil {
			return readError(err)
		}
		return fmt.Errorf("%w: data after the trailer", errCutOff)
	}

	switch status {
	case statusPartial:
		return &importer.PartialError{Resume: field}
	case statusTransient:
		return fmt.Errorf("the source failed the part: %s", field)
	case statusPermanent:
		return importer.PermanentError(fmt.Errorf("the source refused the part: %s", field))
	default:
		return nil
	}
}

// readField reads a field of n bytes from br and appends it to buf. It
// returns buf, which may have moved, and the field, a slice of it. A field
// read earlier into buf stays valid when buf moves, as the old array is not
// changed. A length over limit is an error.
func readField(
	buf []byte,
	br *bufio.Reader,
	n, limit uint64,
) (
	newBuf, field []byte,
	err error,
) {
	if n > limit {
		return buf, nil, fmt.Errorf("%w: a field of %d bytes, over the limit of %d", errCutOff, n, limit)
	}
	start := len(buf)
	buf = slices.Grow(buf, int(n))[:start+int(n)]
	if _, err := io.ReadFull(br, buf[start:]); err != nil {
		return buf, nil, readError(err)
	}
	return buf, buf[start:], nil
}

// readError returns the error of a failed read inside a body. The body
// must go on, so its end is a cut off.
func readError(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return errCutOff
	}
	return err
}
