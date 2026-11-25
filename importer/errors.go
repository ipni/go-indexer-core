package importer

import (
	"errors"
	"fmt"
)

// PermanentError marks err as one that a retry cannot fix, so the import
// stops. A Source returns it to refuse a part, for example when it is
// frozen, and a Target for an entry it cannot write. An error that is not
// marked is transient: the part is retried. PermanentError(nil) is nil.
func PermanentError(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or an error it wraps, was marked
// with PermanentError
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }

func (e *permanentError) Unwrap() error { return e.err }

// PartialError ends a part's entries before the part's end. The part is
// not done: the rest of it is read from Resume, up to the same end.
//
// It is the last element that a Source's entries yield when the source
// stops a part early, so that one request reads a bounded amount of data
// however unevenly the data spreads over the keys. Resume is a cursor after
// every key the entries yielded and not after any key they did not.
type PartialError struct{ Resume []byte }

func (e *PartialError) Error() string {
	return fmt.Sprintf("the part stopped early; it resumes from %x", e.Resume)
}

// ErrWireEncoding is the error of an entry that breaks its kind's wire
// encoding. A Target wraps it, marked with PermanentError, when it refuses an
// entry.
var ErrWireEncoding = errors.New("entry breaks the wire encoding")
