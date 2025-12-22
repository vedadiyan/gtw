package gtw

import (
	"errors"
	"fmt"
	"io"
)

type multipleReadCloser struct {
	readers []io.ReadCloser
}

func (x *multipleReadCloser) Close() error {
	var err error
	for _, rc := range x.readers {
		err = errors.Join(err, rc.Close())
	}
	x.readers = nil
	return err
}

func (x *multipleReadCloser) Read(p []byte) (int, error) {
	for len(x.readers) != 0 {
		rc := x.readers[0]
		n, err := rc.Read(p)
		switch err {
		case nil:
			{
				return n, nil
			}
		case io.EOF:
			{
				if err := rc.Close(); err != nil {
					return n, err
				}
				x.readers = x.readers[1:]
			}
		default:
			{
				return n, err
			}
		}

	}
	return 0, io.EOF
}

func (x *multipleReadCloser) Remove() (io.ReadCloser, error) {
	if len(x.readers) == 0 {
		return nil, fmt.Errorf("there is no more reader left")
	}
	r := x.readers[0]
	x.readers = x.readers[1:]
	return r, nil
}

func (x *multipleReadCloser) Len() int {
	return len(x.readers)
}

func (x *multipleReadCloser) add(r io.ReadCloser) {
	if inner, ok := r.(*multipleReadCloser); ok {
		for _, r := range inner.readers {
			x.add(r)
		}
		return
	}
	x.readers = append(x.readers, r)
}

func MultipleReadCloser(r ...io.ReadCloser) *multipleReadCloser {
	out := new(multipleReadCloser)
	for _, r := range r {
		if r == nil {
			continue
		}
		out.add(r)
	}
	return out
}
