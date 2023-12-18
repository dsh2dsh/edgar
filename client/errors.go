package client

import (
	"errors"
	"fmt"
	"net/http"
)

const maxExpectedStatusCode = 299

var ErrUnexpectedStatus = fmt.Errorf("unexpected status code (>%d)",
	maxExpectedStatusCode)

func newUnexpectedStatusError(resp *http.Response) error {
	return errors.Join(
		&UnexpectedStatusError{
			httpStatus:     resp.Status,
			httpStatusCode: resp.StatusCode,
		}, ErrUnexpectedStatus,
	)
}

type UnexpectedStatusError struct {
	httpStatus     string
	httpStatusCode int
}

func (self *UnexpectedStatusError) Error() string {
	return fmt.Sprintf("%d (%v)", self.httpStatusCode, self.httpStatus)
}

func (self *UnexpectedStatusError) Is(target error) bool {
	_, ok := target.(*UnexpectedStatusError)
	return ok
}

func (self *UnexpectedStatusError) StatusCode() int {
	return self.httpStatusCode
}
