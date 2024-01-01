package client

import (
	"fmt"
	"net/http"
)

const maxExpectedStatusCode = 299

var ErrUnexpectedStatus = &UnexpectedStatusError{}

func newUnexpectedStatusError(resp *http.Response) error {
	return &UnexpectedStatusError{
		httpStatus:     resp.Status,
		httpStatusCode: resp.StatusCode,
	}
}

type UnexpectedStatusError struct {
	httpStatus     string
	httpStatusCode int
}

func (self *UnexpectedStatusError) Error() string {
	return fmt.Sprintf("unexpected status code (>%v): %v",
		maxExpectedStatusCode, self.Status())
}

func (self *UnexpectedStatusError) Is(target error) bool {
	_, ok := target.(*UnexpectedStatusError)
	return ok
}

func (self *UnexpectedStatusError) Status() string {
	return self.httpStatus
}

func (self *UnexpectedStatusError) StatusCode() int {
	return self.httpStatusCode
}
