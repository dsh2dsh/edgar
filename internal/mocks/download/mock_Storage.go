// Code generated by mockery. DO NOT EDIT.

package download

import (
	io "io"

	mock "github.com/stretchr/testify/mock"
)

// MockStorage is an autogenerated mock type for the Storage type
type MockStorage struct {
	mock.Mock
}

type MockStorage_Expecter struct {
	mock *mock.Mock
}

func (_m *MockStorage) EXPECT() *MockStorage_Expecter {
	return &MockStorage_Expecter{mock: &_m.Mock}
}

// Save provides a mock function with given fields: path, fname, r
func (_m *MockStorage) Save(path string, fname string, r io.Reader) error {
	ret := _m.Called(path, fname, r)

	if len(ret) == 0 {
		panic("no return value specified for Save")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, io.Reader) error); ok {
		r0 = rf(path, fname, r)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockStorage_Save_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Save'
type MockStorage_Save_Call struct {
	*mock.Call
}

// Save is a helper method to define mock.On call
//   - path string
//   - fname string
//   - r io.Reader
func (_e *MockStorage_Expecter) Save(path interface{}, fname interface{}, r interface{}) *MockStorage_Save_Call {
	return &MockStorage_Save_Call{Call: _e.mock.On("Save", path, fname, r)}
}

func (_c *MockStorage_Save_Call) Run(run func(path string, fname string, r io.Reader)) *MockStorage_Save_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(io.Reader))
	})
	return _c
}

func (_c *MockStorage_Save_Call) Return(_a0 error) *MockStorage_Save_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStorage_Save_Call) RunAndReturn(run func(string, string, io.Reader) error) *MockStorage_Save_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockStorage creates a new instance of MockStorage. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockStorage(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockStorage {
	mock := &MockStorage{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}