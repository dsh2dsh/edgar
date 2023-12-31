// Code generated by mockery. DO NOT EDIT.

package repo

import (
	context "context"

	pgconn "github.com/jackc/pgx/v5/pgconn"
	mock "github.com/stretchr/testify/mock"

	pgx "github.com/jackc/pgx/v5"
)

// MockPostgreser is an autogenerated mock type for the Postgreser type
type MockPostgreser struct {
	mock.Mock
}

type MockPostgreser_Expecter struct {
	mock *mock.Mock
}

func (_m *MockPostgreser) EXPECT() *MockPostgreser_Expecter {
	return &MockPostgreser_Expecter{mock: &_m.Mock}
}

// Begin provides a mock function with given fields: ctx
func (_m *MockPostgreser) Begin(ctx context.Context) (pgx.Tx, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Begin")
	}

	var r0 pgx.Tx
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (pgx.Tx, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) pgx.Tx); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pgx.Tx)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockPostgreser_Begin_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Begin'
type MockPostgreser_Begin_Call struct {
	*mock.Call
}

// Begin is a helper method to define mock.On call
//   - ctx context.Context
func (_e *MockPostgreser_Expecter) Begin(ctx interface{}) *MockPostgreser_Begin_Call {
	return &MockPostgreser_Begin_Call{Call: _e.mock.On("Begin", ctx)}
}

func (_c *MockPostgreser_Begin_Call) Run(run func(ctx context.Context)) *MockPostgreser_Begin_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *MockPostgreser_Begin_Call) Return(_a0 pgx.Tx, _a1 error) *MockPostgreser_Begin_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockPostgreser_Begin_Call) RunAndReturn(run func(context.Context) (pgx.Tx, error)) *MockPostgreser_Begin_Call {
	_c.Call.Return(run)
	return _c
}

// CopyFrom provides a mock function with given fields: ctx, tableName, columnNames, rowSrc
func (_m *MockPostgreser) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	ret := _m.Called(ctx, tableName, columnNames, rowSrc)

	if len(ret) == 0 {
		panic("no return value specified for CopyFrom")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)); ok {
		return rf(ctx, tableName, columnNames, rowSrc)
	}
	if rf, ok := ret.Get(0).(func(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) int64); ok {
		r0 = rf(ctx, tableName, columnNames, rowSrc)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) error); ok {
		r1 = rf(ctx, tableName, columnNames, rowSrc)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockPostgreser_CopyFrom_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CopyFrom'
type MockPostgreser_CopyFrom_Call struct {
	*mock.Call
}

// CopyFrom is a helper method to define mock.On call
//   - ctx context.Context
//   - tableName pgx.Identifier
//   - columnNames []string
//   - rowSrc pgx.CopyFromSource
func (_e *MockPostgreser_Expecter) CopyFrom(ctx interface{}, tableName interface{}, columnNames interface{}, rowSrc interface{}) *MockPostgreser_CopyFrom_Call {
	return &MockPostgreser_CopyFrom_Call{Call: _e.mock.On("CopyFrom", ctx, tableName, columnNames, rowSrc)}
}

func (_c *MockPostgreser_CopyFrom_Call) Run(run func(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource)) *MockPostgreser_CopyFrom_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(pgx.Identifier), args[2].([]string), args[3].(pgx.CopyFromSource))
	})
	return _c
}

func (_c *MockPostgreser_CopyFrom_Call) Return(_a0 int64, _a1 error) *MockPostgreser_CopyFrom_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockPostgreser_CopyFrom_Call) RunAndReturn(run func(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)) *MockPostgreser_CopyFrom_Call {
	_c.Call.Return(run)
	return _c
}

// Exec provides a mock function with given fields: ctx, sql, arguments
func (_m *MockPostgreser) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	var _ca []interface{}
	_ca = append(_ca, ctx, sql)
	_ca = append(_ca, arguments...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Exec")
	}

	var r0 pgconn.CommandTag
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, ...interface{}) (pgconn.CommandTag, error)); ok {
		return rf(ctx, sql, arguments...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, ...interface{}) pgconn.CommandTag); ok {
		r0 = rf(ctx, sql, arguments...)
	} else {
		r0 = ret.Get(0).(pgconn.CommandTag)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, ...interface{}) error); ok {
		r1 = rf(ctx, sql, arguments...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockPostgreser_Exec_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Exec'
type MockPostgreser_Exec_Call struct {
	*mock.Call
}

// Exec is a helper method to define mock.On call
//   - ctx context.Context
//   - sql string
//   - arguments ...interface{}
func (_e *MockPostgreser_Expecter) Exec(ctx interface{}, sql interface{}, arguments ...interface{}) *MockPostgreser_Exec_Call {
	return &MockPostgreser_Exec_Call{Call: _e.mock.On("Exec",
		append([]interface{}{ctx, sql}, arguments...)...)}
}

func (_c *MockPostgreser_Exec_Call) Run(run func(ctx context.Context, sql string, arguments ...interface{})) *MockPostgreser_Exec_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]interface{}, len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(interface{})
			}
		}
		run(args[0].(context.Context), args[1].(string), variadicArgs...)
	})
	return _c
}

func (_c *MockPostgreser_Exec_Call) Return(_a0 pgconn.CommandTag, _a1 error) *MockPostgreser_Exec_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockPostgreser_Exec_Call) RunAndReturn(run func(context.Context, string, ...interface{}) (pgconn.CommandTag, error)) *MockPostgreser_Exec_Call {
	_c.Call.Return(run)
	return _c
}

// Query provides a mock function with given fields: ctx, sql, args
func (_m *MockPostgreser) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	var _ca []interface{}
	_ca = append(_ca, ctx, sql)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Query")
	}

	var r0 pgx.Rows
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, ...interface{}) (pgx.Rows, error)); ok {
		return rf(ctx, sql, args...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, ...interface{}) pgx.Rows); ok {
		r0 = rf(ctx, sql, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pgx.Rows)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, ...interface{}) error); ok {
		r1 = rf(ctx, sql, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockPostgreser_Query_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Query'
type MockPostgreser_Query_Call struct {
	*mock.Call
}

// Query is a helper method to define mock.On call
//   - ctx context.Context
//   - sql string
//   - args ...interface{}
func (_e *MockPostgreser_Expecter) Query(ctx interface{}, sql interface{}, args ...interface{}) *MockPostgreser_Query_Call {
	return &MockPostgreser_Query_Call{Call: _e.mock.On("Query",
		append([]interface{}{ctx, sql}, args...)...)}
}

func (_c *MockPostgreser_Query_Call) Run(run func(ctx context.Context, sql string, args ...interface{})) *MockPostgreser_Query_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]interface{}, len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(interface{})
			}
		}
		run(args[0].(context.Context), args[1].(string), variadicArgs...)
	})
	return _c
}

func (_c *MockPostgreser_Query_Call) Return(_a0 pgx.Rows, _a1 error) *MockPostgreser_Query_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockPostgreser_Query_Call) RunAndReturn(run func(context.Context, string, ...interface{}) (pgx.Rows, error)) *MockPostgreser_Query_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockPostgreser creates a new instance of MockPostgreser. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockPostgreser(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockPostgreser {
	mock := &MockPostgreser{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
