// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio-labs/conduit-connector-snowflake/source (interfaces: Iterator)
//
// Generated by this command:
//
//	mockgen -typed -destination=mock/iterator.go -package=mock . Iterator
//

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	sdk "github.com/conduitio/conduit-connector-sdk"
	gomock "go.uber.org/mock/gomock"
)

// MockIterator is a mock of Iterator interface.
type MockIterator struct {
	ctrl     *gomock.Controller
	recorder *MockIteratorMockRecorder
}

// MockIteratorMockRecorder is the mock recorder for MockIterator.
type MockIteratorMockRecorder struct {
	mock *MockIterator
}

// NewMockIterator creates a new mock instance.
func NewMockIterator(ctrl *gomock.Controller) *MockIterator {
	mock := &MockIterator{ctrl: ctrl}
	mock.recorder = &MockIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIterator) EXPECT() *MockIteratorMockRecorder {
	return m.recorder
}

// Ack mocks base method.
func (m *MockIterator) Ack(arg0 context.Context, arg1 sdk.Position) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ack", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Ack indicates an expected call of Ack.
func (mr *MockIteratorMockRecorder) Ack(arg0, arg1 any) *MockIteratorAckCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockIterator)(nil).Ack), arg0, arg1)
	return &MockIteratorAckCall{Call: call}
}

// MockIteratorAckCall wrap *gomock.Call
type MockIteratorAckCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockIteratorAckCall) Return(arg0 error) *MockIteratorAckCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockIteratorAckCall) Do(f func(context.Context, sdk.Position) error) *MockIteratorAckCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockIteratorAckCall) DoAndReturn(f func(context.Context, sdk.Position) error) *MockIteratorAckCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// HasNext mocks base method.
func (m *MockIterator) HasNext(arg0 context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasNext", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HasNext indicates an expected call of HasNext.
func (mr *MockIteratorMockRecorder) HasNext(arg0 any) *MockIteratorHasNextCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasNext", reflect.TypeOf((*MockIterator)(nil).HasNext), arg0)
	return &MockIteratorHasNextCall{Call: call}
}

// MockIteratorHasNextCall wrap *gomock.Call
type MockIteratorHasNextCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockIteratorHasNextCall) Return(arg0 bool, arg1 error) *MockIteratorHasNextCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockIteratorHasNextCall) Do(f func(context.Context) (bool, error)) *MockIteratorHasNextCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockIteratorHasNextCall) DoAndReturn(f func(context.Context) (bool, error)) *MockIteratorHasNextCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Next mocks base method.
func (m *MockIterator) Next(arg0 context.Context) (sdk.Record, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next", arg0)
	ret0, _ := ret[0].(sdk.Record)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Next indicates an expected call of Next.
func (mr *MockIteratorMockRecorder) Next(arg0 any) *MockIteratorNextCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockIterator)(nil).Next), arg0)
	return &MockIteratorNextCall{Call: call}
}

// MockIteratorNextCall wrap *gomock.Call
type MockIteratorNextCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockIteratorNextCall) Return(arg0 sdk.Record, arg1 error) *MockIteratorNextCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockIteratorNextCall) Do(f func(context.Context) (sdk.Record, error)) *MockIteratorNextCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockIteratorNextCall) DoAndReturn(f func(context.Context) (sdk.Record, error)) *MockIteratorNextCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Stop mocks base method.
func (m *MockIterator) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockIteratorMockRecorder) Stop() *MockIteratorStopCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockIterator)(nil).Stop))
	return &MockIteratorStopCall{Call: call}
}

// MockIteratorStopCall wrap *gomock.Call
type MockIteratorStopCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockIteratorStopCall) Return(arg0 error) *MockIteratorStopCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockIteratorStopCall) Do(f func() error) *MockIteratorStopCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockIteratorStopCall) DoAndReturn(f func() error) *MockIteratorStopCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
