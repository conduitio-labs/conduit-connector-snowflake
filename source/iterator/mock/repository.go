// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio-labs/conduit-connector-snowflake/source/iterator (interfaces: Repository)
//
// Generated by this command:
//
//	mockgen -typed -destination=mock/repository.go -package=mock . Repository
//

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	position "github.com/conduitio-labs/conduit-connector-snowflake/source/position"
	sqlx "github.com/jmoiron/sqlx"
	gomock "go.uber.org/mock/gomock"
)

// MockRepository is a mock of Repository interface.
type MockRepository struct {
	ctrl     *gomock.Controller
	recorder *MockRepositoryMockRecorder
}

// MockRepositoryMockRecorder is the mock recorder for MockRepository.
type MockRepositoryMockRecorder struct {
	mock *MockRepository
}

// NewMockRepository creates a new mock instance.
func NewMockRepository(ctrl *gomock.Controller) *MockRepository {
	mock := &MockRepository{ctrl: ctrl}
	mock.recorder = &MockRepositoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRepository) EXPECT() *MockRepositoryMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockRepository) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockRepositoryMockRecorder) Close() *MockRepositoryCloseCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRepository)(nil).Close))
	return &MockRepositoryCloseCall{Call: call}
}

// MockRepositoryCloseCall wrap *gomock.Call
type MockRepositoryCloseCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockRepositoryCloseCall) Return(arg0 error) *MockRepositoryCloseCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockRepositoryCloseCall) Do(f func() error) *MockRepositoryCloseCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockRepositoryCloseCall) DoAndReturn(f func() error) *MockRepositoryCloseCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// CreateStream mocks base method.
func (m *MockRepository) CreateStream(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateStream", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateStream indicates an expected call of CreateStream.
func (mr *MockRepositoryMockRecorder) CreateStream(arg0, arg1, arg2 any) *MockRepositoryCreateStreamCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateStream", reflect.TypeOf((*MockRepository)(nil).CreateStream), arg0, arg1, arg2)
	return &MockRepositoryCreateStreamCall{Call: call}
}

// MockRepositoryCreateStreamCall wrap *gomock.Call
type MockRepositoryCreateStreamCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockRepositoryCreateStreamCall) Return(arg0 error) *MockRepositoryCreateStreamCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockRepositoryCreateStreamCall) Do(f func(context.Context, string, string) error) *MockRepositoryCreateStreamCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockRepositoryCreateStreamCall) DoAndReturn(f func(context.Context, string, string) error) *MockRepositoryCreateStreamCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// CreateTrackingTable mocks base method.
func (m *MockRepository) CreateTrackingTable(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTrackingTable", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateTrackingTable indicates an expected call of CreateTrackingTable.
func (mr *MockRepositoryMockRecorder) CreateTrackingTable(arg0, arg1, arg2 any) *MockRepositoryCreateTrackingTableCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTrackingTable", reflect.TypeOf((*MockRepository)(nil).CreateTrackingTable), arg0, arg1, arg2)
	return &MockRepositoryCreateTrackingTableCall{Call: call}
}

// MockRepositoryCreateTrackingTableCall wrap *gomock.Call
type MockRepositoryCreateTrackingTableCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockRepositoryCreateTrackingTableCall) Return(arg0 error) *MockRepositoryCreateTrackingTableCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockRepositoryCreateTrackingTableCall) Do(f func(context.Context, string, string) error) *MockRepositoryCreateTrackingTableCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockRepositoryCreateTrackingTableCall) DoAndReturn(f func(context.Context, string, string) error) *MockRepositoryCreateTrackingTableCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// GetMaxValue mocks base method.
func (m *MockRepository) GetMaxValue(arg0 context.Context, arg1, arg2 string) (any, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMaxValue", arg0, arg1, arg2)
	ret0, _ := ret[0].(any)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMaxValue indicates an expected call of GetMaxValue.
func (mr *MockRepositoryMockRecorder) GetMaxValue(arg0, arg1, arg2 any) *MockRepositoryGetMaxValueCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMaxValue", reflect.TypeOf((*MockRepository)(nil).GetMaxValue), arg0, arg1, arg2)
	return &MockRepositoryGetMaxValueCall{Call: call}
}

// MockRepositoryGetMaxValueCall wrap *gomock.Call
type MockRepositoryGetMaxValueCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockRepositoryGetMaxValueCall) Return(arg0 any, arg1 error) *MockRepositoryGetMaxValueCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockRepositoryGetMaxValueCall) Do(f func(context.Context, string, string) (any, error)) *MockRepositoryGetMaxValueCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockRepositoryGetMaxValueCall) DoAndReturn(f func(context.Context, string, string) (any, error)) *MockRepositoryGetMaxValueCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// GetPrimaryKeys mocks base method.
func (m *MockRepository) GetPrimaryKeys(arg0 context.Context, arg1 string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPrimaryKeys", arg0, arg1)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPrimaryKeys indicates an expected call of GetPrimaryKeys.
func (mr *MockRepositoryMockRecorder) GetPrimaryKeys(arg0, arg1 any) *MockRepositoryGetPrimaryKeysCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPrimaryKeys", reflect.TypeOf((*MockRepository)(nil).GetPrimaryKeys), arg0, arg1)
	return &MockRepositoryGetPrimaryKeysCall{Call: call}
}

// MockRepositoryGetPrimaryKeysCall wrap *gomock.Call
type MockRepositoryGetPrimaryKeysCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockRepositoryGetPrimaryKeysCall) Return(arg0 []string, arg1 error) *MockRepositoryGetPrimaryKeysCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockRepositoryGetPrimaryKeysCall) Do(f func(context.Context, string) ([]string, error)) *MockRepositoryGetPrimaryKeysCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockRepositoryGetPrimaryKeysCall) DoAndReturn(f func(context.Context, string) ([]string, error)) *MockRepositoryGetPrimaryKeysCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// GetRows mocks base method.
func (m *MockRepository) GetRows(arg0 context.Context, arg1, arg2 string, arg3 []string, arg4 *position.Position, arg5 any, arg6 int) (*sqlx.Rows, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRows", arg0, arg1, arg2, arg3, arg4, arg5, arg6)
	ret0, _ := ret[0].(*sqlx.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetRows indicates an expected call of GetRows.
func (mr *MockRepositoryMockRecorder) GetRows(arg0, arg1, arg2, arg3, arg4, arg5, arg6 any) *MockRepositoryGetRowsCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRows", reflect.TypeOf((*MockRepository)(nil).GetRows), arg0, arg1, arg2, arg3, arg4, arg5, arg6)
	return &MockRepositoryGetRowsCall{Call: call}
}

// MockRepositoryGetRowsCall wrap *gomock.Call
type MockRepositoryGetRowsCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockRepositoryGetRowsCall) Return(arg0 *sqlx.Rows, arg1 error) *MockRepositoryGetRowsCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockRepositoryGetRowsCall) Do(f func(context.Context, string, string, []string, *position.Position, any, int) (*sqlx.Rows, error)) *MockRepositoryGetRowsCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockRepositoryGetRowsCall) DoAndReturn(f func(context.Context, string, string, []string, *position.Position, any, int) (*sqlx.Rows, error)) *MockRepositoryGetRowsCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// GetTrackingData mocks base method.
func (m *MockRepository) GetTrackingData(arg0 context.Context, arg1, arg2 string, arg3 []string, arg4, arg5 int) ([]map[string]any, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTrackingData", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].([]map[string]any)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTrackingData indicates an expected call of GetTrackingData.
func (mr *MockRepositoryMockRecorder) GetTrackingData(arg0, arg1, arg2, arg3, arg4, arg5 any) *MockRepositoryGetTrackingDataCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTrackingData", reflect.TypeOf((*MockRepository)(nil).GetTrackingData), arg0, arg1, arg2, arg3, arg4, arg5)
	return &MockRepositoryGetTrackingDataCall{Call: call}
}

// MockRepositoryGetTrackingDataCall wrap *gomock.Call
type MockRepositoryGetTrackingDataCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockRepositoryGetTrackingDataCall) Return(arg0 []map[string]any, arg1 error) *MockRepositoryGetTrackingDataCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockRepositoryGetTrackingDataCall) Do(f func(context.Context, string, string, []string, int, int) ([]map[string]any, error)) *MockRepositoryGetTrackingDataCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockRepositoryGetTrackingDataCall) DoAndReturn(f func(context.Context, string, string, []string, int, int) ([]map[string]any, error)) *MockRepositoryGetTrackingDataCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}