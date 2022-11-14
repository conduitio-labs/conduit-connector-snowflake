// Code generated by MockGen. DO NOT EDIT.
// Source: source/iterator/interface.go

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	position "github.com/conduitio-labs/conduit-connector-snowflake/source/position"
	gomock "github.com/golang/mock/gomock"
	sqlx "github.com/jmoiron/sqlx"
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
func (mr *MockRepositoryMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRepository)(nil).Close))
}

// CreateStream mocks base method.
func (m *MockRepository) CreateStream(ctx context.Context, stream, table string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateStream", ctx, stream, table)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateStream indicates an expected call of CreateStream.
func (mr *MockRepositoryMockRecorder) CreateStream(ctx, stream, table interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateStream", reflect.TypeOf((*MockRepository)(nil).CreateStream), ctx, stream, table)
}

// CreateTrackingTable mocks base method.
func (m *MockRepository) CreateTrackingTable(ctx context.Context, trackingTable, table string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTrackingTable", ctx, trackingTable, table)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateTrackingTable indicates an expected call of CreateTrackingTable.
func (mr *MockRepositoryMockRecorder) CreateTrackingTable(ctx, trackingTable, table interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTrackingTable", reflect.TypeOf((*MockRepository)(nil).CreateTrackingTable), ctx, trackingTable, table)
}

// GetMaxValue mocks base method.
func (m *MockRepository) GetMaxValue(ctx context.Context, table, orderingColumn string) (any, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMaxValue", ctx, table, orderingColumn)
	ret0, _ := ret[0].(any)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMaxValue indicates an expected call of GetMaxValue.
func (mr *MockRepositoryMockRecorder) GetMaxValue(ctx, table, orderingColumn interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMaxValue", reflect.TypeOf((*MockRepository)(nil).GetMaxValue), ctx, table, orderingColumn)
}

// GetRows mocks base method.
func (m *MockRepository) GetRows(ctx context.Context, table, orderingColumn string, fields []string, pos *position.Position, maxValue any, limit int) (*sqlx.Rows, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRows", ctx, table, orderingColumn, fields, pos, maxValue, limit)
	ret0, _ := ret[0].(*sqlx.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetRows indicates an expected call of GetRows.
func (mr *MockRepositoryMockRecorder) GetRows(ctx, table, orderingColumn, fields, pos, maxValue, limit interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRows", reflect.TypeOf((*MockRepository)(nil).GetRows), ctx, table, orderingColumn, fields, pos, maxValue, limit)
}

// GetTrackingData mocks base method.
func (m *MockRepository) GetTrackingData(ctx context.Context, stream, trackingTable string, fields []string, offset, limit int) ([]map[string]interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTrackingData", ctx, stream, trackingTable, fields, offset, limit)
	ret0, _ := ret[0].([]map[string]interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTrackingData indicates an expected call of GetTrackingData.
func (mr *MockRepositoryMockRecorder) GetTrackingData(ctx, stream, trackingTable, fields, offset, limit interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTrackingData", reflect.TypeOf((*MockRepository)(nil).GetTrackingData), ctx, stream, trackingTable, fields, offset, limit)
}
