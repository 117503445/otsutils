// Package otsutils provides utilities for working with Alibaba Cloud Tablestore (OTS).
package otsutils

import "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

// PutRowParams contains parameters for the PutRow operation.
type PutRowParams struct {
	// RowExistenceExpectation specifies the row existence expectation for the operation.
	RowExistenceExpectation *tablestore.RowExistenceExpectation
}

// GetRowParams contains parameters for the GetRow operation.
type GetRowParams struct {
}

// UpdateRowParams contains parameters for the UpdateRow operation.
type UpdateRowParams struct {
	// RowExistenceExpectation specifies the row existence expectation for the operation.
	RowExistenceExpectation *tablestore.RowExistenceExpectation
	
	// DeletedColumns is a list of column names to delete.
	DeletedColumns []string
	
	// UpdatedColumns is a map of column names to values to update or add.
	UpdatedColumns map[string]any
}