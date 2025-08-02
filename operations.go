// Package otsutils provides utilities for working with Alibaba Cloud Tablestore (OTS).
package otsutils

import (
	"context"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/rs/zerolog"
)

// PutRow inserts a row into the table.
// The obj parameter should be a pointer to a struct with fields tagged with "json" and optionally "pk".
// Fields tagged with "pk" are treated as primary key columns, others are treated as attribute columns.
//
// Example usage:
//
//	type MyRow struct {
//	    PK1 *string `json:"pk1" pk:"1"`
//	    Col1 *string `json:"col1"`
//	}
//
//	row := MyRow{
//	    PK1: tea.String("pk1value"),
//	    Col1: tea.String("col1value"),
//	}
//	err := PutRow(ctx, &row)
func PutRow(ctx context.Context, obj any, params ...PutRowParams) error {
	buildReq := func(otsParams *OtsUtilsParams, logger *zerolog.Logger, obj any, params ...any) (any, error) {
		rowExistenceExpectation := tablestore.RowExistenceExpectation_EXPECT_NOT_EXIST
		if len(params) > 0 {
			if p, ok := params[0].(PutRowParams); ok && p.RowExistenceExpectation != nil {
				rowExistenceExpectation = *p.RowExistenceExpectation
			}
		}

		putRowChange := &tablestore.PutRowChange{
			TableName:  otsParams.TableName,
			PrimaryKey: &tablestore.PrimaryKey{},
		}
		putRowChange.SetCondition(rowExistenceExpectation)

		pks, cols, err := ParseObj(ctx, obj)
		if err != nil {
			return nil, err
		}

		for k, v := range pks {
			putRowChange.PrimaryKey.AddPrimaryKeyColumn(k, v)
		}
		for k, v := range cols {
			putRowChange.AddColumn(k, v)
		}

		return &tablestore.PutRowRequest{PutRowChange: putRowChange}, nil
	}

	execute := func(client *tablestore.TableStoreClient, req any) (any, error) {
		return client.PutRow(req.(*tablestore.PutRowRequest))
	}

	// PutRow does not need to handle response data
	return executeOTSOperation(ctx, "PutRow", obj, buildReq, execute, nil, toAnySlice(params)...)
}

// UpdateRow updates a row in the table.
// The obj parameter should be a pointer to a struct with fields tagged with "json" and "pk".
// Fields tagged with "pk" are treated as primary key columns and used to locate the row.
// Other fields in the struct are treated as attribute columns to update or add.
//
// Example usage:
//
//	type MyRow struct {
//	    PK1 *string `json:"pk1" pk:"1"`
//	    Col1 *string `json:"col1"`
//	    Col2 *int64 `json:"col2"`
//	}
//
//	row := MyRow{
//	    PK1: tea.String("pk1value"),
//	    Col1: tea.String("newcol1value"),
//	    Col2: tea.Int64(42),
//	}
//
//	expectExist := tablestore.RowExistenceExpectation_EXPECT_EXIST
//	err := UpdateRow(ctx, &row, UpdateRowParams{
//	    RowExistenceExpectation: &expectExist,
//	    DeletedColumns: []string{"old_column"},
//	})
func UpdateRow(ctx context.Context, obj any, params ...UpdateRowParams) error {
	buildReq := func(otsParams *OtsUtilsParams, logger *zerolog.Logger, obj any, params ...any) (any, error) {
		rowExistenceExpectation := tablestore.RowExistenceExpectation_IGNORE
		var deletedColumns []string
		var updatedColumns map[string]any

		if len(params) > 0 {
			if p, ok := params[0].(UpdateRowParams); ok {
				if p.RowExistenceExpectation != nil {
					rowExistenceExpectation = *p.RowExistenceExpectation
				}
				deletedColumns = p.DeletedColumns
				updatedColumns = p.UpdatedColumns
			}
		}

		logger.Debug().Interface("rowExistenceExpectation", rowExistenceExpectation).Send()

		updateRowChange := &tablestore.UpdateRowChange{
			TableName:  otsParams.TableName,
			PrimaryKey: &tablestore.PrimaryKey{},
		}
		updateRowChange.SetCondition(rowExistenceExpectation)

		pks, cols, err := ParseObj(ctx, obj)
		if err != nil {
			return nil, err
		}

		for k, v := range pks {
			updateRowChange.PrimaryKey.AddPrimaryKeyColumn(k, v)
		}

		// Process deleted columns
		for _, colName := range deletedColumns {
			updateRowChange.DeleteColumn(colName)
		}

		// Process updated/added columns
		for colName, value := range updatedColumns {
			updateRowChange.PutColumn(colName, value)
		}

		// Process columns extracted from obj (except primary key columns)
		for k, v := range cols {
			updateRowChange.PutColumn(k, v)
		}

		return &tablestore.UpdateRowRequest{UpdateRowChange: updateRowChange}, nil
	}

	execute := func(client *tablestore.TableStoreClient, req any) (any, error) {
		return client.UpdateRow(req.(*tablestore.UpdateRowRequest))
	}

	// UpdateRow does not need special response handling
	return executeOTSOperation(ctx, "UpdateRow", obj, buildReq, execute, nil, toAnySlice(params)...)
}

// GetRow retrieves a row from the table.
// The obj parameter should be a pointer to a struct with fields tagged with "json" and "pk".
// Fields tagged with "pk" are used to locate the row, and other fields are populated with the retrieved values.
//
// Example usage:
//
//	type MyRow struct {
//	    PK1 *string `json:"pk1" pk:"1"`
//	    Col1 *string `json:"col1"`
//	    Col2 *int64 `json:"col2"`
//	}
//
//	row := MyRow{
//	    PK1: tea.String("pk1value"),
//	}
//	err := GetRow(ctx, &row)
//	if err == nil {
//	    // row.Col1 and row.Col2 are now populated with values from the table
//	}
func GetRow(ctx context.Context, obj any, params ...GetRowParams) error {
	buildReq := func(otsParams *OtsUtilsParams, logger *zerolog.Logger, obj any, params ...any) (any, error) {
		criteria := &tablestore.SingleRowQueryCriteria{
			TableName:  otsParams.TableName,
			MaxVersion: 1,
			PrimaryKey: &tablestore.PrimaryKey{},
		}

		pks, _, err := ParseObj(ctx, obj)
		if err != nil {
			return nil, err
		}
		for k, v := range pks {
			criteria.PrimaryKey.AddPrimaryKeyColumn(k, v)
		}

		return &tablestore.GetRowRequest{SingleRowQueryCriteria: criteria}, nil
	}

	execute := func(client *tablestore.TableStoreClient, req any) (any, error) {
		return client.GetRow(req.(*tablestore.GetRowRequest))
	}

	handleResp := func(logger *zerolog.Logger, resp any, obj any) error {
		getResp := resp.(*tablestore.GetRowResponse)

		pks := make(map[string]any)
		for _, pk := range getResp.PrimaryKey.PrimaryKeys {
			pks[pk.ColumnName] = pk.Value
		}

		cols := make(map[string]any)
		for _, col := range getResp.Columns {
			cols[col.ColumnName] = col.Value
		}

		return ParseResult(ctx, obj, pks, cols)
	}

	return executeOTSOperation(ctx, "GetRow", obj, buildReq, execute, handleResp, toAnySlice(params)...)
}
