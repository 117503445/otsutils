// Package otsutils provides utilities for working with Alibaba Cloud Tablestore (OTS).
package otsutils

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type otsUtilsParamsCtxKey struct{}

// NewClient creates a new TableStore client with the provided credentials.
// It will panic if any of the required parameters are empty.
func NewClient(ctx context.Context, endPoint, instanceName, accessKeyId, accessKeySecret string) *tablestore.TableStoreClient {
	logger := log.Ctx(ctx)

	if endPoint == "" || instanceName == "" || accessKeyId == "" || accessKeySecret == "" {
		logger.Panic().Msg("endPoint, instanceName, accessKeyId, accessKeySecret can not be empty")
	}
	return tablestore.NewClient(endPoint, instanceName, accessKeyId, accessKeySecret)
}

type OtsUtilsParams struct {
	Client    *tablestore.TableStoreClient
	TableName string
}

// WithContext adds the OtsUtilsParams to the context.
// It will panic if TableName or Client are not set.
func (otsUtilsParams *OtsUtilsParams) WithContext(ctx context.Context) context.Context {
	logger := log.Ctx(ctx)

	if otsUtilsParams.TableName == "" {
		logger.Panic().Msg("TableName can not be empty")
	}
	if otsUtilsParams.Client == nil {
		logger.Panic().Msg("Client can not be nil")
	}

	return context.WithValue(ctx, otsUtilsParamsCtxKey{}, otsUtilsParams)
}

// OtsUtilsParamsFromCtx retrieves the OtsUtilsParams from the context.
func OtsUtilsParamsFromCtx(ctx context.Context) *OtsUtilsParams {
	otsUtilsParams := ctx.Value(otsUtilsParamsCtxKey{})
	return otsUtilsParams.(*OtsUtilsParams)
}

func otsUtilsParamsFromCtx(ctx context.Context) *OtsUtilsParams {
	logger := log.Ctx(ctx)

	otsUtilsParams := OtsUtilsParamsFromCtx(ctx)
	if otsUtilsParams == nil {
		logger.Panic().Msg("OtsUtilsParams can not be nil")
	}

	return otsUtilsParams
}

func parseObj(ctx context.Context, obj any) (pks map[string]any, cols map[string]any, err error) {
	logger := log.Ctx(ctx)
	logger.Debug().Discard().Interface("obj", obj).Send()

	pks = make(map[string]any)
	cols = make(map[string]any)

	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr {
		return nil, nil, fmt.Errorf("obj must be a pointer")
	}

	t := reflect.TypeOf(obj)

	// If it's a pointer, dereference it
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	// Ensure it's a struct
	if v.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("obj must be a struct or pointer to struct")
	}

	// Iterate through all fields
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		isValidPointerType := func(t reflect.Type) bool {
			// Must be a pointer
			if t.Kind() != reflect.Ptr {
				return false
			}
			// The pointed-to type must be string, int64, or []byte
			elem := t.Elem()
			switch elem.Kind() {
			case reflect.String:
				return true
			case reflect.Int64:
				return true
			case reflect.Slice:
				return elem.Elem().Kind() == reflect.Uint8 // []byte is []uint8
			default:
				return false
			}
		}
		// Check if field type is valid
		if !isValidPointerType(field.Type()) {
			return nil, nil, fmt.Errorf("field %s has invalid type: %s. Only *string, *int64, and *[]byte are allowed", fieldType.Name, field.Type())
		}

		// If it's a pointer and is nil, skip
		if field.IsNil() {
			continue // Note: continue here, not participating in PutRow
		}

		jsonTag := fieldType.Tag.Get("json")
		pkTag := fieldType.Tag.Get("pk")

		// logger.Debug().Str("jsonTag", jsonTag).Str("pkTag", pkTag).Send()

		value := field.Elem().Interface()

		// Check if it's a primary key
		isPk := pkTag != ""

		// Add to corresponding place based on whether it's a primary key
		// if isPk {
		// 	putPk.AddPrimaryKeyColumn(jsonTag, value)
		// } else {
		// 	putRowChange.AddColumn(jsonTag, value)
		// }
		if isPk {
			pks[jsonTag] = value
		} else {
			cols[jsonTag] = value
		}
	}
	return pks, cols, nil
}

func parseResult(ctx context.Context, obj any, pks map[string]any, cols map[string]any) error {
	logger := log.Ctx(ctx)
	logger.Debug().Discard().Interface("obj", obj).Interface("pks", pks).Interface("cols", cols).Send()

	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("parseResult: obj must be a non-nil pointer to struct, got %T", obj)
	}
	v = v.Elem()
	t := v.Type()

	if v.Kind() != reflect.Struct {
		return fmt.Errorf("parseResult: obj must be a pointer to struct, got %s", t.Name())
	}

	// Internal function: type mismatch error
	typeMismatchError := func(fieldType, value any) error {
		return fmt.Errorf("expected %v, but got %T", fieldType, value)
	}

	// Internal function: assign to pointer field
	assignToPointerField := func(field reflect.Value, value any) error {
		if field.Kind() != reflect.Ptr {
			return fmt.Errorf("field is not a pointer, got %s", field.Kind())
		}

		elemType := field.Type().Elem()

		switch elemType.Kind() {
		case reflect.String:
			if v, ok := value.(string); ok {
				newVal := reflect.New(elemType)
				newVal.Elem().SetString(v)
				field.Set(newVal)
			} else {
				return typeMismatchError("string", value)
			}

		case reflect.Int64:
			if v, ok := value.(int64); ok {
				newVal := reflect.New(elemType)
				newVal.Elem().SetInt(v)
				field.Set(newVal)
			} else {
				return typeMismatchError("int64", value)
			}

		case reflect.Slice:
			if elemType.Elem().Kind() == reflect.Uint8 { // []byte
				if v, ok := value.([]byte); ok {
					newVal := reflect.New(elemType)
					newVal.Elem().SetBytes(v)
					field.Set(newVal)
				} else {
					return typeMismatchError("[]byte", value)
				}
			} else {
				return fmt.Errorf("unsupported slice element type: %s", elemType)
			}

		default:
			return fmt.Errorf("unsupported field type: %s", elemType.Kind())
		}

		return nil
	}

	// Build json tag to field mapping
	fieldMap := make(map[string]reflect.Value)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		ft := t.Field(i)

		if !field.CanSet() {
			continue
		}

		jsonTag := ft.Tag.Get("json")
		if jsonTag == "" {
			continue
		}

		// Remove modifiers like ,omitempty
		if idx := strings.Index(jsonTag, ","); idx != -1 {
			jsonTag = jsonTag[:idx]
		}

		fieldMap[jsonTag] = field
	}

	// Process primary keys
	for colName, value := range pks {
		if field, ok := fieldMap[colName]; ok {
			if err := assignToPointerField(field, value); err != nil {
				return fmt.Errorf("primary key %q: %w", colName, err)
			}
		}
	}

	// Process regular columns
	for colName, value := range cols {
		if field, ok := fieldMap[colName]; ok {
			if err := assignToPointerField(field, value); err != nil {
				return fmt.Errorf("column %q: %w", colName, err)
			}
		}
	}

	return nil
}

// executeOTSOperation is a generic OTS operation execution function
func executeOTSOperation(
	ctx context.Context,
	operation string,
	obj any,
	buildRequest func(*OtsUtilsParams, *zerolog.Logger, any, ...any) (any, error),
	execute func(*tablestore.TableStoreClient, any) (any, error),
	handleResponse func(*zerolog.Logger, any, any) error,
	params ...any,
) error {
	logger := zerolog.Ctx(ctx).With().Str("operation", operation).CallerWithSkipFrameCount(4).Logger()
	otsParams := otsUtilsParamsFromCtx(ctx)

	{
		e := logger.Debug().Interface("obj", obj)
		if len(params) > 0 {
			e = e.Interface("params", params[0])
		}
		e.Msg("Executing OTS operation")
	}

	// Build request
	req, err := buildRequest(otsParams, &logger, obj, params...)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to build request")
		return err
	}

	logger.Debug().Interface("request", req).Msg("Request built")

	// Execute request
	resp, err := execute(otsParams.Client, req)
	if err != nil {
		logger.Error().Err(err).Msg("OTS operation failed")
		return err
	}

	logger.Debug().Interface("response", resp).Msg("Response received")

	// Handle response
	if handleResponse != nil {
		if err := handleResponse(&logger, resp, obj); err != nil {
			logger.Error().Err(err).Msg("Failed to handle response")
			return err
		}
	}

	return nil
}

// toAnySlice converts a slice of a specific type to []any
func toAnySlice[T any](slice []T) []any {
	result := make([]any, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// PutRowParams contains parameters for the PutRow operation.
type PutRowParams struct {
	// RowExistenceExpectation specifies the row existence expectation for the operation.
	RowExistenceExpectation *tablestore.RowExistenceExpectation
}

// PutRow inserts a row into the table.
// The obj parameter should be a pointer to a struct with fields tagged with "json" and optionally "pk".
// Fields tagged with "pk" are treated as primary key columns, others are treated as attribute columns.
// 
// Example usage:
// 
//  type MyRow struct {
//      PK1 *string `json:"pk1" pk:"1"`
//      Col1 *string `json:"col1"`
//  }
//  
//  row := MyRow{
//      PK1: tea.String("pk1value"),
//      Col1: tea.String("col1value"),
//  }
//  err := PutRow(ctx, &row)
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

		pks, cols, err := parseObj(ctx, obj)
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

// UpdateRow updates a row in the table.
// The obj parameter should be a pointer to a struct with fields tagged with "json" and "pk".
// Fields tagged with "pk" are treated as primary key columns and used to locate the row.
// Other fields in the struct are treated as attribute columns to update or add.
// 
// Example usage:
// 
//  type MyRow struct {
//      PK1 *string `json:"pk1" pk:"1"`
//      Col1 *string `json:"col1"`
//      Col2 *int64 `json:"col2"`
//  }
//  
//  row := MyRow{
//      PK1: tea.String("pk1value"),
//      Col1: tea.String("newcol1value"),
//      Col2: tea.Int64(42),
//  }
//  
//  expectExist := tablestore.RowExistenceExpectation_EXPECT_EXIST
//  err := UpdateRow(ctx, &row, UpdateRowParams{
//      RowExistenceExpectation: &expectExist,
//      DeletedColumns: []string{"old_column"},
//  })
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

		pks, cols, err := parseObj(ctx, obj)
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
//  type MyRow struct {
//      PK1 *string `json:"pk1" pk:"1"`
//      Col1 *string `json:"col1"`
//      Col2 *int64 `json:"col2"`
//  }
//  
//  row := MyRow{
//      PK1: tea.String("pk1value"),
//  }
//  err := GetRow(ctx, &row)
//  if err == nil {
//      // row.Col1 and row.Col2 are now populated with values from the table
//  }
func GetRow(ctx context.Context, obj any, params ...GetRowParams) error {
	buildReq := func(otsParams *OtsUtilsParams, logger *zerolog.Logger, obj any, params ...any) (any, error) {
		criteria := &tablestore.SingleRowQueryCriteria{
			TableName:  otsParams.TableName,
			MaxVersion: 1,
			PrimaryKey: &tablestore.PrimaryKey{},
		}

		pks, _, err := parseObj(ctx, obj)
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

		return parseResult(ctx, obj, pks, cols)
	}

	return executeOTSOperation(ctx, "GetRow", obj, buildReq, execute, handleResp, toAnySlice(params)...)
}