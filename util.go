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

	// 如果是指针，解引用
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	// 确保是结构体
	if v.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("obj must be a struct or pointer to struct")
	}

	// 遍历所有字段
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		isValidPointerType := func(t reflect.Type) bool {
			// 必须是指针
			if t.Kind() != reflect.Ptr {
				return false
			}
			// 指向的类型必须是 string、int64 或 []byte
			elem := t.Elem()
			switch elem.Kind() {
			case reflect.String:
				return true
			case reflect.Int64:
				return true
			case reflect.Slice:
				return elem.Elem().Kind() == reflect.Uint8 // []byte 是 []uint8
			default:
				return false
			}
		}
		// 检查字段类型是否合法
		if !isValidPointerType(field.Type()) {
			return nil, nil, fmt.Errorf("field %s has invalid type: %s. Only *string, *int64, and *[]byte are allowed", fieldType.Name, field.Type())
		}

		// 如果是指针且为 nil，跳过
		if field.IsNil() {
			continue // 注意：这里 continue，不参与 PutRow
		}

		jsonTag := fieldType.Tag.Get("json")
		pkTag := fieldType.Tag.Get("pk")

		// logger.Debug().Str("jsonTag", jsonTag).Str("pkTag", pkTag).Send()

		value := field.Elem().Interface()

		// 判断是否为主键
		isPk := pkTag != ""

		// 根据是否为主键，添加到对应的地方
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

	// 内部函数：类型不匹配错误
	typeMismatchError := func(fieldType, value any) error {
		return fmt.Errorf("expected %v, but got %T", fieldType, value)
	}

	// 内部函数：赋值到指针字段
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

	// 构建 json tag 到字段的映射
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

		// 去除 ,omitempty 等修饰
		if idx := strings.Index(jsonTag, ","); idx != -1 {
			jsonTag = jsonTag[:idx]
		}

		fieldMap[jsonTag] = field
	}

	// 处理主键
	for colName, value := range pks {
		if field, ok := fieldMap[colName]; ok {
			if err := assignToPointerField(field, value); err != nil {
				return fmt.Errorf("primary key %q: %w", colName, err)
			}
		}
	}

	// 处理普通列
	for colName, value := range cols {
		if field, ok := fieldMap[colName]; ok {
			if err := assignToPointerField(field, value); err != nil {
				return fmt.Errorf("column %q: %w", colName, err)
			}
		}
	}

	return nil
}

type PutRowParams struct {
	RowExistenceExpectation *tablestore.RowExistenceExpectation
}

func PutRow(ctx context.Context, obj any, params ...PutRowParams) error {
	logger := zerolog.Ctx(ctx).With().CallerWithSkipFrameCount(3).Logger()

	rowExistenceExpectation := tablestore.RowExistenceExpectation_EXPECT_NOT_EXIST
	if len(params) > 0 && params[0].RowExistenceExpectation != nil {
		rowExistenceExpectation = *params[0].RowExistenceExpectation
	}

	logger.Debug().
		Interface("obj", obj).
		Interface("rowExistenceExpectation", rowExistenceExpectation).
		Msg("PutRow")

	otsUtilsParams := otsUtilsParamsFromCtx(ctx)

	putRowRequest := new(tablestore.PutRowRequest)
	putRowChange := new(tablestore.PutRowChange)
	putRowChange.TableName = otsUtilsParams.TableName
	putPk := new(tablestore.PrimaryKey)

	putRowChange.PrimaryKey = putPk

	putRowChange.SetCondition(rowExistenceExpectation)
	putRowRequest.PutRowChange = putRowChange

	pks, cols, err := parseObj(ctx, obj)
	if err != nil {
		logger.Error().Err(err).Msg("parseObj")
		return err
	}
	for k, v := range pks {
		putPk.AddPrimaryKeyColumn(k, v)
	}
	for k, v := range cols {
		putRowChange.AddColumn(k, v)
	}

	logger.Debug().Interface("PutRowRequest", putRowRequest).Send()

	resp, err := otsUtilsParams.Client.PutRow(putRowRequest)
	if err != nil {
		logger.Error().Err(err).Msg("put row failed")
		return err
	}
	logger.Debug().Interface("PutRowResponse", resp).Send()

	return nil
}

type GetRowParams struct {
}

func GetRow(ctx context.Context, obj any, params ...GetRowParams) error {
	logger := zerolog.Ctx(ctx).With().CallerWithSkipFrameCount(3).Logger()

	otsUtilsParams := otsUtilsParamsFromCtx(ctx)

	logger.Debug().
		Interface("obj", obj).
		Msg("GetRow")

	getRowRequest := new(tablestore.GetRowRequest)
	criteria := new(tablestore.SingleRowQueryCriteria)
	putPk := new(tablestore.PrimaryKey)

	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = otsUtilsParams.TableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1

	pks, _, err := parseObj(ctx, obj)
	if err != nil {
		logger.Error().Err(err).Msg("parseObj")
		return err
	}
	for k, v := range pks {
		putPk.AddPrimaryKeyColumn(k, v)
	}

	logger.Debug().Interface("getRowRequest", getRowRequest).Send()
	resp, err := otsUtilsParams.Client.GetRow(getRowRequest)
	if err != nil {
		logger.Error().Err(err).Msg("GetRow error")
		return err
	}
	logger.Debug().Interface("getRowResponse", resp).Send()
	{
		pks := make(map[string]any)
		for _, pk := range resp.PrimaryKey.PrimaryKeys {
			// logger.Info().
			// 	Str("pk", pk.ColumnName).
			// 	Str("value", pk.Value.(string)).
			// 	Msg("GetRow")
			pks[pk.ColumnName] = pk.Value
		}

		cols := make(map[string]any)
		for _, col := range resp.Columns {
			// logger.Info().
			// 	Str("pk", col.ColumnName).
			// 	Str("value", col.Value.(string)).
			// 	Msg("GetRow")
			cols[col.ColumnName] = col.Value
		}

		err := parseResult(ctx, obj, pks, cols)
		if err != nil {
			logger.Error().
				Err(err).
				Msg("parseResult")
			return err
		}
	}

	return nil
}
