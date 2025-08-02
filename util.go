package otsutils

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
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

type PutRowParams struct {
	RowExistenceExpectation *tablestore.RowExistenceExpectation
}

func PutRow(ctx context.Context, obj any, params ...PutRowParams) error {
	logger := log.Ctx(ctx)

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

	v := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)

	// 如果是指针，解引用
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	// 确保是结构体
	if v.Kind() != reflect.Struct {
		err := fmt.Errorf("obj must be a struct or pointer to struct")
		logger.Error().Err(err).Send()
		return err
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
			err := fmt.Errorf("field %s has invalid type: %s. Only *string, *int64, and *[]byte are allowed", fieldType.Name, field.Type())
			logger.Error().Err(err).Send()
			return err
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
		if isPk {
			putPk.AddPrimaryKeyColumn(jsonTag, value)
		} else {
			putRowChange.AddColumn(jsonTag, value)
		}
	}

	logger.Debug().Interface("PutRowRequest", putRowRequest).Send()

	_, err := otsUtilsParams.Client.PutRow(putRowRequest)
	if err != nil {
		logger.Error().Err(err).Msg("put row failed")
		return err
	}

	return nil
}
