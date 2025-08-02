// Package otsutils provides utilities for working with Alibaba Cloud Tablestore (OTS).
package otsutils

import (
	"context"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
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

// OtsUtilsParams holds the TableStore client and table name.
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