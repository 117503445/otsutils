// Package otsutils provides utilities for working with Alibaba Cloud Tablestore (OTS).
package otsutils

import (
	"context"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/rs/zerolog"
)

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
