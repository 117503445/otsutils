package otsutils

import (
	"context"

	"github.com/rs/zerolog/log"
)

func Insert(ctx context.Context) {
	logger := log.Ctx(ctx)

	logger.Debug().Msg("insert")
}
