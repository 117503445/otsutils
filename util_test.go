package otsutils

import (
	"context"
	"testing"

	"github.com/117503445/goutils"
	"github.com/rs/zerolog/log"
)

func TestXxx(t *testing.T) {
	goutils.InitZeroLog()

	ctx := context.Background()

	ctx = log.Logger.WithContext(ctx)

	Insert(ctx)
}
