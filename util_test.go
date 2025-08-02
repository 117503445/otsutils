package otsutils

import (
	"context"
	"os"
	"testing"

	"github.com/117503445/goutils"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/rs/zerolog/log"
)

type Obj struct {
	Pk1  *string `json:"pk1" pk:"1"`
	Pk2  *int64  `json:"pk2" pk:"1"`
	Pk3  *[]byte `json:"pk3" pk:"1"`
	Col1 *string `json:"col1"`
	Col2 *int64  `json:"col2"`
	Col3 *string `json:"col3"`
}

func TestXxx(t *testing.T) {
	goutils.InitZeroLog()

	ctx := context.Background()
	ctx = log.Logger.WithContext(ctx)

	client := NewClient(ctx, os.Getenv("endpoint"), os.Getenv("instanceName"), os.Getenv("ak"), os.Getenv("sk"))

	o := OtsUtilsParams{
		Client:    client,
		TableName: "test_table",
	}
	ctx = o.WithContext(ctx)

	obj := Obj{
		Pk1:  tea.String("pk1"),
		Pk2:  tea.Int64(1),
		Col1: tea.String("col1"),
	}
	PutRow(ctx, obj)
}
