// Package otsutils provides utilities for working with Alibaba Cloud Tablestore (OTS).
package otsutils

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/rs/zerolog/log"
)

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