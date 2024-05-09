// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package avrologencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/avrologencodingextension"

import (
	"fmt"

	"github.com/linkedin/goavro/v2"
)

type avroLogCodec interface {
	Deserialize([]byte) (map[string]any, error)
	Serialize(map[string]any) ([]byte, error)
}

type avroStaticSchemaLogCodec struct {
	codec *goavro.Codec
}

func newAVROStaticSchemaLogCodec(schema string) (avroLogCodec, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro codec: %w", err)
	}

	return &avroStaticSchemaLogCodec{
		codec: codec,
	}, nil
}

func (d *avroStaticSchemaLogCodec) Deserialize(data []byte) (map[string]any, error) {
	native, _, err := d.codec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize avro record: %w", err)
	}

	return native.(map[string]any), nil
}

func (d *avroStaticSchemaLogCodec) Serialize(data map[string]any) ([]byte, error) {
	binary, err := d.codec.BinaryFromNative(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize avro record: %w", err)
	}

	return binary, nil
}

