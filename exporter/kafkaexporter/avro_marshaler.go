// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"

import (
	"errors"
	"fmt"
	"io"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)


const (
	avroEncoding         = "avro"
	avroSchemaPrefixFile = "file:"
)


var (
	errFailedToReadAvroSchema = errors.New("failed to read avro schema")
)


type avroLogsMarshaler struct {
	serializer avroSerializer
}


func newAVROLogsMarshaler() *avroLogsMarshaler {
	return &avroLogsMarshaler{}
}

func (a *avroLogsMarshaler) Encoding() string {
	return avroEncoding
}


func (a *avroLogsMarshaler) WithSchema(schemaReader io.Reader) (*avroLogsMarshaler, error) {
	schema, err := io.ReadAll(schemaReader)
	if err != nil {
		return nil, errFailedToReadAvroSchema
	}

	serializer, err := newAVROStaticSchemaSerializer(string(schema))
	a.serializer = serializer

	return a, err
}


func (a *avroLogsMarshaler) Marshal(logs plog.Logs, topic string) ([]*sarama.ProducerMessage, error) {
	var messages []*sarama.ProducerMessage

	// if a.serializer == nil {
	// 	return p, errors.New("avro serializer not set")
	// }

	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				bodyMap, err := logBodyMap(lr.Body())
				if err != nil {
					return nil, err
				}
				b, err := a.serializer.Serialize(bodyMap)
				if err != nil {
					return nil, err
				}
				if len(b) == 0 {
					continue
				}

				messages = append(messages, &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.ByteEncoder(b),
				})
			}
		}
	}
	return messages, nil
}


type avroSerializer interface {
	Serialize(any) ([]byte, error)
}

type avroStaticSchemaSerializer struct {
	codec *goavro.Codec
}

func newAVROStaticSchemaSerializer(schema string) (avroSerializer, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro codec: %w", err)
	}

	return &avroStaticSchemaSerializer{
		codec: codec,
	}, nil
}

func (d *avroStaticSchemaSerializer) Serialize(value any) ([]byte, error) {
	binary, err := d.codec.BinaryFromNative(nil, value)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize into avro record: %w", err)
	}

	return binary, nil
}

func logBodyMap(value pcommon.Value) (map[string]any, error) {
	switch value.Type() {
	case pcommon.ValueTypeMap:
		return value.Map().AsRaw(), nil
	default:
		return nil, errUnsupported
	}
}