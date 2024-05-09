// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package avrologencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/avrologencodingextension"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
)

var (
	_ encoding.LogsMarshalerExtension   = (*avroLogExtension)(nil)
	_ encoding.LogsUnmarshalerExtension = (*avroLogExtension)(nil)
)

type avroLogExtension struct {
	config      *Config
	avroEncoder avroLogCodec
}

func newExtension(config *Config) (*avroLogExtension, error) {
	avroEncoder, err := newAVROStaticSchemaLogCodec(config.Schema)
	if err != nil {
		return nil, err
	}

	return &avroLogExtension{config: config, avroEncoder: avroEncoder}, nil
}


func (e *avroLogExtension) MarshalLogs(logs plog.Logs) ([]byte, error) {
	logRecord := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body()

	var raw map[string]any
	switch logRecord.Type() {
	case pcommon.ValueTypeMap:
		raw = logRecord.Map().AsRaw()
	default:
		return nil, fmt.Errorf("Marshal: Expected 'Map' found '%v'", logRecord.Type().String())
	}

	buf, err := e.avroEncoder.Serialize(raw, e.config.SchemaID)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (e *avroLogExtension) UnmarshalLogs(buf []byte) (plog.Logs, error) {
	p := plog.NewLogs()

	avroLog, err := e.avroEncoder.Deserialize(buf)
	if err != nil {
		return p, fmt.Errorf("failed to deserialize avro log: %w", err)
	}

	logRecords := p.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	logRecords.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// removes time.Time values as FromRaw does not support it
	replaceLogicalTypes(avroLog)

	// Set the unmarshaled avro as the body of the log record
	if err := logRecords.Body().SetEmptyMap().FromRaw(avroLog); err != nil {
		return p, err
	}

	return p, nil
}

func replaceLogicalTypes(m map[string]any) {
	for k, v := range m {
		m[k] = transformValue(v)
	}
}

func transformValue(value any) any {
	if timeValue, ok := value.(time.Time); ok {
		return timeValue.UnixNano()
	}

	if mapValue, ok := value.(map[string]any); ok {
		replaceLogicalTypes(mapValue)
		return mapValue
	}

	if arrayValue, ok := value.([]any); ok {
		for i, v := range arrayValue {
			arrayValue[i] = transformValue(v)
		}
		return arrayValue
	}

	return value
}

func (e *avroLogExtension) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (e *avroLogExtension) Shutdown(_ context.Context) error {
	return nil
}
