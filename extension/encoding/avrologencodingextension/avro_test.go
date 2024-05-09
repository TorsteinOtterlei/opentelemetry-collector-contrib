// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package avrologencodingextension

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewAvroLogsUnmarshaler(t *testing.T) {
	schema, data := createAVROTestData(t)

	avroEncoder, err := newAVROStaticSchemaLogCodec(schema)
	if err != nil {
		t.Errorf("Did not expect an error, got %q", err.Error())
	}

	logMap, err := avroEncoder.Deserialize(data)
	if err != nil {
		t.Fatalf("Did not expect an error, got %q", err.Error())
	}

	assert.Equal(t, int64(1697187201488000000), logMap["timestamp"].(time.Time).UnixNano())
	assert.Equal(t, "host1", logMap["hostname"])
	assert.Equal(t, int64(12), logMap["nestedRecord"].(map[string]any)["field1"])

	props := logMap["properties"].([]any)
	propsStr := make([]string, len(props))
	for i, prop := range props {
		propsStr[i] = prop.(string)
	}

	assert.Equal(t, []string{"prop1", "prop2"}, propsStr)
}

func TestNewAvroLogsUnmarshalerInvalidSchema(t *testing.T) {
	_, err := newAVROStaticSchemaLogCodec("invalid schema")
	assert.Error(t, err)
}

func TestNewAvroLogsMarshaler(t *testing.T) {

	schema, jsonMap := createMapTestData(t)

	avroEncoder, err := newAVROStaticSchemaLogCodec(schema)
	if err != nil {
		t.Errorf("Did not expect an error, got %q", err.Error())
	}

	avroData, err := avroEncoder.Serialize(jsonMap)
	if err != nil {
		t.Fatalf("Did not expect an error, got %q", err.Error())
	}

	logMap, err := avroEncoder.Deserialize(avroData)
		if err != nil {
		t.Fatalf("Did not expect an error, got %q", err.Error())
	}

	assert.Equal(t, int64(1697187201488000000), logMap["timestamp"].(time.Time).UnixNano())
	assert.Equal(t, "host1", logMap["hostname"])
	assert.Equal(t, int64(12), logMap["nestedRecord"].(map[string]any)["field1"])

	props := logMap["properties"].([]any)
	propsStr := make([]string, len(props))
	for i, prop := range props {
		propsStr[i] = prop.(string)
	}

	assert.Equal(t, []string{"prop1", "prop2"}, propsStr)
}