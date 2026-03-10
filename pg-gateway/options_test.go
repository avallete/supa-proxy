package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func intPtr(v int) *int       { return &v }
func strPtr(v string) *string { return &v }

func TestResolveOptions_DefaultsFromConfig(t *testing.T) {
	cfg := &Config{
		MaxFieldBytes:    1048576,
		MaxRows:          500,
		StatementTimeout: 30 * time.Second,
		CursorBatchSize:  100,
		MaxMemory:        10485760,
	}
	req := &sqlRequest{}
	opts := resolveOptions(cfg, req)

	assert.Equal(t, 1048576, opts.MaxFieldBytes)
	assert.Equal(t, 500, opts.MaxRows)
	assert.Equal(t, 30*time.Second, opts.StatementTimeout)
	assert.Equal(t, 100, opts.CursorBatchSize)
	assert.Equal(t, 10485760, opts.MaxMemory)
}

func TestResolveOptions_RequestOverrides(t *testing.T) {
	cfg := &Config{
		MaxFieldBytes:    1048576,
		MaxRows:          500,
		StatementTimeout: 30 * time.Second,
		CursorBatchSize:  100,
		MaxMemory:        10485760,
	}
	req := &sqlRequest{
		MaxFieldBytes:    intPtr(512),
		MaxRows:          intPtr(10),
		StatementTimeout: strPtr("5s"),
		CursorBatchSize:  intPtr(50),
		MaxMemory:        intPtr(5242880),
	}
	opts := resolveOptions(cfg, req)

	assert.Equal(t, 512, opts.MaxFieldBytes)
	assert.Equal(t, 10, opts.MaxRows)
	assert.Equal(t, 5*time.Second, opts.StatementTimeout)
	assert.Equal(t, 50, opts.CursorBatchSize)
	assert.Equal(t, 5242880, opts.MaxMemory)
}

func TestResolveOptions_PartialOverride(t *testing.T) {
	cfg := &Config{
		MaxFieldBytes:    1048576,
		MaxRows:          0,
		StatementTimeout: 30 * time.Second,
		CursorBatchSize:  100,
		MaxMemory:        10485760,
	}
	req := &sqlRequest{
		MaxFieldBytes: intPtr(0),
	}
	opts := resolveOptions(cfg, req)

	assert.Equal(t, 0, opts.MaxFieldBytes)
	assert.Equal(t, 0, opts.MaxRows)
	assert.Equal(t, 30*time.Second, opts.StatementTimeout)
	assert.Equal(t, 100, opts.CursorBatchSize)
}

func TestResolveOptions_InvalidTimeout(t *testing.T) {
	cfg := &Config{
		StatementTimeout: 30 * time.Second,
		CursorBatchSize:  100,
	}
	req := &sqlRequest{
		StatementTimeout: strPtr("not_a_duration"),
	}
	opts := resolveOptions(cfg, req)
	assert.Equal(t, 30*time.Second, opts.StatementTimeout)
}

func TestResolveOptions_ZeroCursorBatchSize(t *testing.T) {
	cfg := &Config{CursorBatchSize: 0}
	req := &sqlRequest{}
	opts := resolveOptions(cfg, req)
	assert.Equal(t, 100, opts.CursorBatchSize)
}

func TestResolveOptions_RequestZeroCursorBatchSizeIgnored(t *testing.T) {
	cfg := &Config{CursorBatchSize: 200}
	req := &sqlRequest{CursorBatchSize: intPtr(0)}
	opts := resolveOptions(cfg, req)
	assert.Equal(t, 200, opts.CursorBatchSize)
}
