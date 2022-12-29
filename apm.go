package main

import (
	"context"
	"errors"
	"fmt"
	"go.elastic.co/apm/module/apmhttp/v2"
	"go.elastic.co/apm/v2"
)

const (
	TraceParentKey = "elasticapmtraceparent"
	TraceStateKey  = "tracestate"
)

var (
	NoTransactionErr       = errors.New("no transaction in given context")
	NoTraceParentHeaderErr = errors.New("no header with trace parent present")
	NoTraceStateHeaderErr  = errors.New("no header with trace state present")
)

type Header struct {
	Key   string
	Value []byte
}

func TraceHeaders(ctxWithTransaction context.Context) ([]Header, error) {
	tx := apm.TransactionFromContext(ctxWithTransaction)
	if tx == nil {
		return nil, NoTransactionErr
	}

	return []Header{
		{Key: TraceParentKey, Value: []byte(apmhttp.FormatTraceparentHeader(tx.TraceContext()))},
		{Key: TraceStateKey, Value: []byte(tx.TraceContext().State.String())},
	}, nil
}

func ParseTraceHeaders(headers ...Header) (*apm.TraceContext, error) {
	parent, state, err := findTraceHeaders(headers)
	if err != nil {
		return nil, err
	}

	traceCtx, err := apmhttp.ParseTraceparentHeader(string(parent.Value))
	if err != nil {
		return nil, fmt.Errorf("could not parse trace parent: %w", err)
	}

	traceCtx.State, err = apmhttp.ParseTracestateHeader(string(state.Value))
	if err != nil {
		return nil, fmt.Errorf("could not parse trace state: %w", err)
	}

	return &traceCtx, nil
}

func findTraceHeaders(headers []Header) (parent Header, state Header, err error) {
	for _, header := range headers {
		if header.Key == TraceParentKey {
			parent = header
		}
		if header.Key == TraceStateKey {
			state = header
		}
	}

	if parent.Key == "" {
		err = NoTraceParentHeaderErr
		return
	}

	if state.Key == "" {
		err = NoTraceStateHeaderErr
	}
	return parent, state, err
}

func FormatTraceParent(ctxWithTransaction context.Context) (string, error) {
	tx := apm.TransactionFromContext(ctxWithTransaction)
	if tx == nil {
		return "", NoTransactionErr
	}

	return apmhttp.FormatTraceparentHeader(tx.TraceContext()), nil
}

func ParseTraceParent(header string) (apm.TraceContext, error) {
	return apmhttp.ParseTraceparentHeader(header)
}

func FormatTraceState(ctxWithTransaction context.Context) (string, error) {
	tx := apm.TransactionFromContext(ctxWithTransaction)
	if tx == nil {
		return "", NoTransactionErr
	}

	return tx.TraceContext().State.String(), nil
}

func ParseTraceState(header string) (apm.TraceState, error) {
	return apmhttp.ParseTracestateHeader(header)
}
