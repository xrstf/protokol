// SPDX-FileCopyrightText: 2023 Christoph Mewes
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"sync"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
)

type multiplexCollector struct {
	a Collector
	b Collector
}

var _ Collector = &multiplexCollector{}

func NewMultiplexCollector(a, b Collector) (Collector, error) {
	return &multiplexCollector{
		a: a,
		b: b,
	}, nil
}

func (c *multiplexCollector) CollectEvent(ctx context.Context, event *corev1.Event) error {
	if err := c.a.CollectEvent(ctx, event); err != nil {
		return err
	}

	return c.b.CollectEvent(ctx, event)
}

func (c *multiplexCollector) CollectPodMetadata(ctx context.Context, pod *corev1.Pod) error {
	if err := c.a.CollectPodMetadata(ctx, pod); err != nil {
		return err
	}

	return c.b.CollectPodMetadata(ctx, pod)
}

func (c *multiplexCollector) CollectLogs(ctx context.Context, log logrus.FieldLogger, pod *corev1.Pod, containerName string, stream io.Reader) error {
	pipeReader, pipeWriter := io.Pipe()
	teeReader := io.TeeReader(stream, pipeWriter)

	waiter := sync.WaitGroup{}
	waiter.Add(1)
	go func() {
		c.a.CollectLogs(ctx, log, pod, containerName, teeReader)
		pipeWriter.Close()
		waiter.Done()
	}()

	waiter.Add(1)
	go func() {
		c.b.CollectLogs(ctx, log, pod, containerName, pipeReader)
		waiter.Done()
	}()

	waiter.Wait()

	return nil
}
