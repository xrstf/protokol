package collector

import (
	"context"
	"io"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
)

type Collector interface {
	CollectLogs(ctx context.Context, log logrus.FieldLogger, pod *corev1.Pod, containerName string, stream io.ReadCloser) error
}
