package collector

import (
	"context"
	"io"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
)

type Collector interface {
	CollectPodMetadata(ctx context.Context, pod *corev1.Pod) error
	CollectLogs(ctx context.Context, log logrus.FieldLogger, pod *corev1.Pod, containerName string, stream io.Reader) error
}
