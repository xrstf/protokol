package collector

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
)

type diskCollector struct {
	directory string
	flatFiles bool
}

var _ Collector = &diskCollector{}

func NewDiskCollector(directory string, flatFiles bool) (Collector, error) {
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", directory, err)
	}

	abs, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to determine absolute path to %q: %w", directory, err)
	}

	return &diskCollector{
		directory: abs,
		flatFiles: flatFiles,
	}, nil
}

func (c *diskCollector) CollectLogs(ctx context.Context, log logrus.FieldLogger, pod *corev1.Pod, containerName string, stream io.ReadCloser) error {
	filename := fmt.Sprintf("%s_%s_%03d.log", pod.Name, containerName, getContainerIncarnation(pod, containerName))
	directory := c.directory

	if c.flatFiles {
		filename = fmt.Sprintf("%s_%s", pod.Namespace, filename)
	} else {
		directory = filepath.Join(directory, pod.Namespace)

		if err := os.MkdirAll(directory, 0755); err != nil {
			return fmt.Errorf("failed to create directory %q: %w", directory, err)
		}
	}

	filename = filepath.Join(directory, filename)

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to open log file %q: %w", filename, err)
	}
	defer f.Close()

	_, err = io.Copy(f, stream)
	if err != nil {
		return fmt.Errorf("failed to write to log file %q: %w", filename, err)
	}

	return nil
}

func getContainerIncarnation(pod *corev1.Pod, containerName string) int {
	for _, s := range pod.Status.ContainerStatuses {
		if s.Name == containerName {
			return int(s.RestartCount)
		}
	}

	return 0
}
