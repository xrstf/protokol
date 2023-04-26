package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

type diskCollector struct {
	directory    string
	flatFiles    bool
	eventsAsText bool
	rawEvents    bool
}

var _ Collector = &diskCollector{}

func NewDiskCollector(directory string, flatFiles bool, eventsAsText bool, rawEvents bool) (Collector, error) {
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", directory, err)
	}

	abs, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to determine absolute path to %q: %w", directory, err)
	}

	return &diskCollector{
		directory:    abs,
		flatFiles:    flatFiles,
		eventsAsText: eventsAsText,
		rawEvents:    rawEvents,
	}, nil
}

func (c *diskCollector) getDirectory(namespace string) (string, error) {
	directory := c.directory

	if !c.flatFiles {
		directory = filepath.Join(c.directory, namespace)
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory %q: %w", directory, err)
	}

	return directory, nil
}

func (c *diskCollector) CollectPodMetadata(ctx context.Context, pod *corev1.Pod) error {
	directory, err := c.getDirectory(pod.Namespace)
	if err != nil {
		return err
	}

	filename := filepath.Join(directory, fmt.Sprintf("%s.yaml", pod.Name))

	// file exists already, do not overwrite
	if _, err := os.Stat(filename); err == nil {
		return nil
	}

	pod.APIVersion = "v1"
	pod.Kind = "Pod"

	encoded, err := yaml.Marshal(pod)
	if err != nil {
		return err
	}

	return os.WriteFile(filename, encoded, 0644)
}

func (c *diskCollector) CollectEvent(ctx context.Context, event *corev1.Event) error {
	if !c.eventsAsText && !c.rawEvents {
		return errors.New("event dumping is not enabled")
	}

	directory, err := c.getDirectory(event.InvolvedObject.Namespace)
	if err != nil {
		return err
	}

	if c.eventsAsText {
		if err := c.dumpEventAsText(directory, event); err != nil {
			return err
		}
	}

	if c.rawEvents {
		if err := c.dumpEventAsYAML(directory, event); err != nil {
			return err
		}
	}

	return nil
}

func (c *diskCollector) dumpEventAsText(directory string, event *corev1.Event) error {
	filename := filepath.Join(directory, fmt.Sprintf("%s.events.log", event.InvolvedObject.Name))

	stringified := fmt.Sprintf("%s: [%s]", event.LastTimestamp.Format(time.RFC1123), event.Type)
	if event.Source.Component != "" {
		stringified = fmt.Sprintf("%s [%s]", stringified, event.Source.Component)
	}
	stringified = fmt.Sprintf("%s %s (reason: %s) (%dx)\n", stringified, event.Message, event.Reason, event.Count)

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	_, err = f.WriteString(stringified)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}

	return err
}

func (c *diskCollector) dumpEventAsYAML(directory string, event *corev1.Event) error {
	filename := filepath.Join(directory, fmt.Sprintf("%s.events.yaml", event.InvolvedObject.Name))

	trimmedEvent := event.DeepCopy()
	trimmedEvent.ManagedFields = nil

	encoded, err := yaml.Marshal(trimmedEvent)
	if err != nil {
		return err
	}

	encoded = append([]byte("---\n"), encoded...)
	encoded = append(encoded, []byte("\n")...)

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	_, err = f.Write(encoded)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}

	return err
}

func (c *diskCollector) CollectLogs(ctx context.Context, log logrus.FieldLogger, pod *corev1.Pod, containerName string, stream io.Reader) error {
	directory, err := c.getDirectory(pod.Namespace)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s_%s_%03d.log", pod.Name, containerName, getContainerIncarnation(pod, containerName))
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
