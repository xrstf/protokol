package collector

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/goware/prefixer"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
)

type streamCollector struct {
	prefixFormat string
}

var _ Collector = &streamCollector{}

func NewStreamCollector(prefixFormat string) (Collector, error) {
	return &streamCollector{
		prefixFormat: prefixFormat,
	}, nil
}

func (c *streamCollector) CollectPodMetadata(ctx context.Context, pod *corev1.Pod) error {
	return nil
}

func (c *streamCollector) CollectLogs(ctx context.Context, log logrus.FieldLogger, pod *corev1.Pod, containerName string, stream io.Reader) error {
	prefixReader := prefixer.New(stream, c.prefix(pod, containerName)+" ")
	rd := bufio.NewReader(prefixReader)

	for {
		str, err := rd.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		fmt.Println(strings.TrimSpace(str))
	}

	return nil
}

var placeholders = regexp.MustCompile(`%([a-zA-Z]+)`)

func (c *streamCollector) prefix(pod *corev1.Pod, containerName string) string {
	return strings.TrimSpace(placeholders.ReplaceAllStringFunc(c.prefixFormat, func(s string) string {
		switch s {
		case "%pn":
			return pod.Name
		case "%pN":
			return pod.Namespace
		case "%c":
			return containerName
		}

		return s
	}))
}
