package watcher

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"go.xrstf.de/loks/pkg/collector"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type Watcher struct {
	clientset      *kubernetes.Clientset
	log            logrus.FieldLogger
	collector      collector.Collector
	namespaces     []string
	resourceNames  []string
	containerNames []string
	seenContainers sets.String
}

func NewWatcher(clientset *kubernetes.Clientset, c collector.Collector, log logrus.FieldLogger, namespaces, resourceNames, containerNames []string) *Watcher {
	return &Watcher{
		clientset:      clientset,
		log:            log,
		collector:      c,
		namespaces:     namespaces,
		resourceNames:  resourceNames,
		containerNames: containerNames,
		seenContainers: sets.NewString(),
	}
}

func (w *Watcher) Watch(ctx context.Context, wi watch.Interface) {
	for event := range wi.ResultChan() {
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			continue
		}

		pod := &corev1.Pod{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), pod)
		if err != nil {
			continue
		}

		if w.resourceNameMatches(pod) && w.resourceNamespaceMatches(pod) {
			w.startLogCollectors(ctx, pod)
		}
	}
}

func (w *Watcher) startLogCollectors(ctx context.Context, pod *corev1.Pod) {
	for _, container := range pod.Spec.Containers {
		containerName := container.Name

		if !w.containerNameMatches(containerName) {
			continue
		}

		var status *corev1.ContainerStatus
		for i, s := range pod.Status.ContainerStatuses {
			if s.Name == containerName {
				status = &pod.Status.ContainerStatuses[i]
				break
			}
		}

		// container has no status yet
		if status == nil {
			continue
		}

		// container is not running
		if status.State.Running == nil {
			continue
		}

		ident := fmt.Sprintf("%s:%s:%s:%d", pod.Namespace, pod.Name, containerName, status.RestartCount)

		// we have already started a collector for this incarnation of the container;
		// whenever a container restarts, we want to create a new collector with the
		// new restart count
		if w.seenContainers.Has(ident) {
			continue
		}

		// remember that we have seen this incarnation
		w.seenContainers.Insert(ident)
		log := w.log.WithFields(logrus.Fields{
			"namespace": pod.Namespace,
			"pod":       pod.Name,
			"container": containerName,
		})

		go w.collectLogs(ctx, log, pod, containerName, int(status.RestartCount))
	}
}

func (w *Watcher) collectLogs(ctx context.Context, log logrus.FieldLogger, pod *corev1.Pod, containerName string, restartCount int) {
	log.Info("Starting to collect logs.")

	request := w.clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		Container: containerName,
		Follow:    true,
	})

	stream, err := request.Stream(ctx)
	if err != nil {
		log.WithError(err).Error("Failed to stream logs.")
		return
	}
	defer stream.Close()

	if err := w.collector.CollectLogs(ctx, log, pod, containerName, stream); err != nil {
		log.WithError(err).Error("Failed to collect logs.")
	}

	log.Info("Logs have finished.")
}

func (w *Watcher) resourceNameMatches(pod *corev1.Pod) bool {
	// no names given, so all resources match
	if len(w.resourceNames) == 0 {
		return true
	}

	for _, pattern := range w.resourceNames {
		if nameMatches(pod.GetName(), pattern) {
			return true
		}
	}

	return false
}

func (w *Watcher) resourceNamespaceMatches(pod *corev1.Pod) bool {
	// no namespaces given, so all resources match
	if len(w.namespaces) == 0 {
		return true
	}

	for _, pattern := range w.namespaces {
		if nameMatches(pod.GetNamespace(), pattern) {
			return true
		}
	}

	return false
}

func (w *Watcher) containerNameMatches(containerName string) bool {
	// no container names given, so all resources match
	if len(w.containerNames) == 0 {
		return true
	}

	for _, pattern := range w.containerNames {
		if nameMatches(containerName, pattern) {
			return true
		}
	}

	return false
}

func nameMatches(name string, pattern string) bool {
	if strings.Contains(pattern, "*") {
		matched, _ := filepath.Match(pattern, name)
		return matched
	}

	return name == pattern
}
