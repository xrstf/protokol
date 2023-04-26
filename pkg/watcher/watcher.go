package watcher

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"go.xrstf.de/protokol/pkg/collector"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type Watcher struct {
	clientset      *kubernetes.Clientset
	log            logrus.FieldLogger
	collector      collector.Collector
	initialPods    []corev1.Pod
	initialEvents  []corev1.Event
	opt            Options
	seenContainers sets.String
}

type Options struct {
	LabelSelector  labels.Selector
	Namespaces     []string
	ResourceNames  []string
	ContainerNames []string
	RunningOnly    bool
	OneShot        bool
	DumpMetadata   bool
	DumpEvents     bool
}

func NewWatcher(
	clientset *kubernetes.Clientset,
	c collector.Collector,
	log logrus.FieldLogger,
	initialPods []corev1.Pod,
	initialEvents []corev1.Event,
	opt Options,
) *Watcher {
	return &Watcher{
		clientset:      clientset,
		log:            log,
		collector:      c,
		initialPods:    initialPods,
		initialEvents:  initialEvents,
		opt:            opt,
		seenContainers: sets.NewString(),
	}
}

func (w *Watcher) Watch(ctx context.Context, podWatcher watch.Interface, eventWatcher watch.Interface) {
	wg := sync.WaitGroup{}

	for i := range w.initialPods {
		if w.podMatchesCriteria(&w.initialPods[i]) {
			w.startLogCollectors(ctx, &wg, &w.initialPods[i])
		}
	}

	for i := range w.initialEvents {
		if w.eventMatchesCriteria(&w.initialEvents[i]) {
			w.dumpEvent(ctx, &w.initialEvents[i])
		}
	}

	// eventWatcher is nil if neither --events not --raw-events was not specified.
	if eventWatcher != nil {
		wg.Add(1)

		go func() {
			for event := range eventWatcher.ResultChan() {
				unstructuredObj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}

				k8sEvent := &corev1.Event{}
				err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.UnstructuredContent(), k8sEvent)
				if err != nil {
					continue
				}

				if w.eventMatchesCriteria(k8sEvent) {
					w.dumpEvent(ctx, k8sEvent)
				}
			}

			wg.Done()
		}()
	}

	// wi can be nil if we do not want to actually watch, but instead
	// just process the initial pods (if --oneshot is given)
	if podWatcher != nil {
		for event := range podWatcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}

			pod := &corev1.Pod{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), pod)
			if err != nil {
				continue
			}

			if w.podMatchesCriteria(pod) {
				w.startLogCollectors(ctx, &wg, pod)
			}
		}
	}

	wg.Wait()
}

func (w *Watcher) startLogCollectors(ctx context.Context, wg *sync.WaitGroup, pod *corev1.Pod) {
	w.dumpPodMetadata(ctx, pod)
	w.startLogCollectorsForContainers(ctx, wg, pod, pod.Spec.InitContainers, pod.Status.InitContainerStatuses)
	w.startLogCollectorsForContainers(ctx, wg, pod, pod.Spec.Containers, pod.Status.ContainerStatuses)
}

func (w *Watcher) dumpEvent(ctx context.Context, event *corev1.Event) {
	if !w.opt.DumpEvents {
		return
	}

	if err := w.collector.CollectEvent(ctx, event); err != nil {
		w.getEventLog(event.InvolvedObject).WithError(err).Error("Failed to collect event.")
	}
}

func (w *Watcher) dumpPodMetadata(ctx context.Context, pod *corev1.Pod) {
	if !w.opt.DumpMetadata {
		return
	}

	if err := w.collector.CollectPodMetadata(ctx, pod); err != nil {
		w.getPodLog(pod).WithError(err).Error("Failed to collect pod metadata.")
	}
}

func (w *Watcher) startLogCollectorsForContainers(ctx context.Context, wg *sync.WaitGroup, pod *corev1.Pod, containers []corev1.Container, statuses []corev1.ContainerStatus) {
	podLog := w.getPodLog(pod)

	for _, container := range containers {
		containerName := container.Name
		containerLog := podLog.WithField("container", containerName)

		if !w.containerNameMatches(containerName) {
			containerLog.Debug("Container name does not match.")
			continue
		}

		var status *corev1.ContainerStatus
		for i, s := range statuses {
			if s.Name == containerName {
				status = &statuses[i]
				break
			}
		}

		// container has no status yet
		if status == nil {
			containerLog.Debug("Container has no status yet.")
			continue
		}

		// container sttaus not what we want
		if w.opt.RunningOnly {
			if status.State.Running == nil {
				containerLog.Debug("Container is not running.")
				continue
			}
		} else if status.State.Running == nil && status.State.Terminated == nil {
			containerLog.Debug("Container is still waiting.")
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

		wg.Add(1)
		go w.collectLogs(ctx, wg, containerLog, pod, containerName, int(status.RestartCount))
	}
}

func (w *Watcher) collectLogs(ctx context.Context, wg *sync.WaitGroup, log logrus.FieldLogger, pod *corev1.Pod, containerName string, restartCount int) {
	defer wg.Done()

	log.Info("Starting to collect logs…")

	request := w.clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		Container: containerName,
		Follow:    !w.opt.OneShot,
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

func (w *Watcher) getPodLog(pod *corev1.Pod) logrus.FieldLogger {
	return w.log.WithField("pod", pod.Name).WithField("namespace", pod.Namespace)
}

func (w *Watcher) podMatchesCriteria(pod *corev1.Pod) bool {
	podLog := w.getPodLog(pod)

	return w.resourceNameMatches(podLog, pod) && w.resourceNamespaceMatches(podLog, pod) && w.resourceLabelsMatches(podLog, pod)
}

func (w *Watcher) getEventLog(obj corev1.ObjectReference) logrus.FieldLogger {
	return w.log.WithField("pod", obj.Name).WithField("namespace", obj.Namespace)
}

func (w *Watcher) eventMatchesCriteria(event *corev1.Event) bool {
	obj := event.InvolvedObject

	if obj.Kind != "Pod" || obj.APIVersion != "v1" {
		w.log.Debug("Involved object is not a Pod.")
		return false
	}

	eventLog := w.getEventLog(obj)

	dummyPod := &corev1.Pod{}
	dummyPod.Name = obj.Name
	dummyPod.Namespace = obj.Namespace

	// Without fetching the object and hoping it still exists, we cannot compare the labels, so for
	// events we simply ignore the label selector :grim:

	return w.resourceNameMatches(eventLog, dummyPod) && w.resourceNamespaceMatches(eventLog, dummyPod)
}

func (w *Watcher) resourceNameMatches(log logrus.FieldLogger, pod *corev1.Pod) bool {
	if needleMatchesPatterns(pod.GetName(), w.opt.ResourceNames) {
		return true
	}

	log.Debug("Pod name does not match.")

	return false
}

func (w *Watcher) resourceNamespaceMatches(log logrus.FieldLogger, pod *corev1.Pod) bool {
	if needleMatchesPatterns(pod.GetNamespace(), w.opt.Namespaces) {
		return true
	}

	log.Debug("Pod namespace does not match.")

	return false
}

func (w *Watcher) resourceLabelsMatches(log logrus.FieldLogger, pod *corev1.Pod) bool {
	if w.opt.LabelSelector == nil || w.opt.LabelSelector.Matches(labels.Set(pod.Labels)) {
		return true
	}

	log.Debug("Pod labels do not match.")

	return false
}

func (w *Watcher) containerNameMatches(containerName string) bool {
	return needleMatchesPatterns(containerName, w.opt.ContainerNames)
}

func nameMatches(name string, pattern string) bool {
	if strings.Contains(pattern, "*") {
		matched, _ := filepath.Match(pattern, name)
		return matched
	}

	return name == pattern
}

func needleMatchesPatterns(needle string, patterns []string) bool {
	// no patterns given, so everything matches
	if len(patterns) == 0 {
		return true
	}

	for _, pattern := range patterns {
		if nameMatches(needle, pattern) {
			return true
		}
	}

	return false
}
