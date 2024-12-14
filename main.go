// SPDX-FileCopyrightText: 2023 Christoph Mewes
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"go.xrstf.de/protokol/pkg/collector"
	"go.xrstf.de/protokol/pkg/watcher"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/tools/clientcmd"
	watchtools "k8s.io/client-go/tools/watch"
)

// These variables get set by ldflags during compilation.
var (
	BuildTag    string
	BuildCommit string
	BuildDate   string // RFC3339 format ("2006-01-02T15:04:05Z07:00")
)

func printVersion() {
	// handle empty values in case `go install` was used
	if BuildCommit == "" {
		fmt.Printf("protokol dev, built with %s\n",
			runtime.Version(),
		)
	} else {
		fmt.Printf("protokol %s (%s), built with %s on %s\n",
			BuildTag,
			BuildCommit[:10],
			runtime.Version(),
			BuildDate,
		)
	}
}

type options struct {
	kubeconfig     string
	directory      string
	namespaces     []string
	containerNames []string
	stream         bool
	streamPrefix   string
	labels         string
	live           bool
	oneShot        bool
	flatFiles      bool
	dumpMetadata   bool
	dumpEvents     bool
	dumpRawEvents  bool
	verbose        bool
	version        bool
}

func main() {
	rootCtx := context.Background()
	opt := options{
		streamPrefix: "[%pN/%pn:%c] >>",
	}

	pflag.StringVar(&opt.kubeconfig, "kubeconfig", opt.kubeconfig, "kubeconfig file to use (uses $KUBECONFIG by default)")
	pflag.StringArrayVarP(&opt.namespaces, "namespace", "n", opt.namespaces, "Kubernetes namespace to watch resources in (supports glob expression) (can be given multiple times)")
	pflag.StringArrayVarP(&opt.containerNames, "container", "c", opt.containerNames, "Container names to store logs for (supports glob expression) (can be given multiple times)")
	pflag.StringVarP(&opt.labels, "labels", "l", opt.labels, "Label-selector as an alternative to specifying resource names")
	pflag.StringVarP(&opt.directory, "output", "o", opt.directory, "Directory where logs should be stored")
	pflag.BoolVarP(&opt.flatFiles, "flat", "f", opt.flatFiles, "Do not create directory per namespace, but put all logs in the same directory")
	pflag.BoolVar(&opt.live, "live", opt.live, "Only consider running pods, ignore completed/failed pods")
	pflag.BoolVar(&opt.stream, "stream", opt.stream, "Do not just dump logs to disk, but also stream them to stdout")
	pflag.StringVar(&opt.streamPrefix, "prefix", opt.streamPrefix, "Prefix pattern to put at the beginning of each streamed line (pn = Pod name, pN = Pod namespace, c = container name)")
	pflag.BoolVar(&opt.oneShot, "oneshot", opt.oneShot, "Dump logs, but do not tail the containers (i.e. exit after downloading the current state)")
	pflag.BoolVar(&opt.dumpMetadata, "metadata", opt.dumpMetadata, "Dump Pods additionally as YAML (note that this can include secrets in environment variables)")
	pflag.BoolVar(&opt.dumpEvents, "events", opt.dumpEvents, "Dump events for each matching Pod as a human readable log file (note: label selectors are not respected)")
	pflag.BoolVar(&opt.dumpRawEvents, "events-raw", opt.dumpRawEvents, "Dump events for each matching Pod as YAML (note: label selectors are not respected)")
	pflag.BoolVarP(&opt.verbose, "verbose", "v", opt.verbose, "Enable more verbose output")
	pflag.BoolVarP(&opt.version, "version", "V", opt.version, "Show version info and exit immediately")
	pflag.Parse()

	if opt.version {
		printVersion()
		return
	}

	// //////////////////////////////////////
	// setup logging

	var log = logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC1123,
	})

	if opt.verbose {
		log.SetLevel(logrus.DebugLevel)
	}

	// //////////////////////////////////////
	// validate CLI flags

	if opt.kubeconfig == "" {
		opt.kubeconfig = os.Getenv("KUBECONFIG")
	}

	var labelSelector labels.Selector
	if opt.labels != "" {
		var err error
		if labelSelector, err = labels.Parse(opt.labels); err != nil {
			log.Fatalf("Invalid label selector: %v", err)
		}
	}

	args := pflag.Args()

	hasNames := len(args) > 0
	if hasNames && opt.labels != "" {
		log.Fatal("Cannot specify both resource names and a label selector at the same time.")
	}

	if !hasNames && len(opt.namespaces) == 0 {
		log.Fatal("At least a namespace or a resource name pattern must be given.")
	}

	if opt.directory == "" {
		opt.directory = fmt.Sprintf("protokol-%s", time.Now().Format("2006.01.02T15.04.05"))
	}

	log.WithField("directory", opt.directory).Info("Storing logs on disk.")

	coll, err := collector.NewDiskCollector(opt.directory, opt.flatFiles, opt.dumpEvents, opt.dumpRawEvents)
	if err != nil {
		log.Fatalf("Failed to create log collector: %v", err)
	}

	if opt.stream {
		stdoutCollector, err := collector.NewStreamCollector(opt.streamPrefix)
		if err != nil {
			log.Fatalf("Failed to create log collector: %v", err)
		}

		coll, err = collector.NewMultiplexCollector(coll, stdoutCollector)
		if err != nil {
			log.Fatalf("Failed to create log collector: %v", err)
		}
	}

	// //////////////////////////////////////
	// setup kubernetes client

	log.Debug("Creating Kubernetes clientset…")

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = opt.kubeconfig

	deferred := clientcmd.NewInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}, os.Stdin)
	config, err := deferred.ClientConfig()
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes clientset: %v", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create dynamic Kubernetes client: %v", err)
	}

	// //////////////////////////////////////
	// start to watch pods & potentially events

	podResourceInterface := dynamicClient.Resource(schema.GroupVersionResource{
		Version:  "v1",
		Resource: "pods",
	})

	eventResourceInterface := dynamicClient.Resource(schema.GroupVersionResource{
		Version:  "v1",
		Resource: "events",
	})

	if opt.dumpEvents || opt.dumpRawEvents {
		log.Debug("Starting to watch pods & events…")
	} else {
		log.Debug("Starting to watch pods…")
	}

	// to use the retrywatcher, we need a start revision; setting this to empty or "0"
	// is not supported, so we need a real revision; to achieve this we simply create
	// a "standard" watcher, takes the first event and its resourceVersion as the
	// starting point for the second, longlived retrying watcher
	initialPods, resourceVersion, err := getStartPods(rootCtx, clientset, opt.labels)
	if err != nil {
		log.Fatalf("Failed to determine initial resourceVersion: %v", err)
	}

	var initialEvents []corev1.Event
	if opt.dumpEvents || opt.dumpRawEvents {
		initialEvents, err = getStartEvents(rootCtx, clientset, opt.labels)
		if err != nil {
			log.Fatalf("Failed to retrieve initial events: %v", err)
		}
	}

	var (
		podWatcher   watch.Interface
		eventWatcher watch.Interface
	)

	if !opt.oneShot {
		podWatcher, err = watchtools.NewRetryWatcher(resourceVersion, &watchContextInjector{
			ctx: rootCtx,
			ri:  podResourceInterface,
		})
		if err != nil {
			log.Fatalf("Failed to create watch for pods: %v", err)
		}

		eventWatcher, err = watchtools.NewRetryWatcher(resourceVersion, &watchContextInjector{
			ctx: rootCtx,
			ri:  eventResourceInterface,
		})
		if err != nil {
			log.Fatalf("Failed to create watch for events: %v", err)
		}
	}

	watcherOpts := watcher.Options{
		LabelSelector:  labelSelector,
		Namespaces:     opt.namespaces,
		ResourceNames:  args,
		ContainerNames: opt.containerNames,
		RunningOnly:    opt.live,
		OneShot:        opt.oneShot,
		DumpMetadata:   opt.dumpMetadata,
		DumpEvents:     opt.dumpEvents || opt.dumpRawEvents,
	}

	w := watcher.NewWatcher(clientset, coll, log, initialPods, initialEvents, watcherOpts)
	w.Watch(rootCtx, podWatcher, eventWatcher)
}

func getStartPods(ctx context.Context, cs *kubernetes.Clientset, labelSelector string) ([]corev1.Pod, string, error) {
	pods, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to perform list on Pods: %w", err)
	}

	return pods.Items, pods.ResourceVersion, nil
}

func getStartEvents(ctx context.Context, cs *kubernetes.Clientset, labelSelector string) ([]corev1.Event, error) {
	events, err := cs.CoreV1().Events("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to perform list on Events: %w", err)
	}

	return events.Items, nil
}

type watchContextInjector struct {
	ctx context.Context
	ri  dynamic.ResourceInterface
}

func (cw *watchContextInjector) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return cw.ri.Watch(cw.ctx, options)
}
