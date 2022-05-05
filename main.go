package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.xrstf.de/loks/pkg/collector"
	"go.xrstf.de/loks/pkg/watcher"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type options struct {
	kubeconfig     string
	directory      string
	namespaces     []string
	containerNames []string
	labels         string
	live           bool
	flatFiles      bool
	verbose        bool
}

func main() {
	rootCtx := context.Background()
	opt := options{}

	pflag.StringVar(&opt.kubeconfig, "kubeconfig", opt.kubeconfig, "kubeconfig file to use (uses $KUBECONFIG by default)")
	pflag.StringArrayVarP(&opt.namespaces, "namespace", "n", opt.namespaces, "Kubernetes namespace to watch resources in (supports glob expression) (can be given multiple times)")
	pflag.StringArrayVarP(&opt.containerNames, "container", "c", opt.containerNames, "Container names to store logs for (supports glob expression) (can be given multiple times)")
	pflag.StringVarP(&opt.labels, "labels", "l", opt.labels, "Label-selector as an alternative to specifying resource names")
	pflag.StringVarP(&opt.directory, "output", "o", opt.directory, "Directory where logs should be stored")
	pflag.BoolVarP(&opt.flatFiles, "flat", "f", opt.flatFiles, "Do not create directory per namespace, but put all logs in the same directory")
	pflag.BoolVar(&opt.live, "live", opt.live, "Only consider running pods, ignore completed/failed pods")
	pflag.BoolVarP(&opt.verbose, "verbose", "v", opt.verbose, "Enable more verbose output")
	pflag.Parse()

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

	if opt.labels != "" {
		if _, err := labels.Parse(opt.labels); err != nil {
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
		opt.directory = fmt.Sprintf("loks-%s", time.Now().Format("2006.01.02T15.04.05"))
	}

	log.WithField("directory", opt.directory).Info("Storing logs on disk.")

	c, err := collector.NewDiskCollector(opt.directory, opt.flatFiles)
	if err != nil {
		log.Fatalf("Failed to create log collector: %v", err)
	}

	// //////////////////////////////////////
	// setup kubernetes client

	log.Debug("Creating Kubernetes clientset…")

	config, err := clientcmd.BuildConfigFromFlags("", opt.kubeconfig)
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
	// start to watch pods

	resourceInterface := dynamicClient.Resource(schema.GroupVersionResource{
		Version:  "v1",
		Resource: "pods",
	})

	log.Debug("Starting to watch pods…")

	wi, err := resourceInterface.Watch(rootCtx, metav1.ListOptions{
		LabelSelector: opt.labels,
	})
	if err != nil {
		log.Fatalf("Failed to create watch for pods: %v", err)
	}

	w := watcher.NewWatcher(clientset, c, log, opt.namespaces, args, opt.containerNames, opt.live)
	w.Watch(rootCtx, wi)
}
