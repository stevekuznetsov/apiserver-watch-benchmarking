package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type options struct {
	kubeconfig string
	outputDir  string

	podSelectors string

	experiment                   string
	latentWatchExperimentOptions *latentWatchExperimentOptions
}

func defaultOptions() *options {
	return &options{
		podSelectors:                 "api:component=kube-apiserver|etcd:component=etcd",
		latentWatchExperimentOptions: defaultLatentWatchExperimentOptions(),
	}
}

func bindOptions(fs *flag.FlagSet, defaults *options) *options {
	fs.StringVar(&defaults.kubeconfig, "kubeconfig", defaults.kubeconfig, "Path to kubeconfig file.")
	fs.StringVar(&defaults.outputDir, "output", defaults.outputDir, "Path to output directory.")
	fs.StringVar(&defaults.podSelectors, "pod-selectors", defaults.podSelectors, "Pipe-delimited list of pod selectors for components to monitor.")
	fs.StringVar(&defaults.experiment, "experiment", defaults.experiment, "Experiment to run.")
	bindLatentWatchExperimentOptions(fs, defaults.latentWatchExperimentOptions)
	return defaults
}

type experiment string

const (
	latentWatchExperiment experiment = "latent-watch"
)

type latentWatchExperimentOptions struct {
	count int
	rate  int
}

func defaultLatentWatchExperimentOptions() *latentWatchExperimentOptions {
	return &latentWatchExperimentOptions{
		count: 10000,
		rate:  100,
	}
}

func bindLatentWatchExperimentOptions(fs *flag.FlagSet, defaults *latentWatchExperimentOptions) *latentWatchExperimentOptions {
	prefix := "latent-watch."
	fs.IntVar(&defaults.count, prefix+"count", defaults.count, "Number of watches to start.")
	fs.IntVar(&defaults.rate, prefix+"rate", defaults.rate, "Rate of watch starts, in Hertz.")
	return defaults
}

func (o *options) validate() error {
	if o.kubeconfig == "" {
		return errors.New("--kubeconfig is required")
	}
	if o.outputDir == "" {
		return errors.New("--output is required")
	}
	if o.experiment == "" {
		return errors.New("--experiment is required")
	} else {
		experiments := sets.New[experiment](latentWatchExperiment)
		if !experiments.Has(experiment(o.experiment)) {
			return fmt.Errorf("unrecognized --experiment %s, must be one of %v", o.experiment, experiments.UnsortedList())
		}
	}
	return nil
}

func main() {
	opts := defaultOptions()
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	opts = bindOptions(fs, opts)
	if err := fs.Parse(os.Args[1:]); err != nil {
		logrus.WithError(err).Fatal("failed to parse arguments")
	}
	if err := opts.validate(); err != nil {
		logrus.WithError(err).Fatal("invalid options")
	}

	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	loader.ExplicitPath = opts.kubeconfig
	apiConfig, err := loader.Load()
	if err != nil {
		logrus.WithError(err).Fatal("could not load kubeconfig")
	}
	clientConfig, err := clientcmd.NewDefaultClientConfig(*apiConfig, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		logrus.WithError(err).Fatal("could not load client configuration")
	}
	clientConfig.QPS = -1

	client, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		logrus.WithError(err).Fatal("could not load create client")
	}

	if err := os.RemoveAll(opts.outputDir); err != nil {
		logrus.WithError(err).Fatal("could not clear output dir")
	}
	if err := os.MkdirAll(opts.outputDir, 0777); err != nil {
		logrus.WithError(err).Fatal("could not create output dir")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer func() {
		cancel()
	}()

	logrus.Info("Waiting for the API server to be ready.")
	var lastHealthContent string
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		reqContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		result := client.RESTClient().Get().AbsPath("/healthz").Do(reqContext)
		status := 0
		result.StatusCode(&status)
		if status == 200 {
			return true, nil
		}
		lastHealthBytes, _ := result.Raw()
		lastHealthContent = fmt.Sprintf("%d: %s", status, string(lastHealthBytes))
		return false, nil
	}); err != nil {
		logrus.WithError(err).Fatalf("did not find API server ready, last response to /healthz: %s", lastHealthContent)
	}

	selectors := map[string]labels.Selector{}
	parts := strings.Split(opts.podSelectors, "|")
	for _, part := range parts {
		subParts := strings.Split(part, ":")
		if len(subParts) != 2 {
			logrus.Fatalf("--pod-selectors invalid: selector %s is not of form identifier:selector", part)
		}
		identifier, selectorString := subParts[0], subParts[1]
		selector, err := labels.Parse(selectorString)
		if err != nil {
			logrus.Fatalf("--pod-selectors invalid: selector %s invalid: %v", part, err)
		}
		selectors[identifier] = selector
	}

	nodes, err := recordPodInfo(ctx, client, opts.outputDir, selectors)
	if err != nil {
		logrus.WithError(err).Fatal("could not record pod info")
	}

	if err := setupContainerMetricsMonitors(ctx, client, nodes, opts.outputDir); err != nil {
		logrus.WithError(err).Fatal("could not monitor container metrics")
	}

	switch experiment(opts.experiment) {
	case latentWatchExperiment:
		if err := runLatentWatchExperiment(ctx, client, opts.outputDir, opts.latentWatchExperimentOptions); err != nil {
			logrus.WithError(err).Fatal("could not run latent watch benchmark")
		}
	}
	logrus.Info("Finished benchmark.")
}

func recordPodInfo(ctx context.Context, client *kubernetes.Clientset, outputDir string, selectors map[string]labels.Selector) ([]string, error) {
	logrus.Info("Recording control plane pod info")
	podsByIdentifier := map[string][]types.NamespacedName{}
	nodes := sets.New[string]()
	for identifier, selector := range selectors {
		pods, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
		if err != nil {
			return nil, fmt.Errorf("could not list %s pods: %w", identifier, err)
		}
		var names []types.NamespacedName
		for _, pod := range pods.Items {
			names = append(names, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name})
			nodes.Insert(pod.Spec.NodeName)
		}
		podsByIdentifier[identifier] = names
	}

	raw, err := json.Marshal(podsByIdentifier)
	if err != nil {
		return nil, fmt.Errorf("could not marshal pod info: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "podInfo.json"), raw, 0777); err != nil {
		return nil, fmt.Errorf("could not write pod info: %w", err)
	}
	fields := logrus.Fields{}
	for k, v := range podsByIdentifier {
		fields[k] = v
	}
	logrus.WithFields(fields).Info("found control plane pods")
	return nodes.UnsortedList(), nil
}

func setupContainerMetricsMonitors(ctx context.Context, client *kubernetes.Clientset, nodes []string, outputDir string) error {
	logrus.Info("Setting up container metrics monitoring")
	for _, node := range nodes {
		nodeDir := filepath.Join(outputDir, "metrics", node)
		if err := os.MkdirAll(nodeDir, 0777); err != nil {
			return fmt.Errorf("could not create output dir for node %s: %w", node, err)
		}
		go monitorContainerMetrics(ctx, client.RESTClient(), node, nodeDir)
	}

	return nil
}

func monitorContainerMetrics(ctx context.Context, client rest.Interface, nodeName, outputDir string) {
	logrus.Info("Setting up container metrics monitoring for node %s", nodeName)
	index := 0
	if err := wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true, func(ctx context.Context) (done bool, err error) {
		go func(index int) {
			result := client.Get().AbsPath("/api/v1/nodes/" + nodeName + "/proxy/stats/summary").Do(ctx)
			raw, err := result.Raw()
			if err != nil {
				logrus.WithError(err).Errorf("failed to fetch container metrics")
			}
			if err := os.WriteFile(filepath.Join(outputDir, strconv.Itoa(index)+".json"), raw, 0777); err != nil {
				logrus.WithError(err).Errorf("failed to record container metrics")
			}
		}(index)
		index++
		return false, nil
	}); err != nil {
		logrus.WithError(err).Errorf("failed to monitor container metrics")
	}
}

func runLatentWatchExperiment(ctx context.Context, client *kubernetes.Clientset, outputDir string, opts *latentWatchExperimentOptions) error {
	logrus.Info("Running latent watch experiment")
	var issued int
	watchers := make(chan watch.Interface, opts.count)
	timeChan := make(chan time.Time)
	var timestamps []time.Time
	go func() {
		for t := range timeChan {
			timestamps = append(timestamps, t)
		}
	}()

	ticker := time.NewTicker(time.Second / time.Duration(opts.rate))
	defer ticker.Stop()
	func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				go func() {
					watcher, err := client.CoreV1().ConfigMaps(strconv.Itoa(issued)).Watch(ctx, metav1.ListOptions{})
					if err != nil {
						logrus.WithError(err).Error("failed to start watch")
					}
					timeChan <- time.Now()
					watchers <- watcher
				}()
				issued++
			}
			if issued%(opts.count/10) == 0 {
				logrus.Infof("issued %d/%d (%.0f%%) watches", issued, opts.count, 100*(float64(issued)/float64(opts.count)))
			}
			if issued == opts.count {
				return
			}
		}
	}()
	raw, err := json.Marshal(timestamps)
	if err != nil {
		return fmt.Errorf("failed to encode latent watch timing: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "latent-watch.json"), raw, 0666); err != nil {
		return fmt.Errorf("failed to record latent watch timing: %w", err)
	}

	logrus.Info("Finished latent watch experiment")
	return nil
}
