package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	statsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type options struct {
	dataDir string
}

func defaultOptions() *options {
	return &options{}
}

func bindOptions(fs *flag.FlagSet, defaults *options) *options {
	fs.StringVar(&defaults.dataDir, "data", defaults.dataDir, "Path to data directory.")
	return defaults
}

func (o *options) validate() error {
	if o.dataDir == "" {
		return errors.New("--data is required")
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

	raw, err := os.ReadFile(filepath.Join(opts.dataDir, "podInfo.json"))
	if err != nil {
		logrus.WithError(err).Fatal("failed to read pod info")
	}

	var podsByIdentifier map[string][]types.NamespacedName
	if err := json.Unmarshal(raw, &podsByIdentifier); err != nil {
		logrus.WithError(err).Fatal("failed to unmarshal pod info")
	}

	fields := logrus.Fields{}
	for k, v := range podsByIdentifier {
		fields[k] = v
	}
	logrus.WithFields(fields).Info("found control plane pods")

	identifierForPod := map[statsv1alpha1.PodReference]string{}
	for identifier, pods := range podsByIdentifier {
		for _, pod := range pods {
			identifierForPod[referenceFor(pod)] = identifier
		}
	}

	metrics := map[string]map[statsv1alpha1.PodReference]map[string][]metric{}
	for identifier, pods := range podsByIdentifier {
		metrics[identifier] = map[statsv1alpha1.PodReference]map[string][]metric{}
		for _, pod := range pods {
			metrics[identifier][referenceFor(pod)] = map[string][]metric{
				"cpu":    {},
				"memory": {},
			}
		}
	}
	if err := filepath.WalkDir(filepath.Join(opts.dataDir, "metrics"), func(path string, info os.DirEntry, err error) error {
		if err != nil || info == nil {
			return err
		}

		if filepath.Ext(path) != ".json" {
			return nil
		}

		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", path, err)
		}

		var summary statsv1alpha1.Summary
		if err := json.Unmarshal(raw, &summary); err != nil {
			return fmt.Errorf("failed to unmarshal %s: %w", path, err)
		}

		for _, pod := range summary.Pods {
			pod.PodRef.UID = ""
			if label, exists := identifierForPod[pod.PodRef]; exists {
				metrics[label][pod.PodRef]["cpu"] = append(metrics[label][pod.PodRef]["cpu"], metric{
					timestamp: pod.CPU.Time,
					value:     pod.CPU.UsageCoreNanoSeconds,
				})
				metrics[label][pod.PodRef]["memory"] = append(metrics[label][pod.PodRef]["memory"], metric{
					timestamp: pod.Memory.Time,
					value:     pod.Memory.WorkingSetBytes,
				})
			}
		}

		return nil
	}); err != nil {
		logrus.WithError(err).Fatal("failed to read metrics")
	}

	data := map[string]map[string][]timeseries{}
	for podLabel, pods := range metrics {
		for _, items := range pods {
			for metricLabel, values := range items {
				series := timeseries{}
				sort.Slice(values, func(i, j int) bool {
					return values[i].timestamp.Time.Before(values[j].timestamp.Time)
				})
				for _, value := range values {
					series.Times = append(series.Times, value.timestamp.Time.Format(time.RFC3339Nano))
					series.Values = append(series.Values, value.value)
				}
				if _, exists := data[metricLabel]; !exists {
					data[metricLabel] = map[string][]timeseries{}
				}
				data[metricLabel][podLabel] = append(data[metricLabel][podLabel], series)
			}
		}
	}

	rawData, err := json.Marshal(data)
	if err != nil {
		logrus.WithError(err).Fatal("failed to marshal raw data")
	}

	if err := os.WriteFile(filepath.Join(opts.dataDir, "data.json"), rawData, 0666); err != nil {
		logrus.WithError(err).Fatal("failed to write raw data")
	}
}

type metric struct {
	timestamp metav1.Time
	value     *uint64
}

type timeseries struct {
	Times  []string  `json:"times"`
	Values []*uint64 `json:"values"`
}

func referenceFor(namespacedName types.NamespacedName) statsv1alpha1.PodReference {
	return statsv1alpha1.PodReference{
		Name:      namespacedName.Name,
		Namespace: namespacedName.Namespace,
	}
}
