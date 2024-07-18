/*
Package envoysink provides a gRPC server for the envoy metrics service sink.

External envoy proxies used to offload the traffic from the interceptor can push their metrics
to this server. The interceptor admin endpoint aggregates internal and external queues to a single
metric per HTTPScaledObject which is used by KEDA for scaling.
*/
package envoysink

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	envoycpv3 "github.com/envoyproxy/go-control-plane/envoy/service/metrics/v3"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"k8s.io/client-go/tools/cache"

	"github.com/kedacore/http-add-on/operator/apis/http/v1alpha1"
	"github.com/kedacore/http-add-on/operator/generated/informers/externalversions"
	informershttpv1alpha1 "github.com/kedacore/http-add-on/operator/generated/informers/externalversions/http/v1alpha1"
	"github.com/kedacore/http-add-on/pkg/queue"
)

const (
	// EnvoyClusterNameAnnotation is the annotation key for the envoy cluster name.
	EnvoyClusterNameAnnotation = "http.kedify.io/envoy-cluster-name"
	externalProxyMetricKey     = "http.kedify.io/external-proxy-metric-key"

	// envoy metrics representing RPS and pending requests.
	envoyMetricRPS         = "cluster.upstream_rq_total"
	envoyMetricConcurrency = "cluster.upstream_rq_active"

	// EnvoyMetricLabelClusterName is the label name for the cluster name.
	EnvoyMetricLabelClusterName = "envoy.cluster_name"
)

type counter interface {
	queue.Counter

	// SetRPS sets the RPS for the given host.
	SetRPS(host string, rps int) error
	// SetConcurrency sets the concurrency for the given host.
	SetConcurrency(host string, concurrency int) error
	// KeyExists checks if the key exists in the counter.
	KeyExists(host string) bool
}

// metricsServiceServer is a server for the metrics service.
type metricsServiceServer struct {
	envoycpv3.UnimplementedMetricsServiceServer

	log logr.Logger

	// concurrentMap is a map of gRPC stream id to map of host to value, access is protected by lock.
	concurrentMap     map[string]map[string]float64
	concurrentMapLock sync.RWMutex

	// counter is the counter to store the metrics and calculate the rate.
	counter counter

	// envoyClusterToHSOMap is a map of envoy cluster_name to the HTTPScaledObject.
	envoyClusterToHSOMap     map[string]*v1alpha1.HTTPScaledObject
	envoyClusterToHSOMapLock sync.RWMutex
}

// NewMetricsServiceServer creates a new metrics service server.
func NewMetricsServiceServer(lggr logr.Logger, counter counter, sharedInformerFactory externalversions.SharedInformerFactory, namespace string) (*metricsServiceServer, error) {
	s := &metricsServiceServer{
		log:                      lggr,
		concurrentMap:            map[string]map[string]float64{},
		concurrentMapLock:        sync.RWMutex{},
		counter:                  counter,
		envoyClusterToHSOMap:     map[string]*v1alpha1.HTTPScaledObject{},
		envoyClusterToHSOMapLock: sync.RWMutex{},
	}
	err := s.setupHTTPScaledObjectInformer(sharedInformerFactory, namespace)
	return s, err
}

// StreamMetrics is the implementation of the StreamMetrics RPC method.
func (s *metricsServiceServer) StreamMetrics(stream envoycpv3.MetricsService_StreamMetricsServer) error {
	id := uuid.New().String()
	s.concurrentMapLock.Lock()
	s.concurrentMap[id] = map[string]float64{}
	s.concurrentMapLock.Unlock()
	for {
		m, err := stream.Recv()
		if err != nil {
			return err
		}
		s.processMetrics(id, m)
	}
	s.concurrentMapLock.Lock()
	delete(s.concurrentMap, id)
	s.concurrentMapLock.Unlock()
	return nil
}

func (s *metricsServiceServer) setupHTTPScaledObjectInformer(sharedInformerFactory externalversions.SharedInformerFactory, namespace string) error {
	httpScaledObjects := informershttpv1alpha1.New(sharedInformerFactory, namespace, nil).HTTPScaledObjects()
	informer, ok := httpScaledObjects.Informer().(cache.SharedIndexInformer)
	if !ok {
		return errors.New("informer is not cache.sharedIndexInformer")
	}
	_, err := informer.AddEventHandler(s)
	return err
}

// Run starts the metrics service server.
func (s *metricsServiceServer) Run(ctx context.Context, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	envoycpv3.RegisterMetricsServiceServer(grpcServer, s)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			s.log.Error(err, "failed to serve")
		}
	}()
	return nil
}

// processMetrics processes the pushed metrics from envoys
func (s *metricsServiceServer) processMetrics(id string, m *envoycpv3.StreamMetricsMessage) {
	for _, m := range m.EnvoyMetrics {
		if m.GetName() == envoyMetricRPS || m.GetName() == envoyMetricConcurrency {
			for _, metricData := range m.Metric {
				for _, label := range metricData.Label {
					if label.GetName() == EnvoyMetricLabelClusterName {
						host := s.convertEnvoyClusterNameToHostKey(label.GetValue())
						if host == "" {
							break
						}
						switch m.GetName() {
						case envoyMetricRPS:
							if metricData.GetCounter().GetValue() == 0 {
								// in order to allow fast scaling with RPS, leading 0 buckets in window should not be inserted
								// the RPS calculation works well with empty trailing and middle buckets
								continue
							}
							s.counter.SetRPS(host, int(metricData.GetCounter().GetValue()))
						case envoyMetricConcurrency:
							s.concurrentMapLock.Lock()
							s.concurrentMap[id][host] = metricData.GetGauge().GetValue()
							s.concurrentMapLock.Unlock()
							val := 0
							s.concurrentMapLock.RLock()
							for _, v := range s.concurrentMap {
								val += int(v[host])
							}
							s.concurrentMapLock.RUnlock()
							s.counter.SetConcurrency(host, val)
						}
					}
				}
			}
		}
	}
}

// OnAdd handles the creation of the HTTPScaledObject.
func (s *metricsServiceServer) OnAdd(obj any, _ bool) {
	hso, ok := obj.(*v1alpha1.HTTPScaledObject)
	if !ok {
		s.log.Info("not a HTTPScaledObject", "obj", obj, "type", fmt.Sprintf("%T", obj))
		return
	}

	clusterName := getClusterNameFromAnnotations(hso.Annotations)
	if clusterName == "" {
		s.log.V(4).Info("cluster name not found", "hso", hso)
		return
	}
	s.addClusterNameToMetrics(clusterName, hso)
}

// OnUpdate handles the update of the HTTPScaledObject.
func (s *metricsServiceServer) OnUpdate(o, n any) {
	hsoOld, ok := o.(*v1alpha1.HTTPScaledObject)
	if !ok {
		s.log.Info("not a HTTPScaledObject", "objOld", o, "type", fmt.Sprintf("%T", o))
		return
	}
	hsoNew, ok := n.(*v1alpha1.HTTPScaledObject)
	if !ok {
		s.log.Info("not a HTTPScaledObject", "objNew", n, "type", fmt.Sprintf("%T", n))
		return
	}
	oldClusterName := getClusterNameFromAnnotations(hsoOld.Annotations)
	newClusterName := getClusterNameFromAnnotations(hsoNew.Annotations)
	if oldClusterName == newClusterName {
		s.log.V(4).Info("cluster name did not change", "hso", hsoNew)
		if newClusterName != "" {
			host := hsoNew.Namespace + "/" + hsoOld.Name
			if !s.counter.KeyExists(host) {
				s.addClusterNameToMetrics(newClusterName, hsoNew)
			}
		}
		return
	}

	if oldClusterName != "" {
		s.deleteClusterNameFromMetrics(oldClusterName, hsoOld)
	}
	if newClusterName != "" {
		s.addClusterNameToMetrics(newClusterName, hsoNew)
	}
}

// OnDelete handles the deletion of the HTTPScaledObject.
func (s *metricsServiceServer) OnDelete(obj any) {
	hso, ok := obj.(*v1alpha1.HTTPScaledObject)
	if !ok {
		s.log.Info("not a HTTPScaledObject", "obj", obj, "type", fmt.Sprintf("%T", obj))
		return
	}
	clusterName := getClusterNameFromAnnotations(hso.Annotations)
	if clusterName == "" {
		s.log.V(4).Info("cluster name not found", "hso", hso)
		return
	}
	s.deleteClusterNameFromMetrics(clusterName, hso)
}

// getClusterNameFromAnnotations gets the cluster name from the annotations.
func getClusterNameFromAnnotations(annotations map[string]string) string {
	if annotations[externalProxyMetricKey] != "" {
		return annotations[externalProxyMetricKey]
	}
	return annotations[EnvoyClusterNameAnnotation]
}

// addCluserNameToMetrics adds the cluster name to the metrics.
func (s *metricsServiceServer) addClusterNameToMetrics(clusterName string, hso *v1alpha1.HTTPScaledObject) {
	window := 1 * time.Minute
	granularity := 1 * time.Second
	if hso.Spec.ScalingMetric != nil && hso.Spec.ScalingMetric.Rate != nil {
		window = hso.Spec.ScalingMetric.Rate.Window.Duration
		granularity = hso.Spec.ScalingMetric.Rate.Granularity.Duration
	}
	host := hso.Namespace + "/" + hso.Name
	s.counter.EnsureKey(host, window, granularity)
	s.envoyClusterToHSOMapLock.Lock()
	s.envoyClusterToHSOMap[clusterName] = hso
	s.envoyClusterToHSOMapLock.Unlock()
}

// deleteCluserNameFromMetrics deletes the cluster name from the metrics.
func (s *metricsServiceServer) deleteClusterNameFromMetrics(clusterName string, hso *v1alpha1.HTTPScaledObject) {
	s.envoyClusterToHSOMapLock.Lock()
	delete(s.envoyClusterToHSOMap, clusterName)
	s.envoyClusterToHSOMapLock.Unlock()
	host := hso.Namespace + "/" + hso.Name
	s.counter.RemoveKey(host)
}

// convertEnvoyClusterNameToHostKey converts the envoy cluster name to the host key.
func (s *metricsServiceServer) convertEnvoyClusterNameToHostKey(clusterName string) string {
	s.envoyClusterToHSOMapLock.RLock()
	defer s.envoyClusterToHSOMapLock.RUnlock()
	hso, ok := s.envoyClusterToHSOMap[clusterName]
	if !ok {
		s.log.V(1).Info("cluster name not found for conversion", "clusterName", clusterName)
		return ""
	}
	return hso.Namespace + "/" + hso.Name
}
