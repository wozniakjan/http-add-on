package envoycp

import (
	"fmt"
	"math"
	"sort"

	duration "github.com/golang/protobuf/ptypes/duration"
	"golang.org/x/exp/constraints"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	wellknown "github.com/envoyproxy/go-control-plane/pkg/wellknown"

	httpaddonv1alpha1 "github.com/kedacore/http-add-on/operator/apis/http/v1alpha1"
)

// getResourcesVersion returns the version of the resources that will form the snapshot
func getResourcesVersion(resources map[resource.Type][]types.Resource) string {
	var content []byte
	snapResourceTypes := []resource.Type{resource.ListenerType, resource.ClusterType}
	for _, rt := range snapResourceTypes {
		resource := resources[rt]
		for _, r := range resource {
			mr, err := cachev3.MarshalResource(r)
			if err != nil {
				return ""
			}
			content = append(content, mr...)
		}
	}
	return cachev3.HashResource(content)
}

// snapVersion returns the version of the snapshot
func snapVersion(snap *cachev3.Snapshot) string {
	listenerVersion := snap.GetVersion(resource.ListenerType)
	clustersVersion := snap.GetVersion(resource.ClusterType)
	return fmt.Sprintf("%s-%s", listenerVersion, clustersVersion)
}

// getManager returns the slice of routes translated from HTTPScaledObject spec
// if there is no HTTPScaledObject.Spec.PathPrefixes, create single envoy route with prefix "/"
func getRoutes(hsoKey string, hso *httpaddonv1alpha1.HTTPScaledObject) []*routev3.Route {
	routes := make([]*routev3.Route, len(hso.Spec.PathPrefixes))
	for i, pathPrefix := range hso.Spec.PathPrefixes {
		routes[i] = &routev3.Route{
			Match: &routev3.RouteMatch{
				PathSpecifier: &routev3.RouteMatch_Prefix{
					Prefix: pathPrefix,
				},
			},
			Action: &routev3.Route_Route{
				Route: &routev3.RouteAction{
					ClusterSpecifier: &routev3.RouteAction_Cluster{
						Cluster: hso.Name,
					},
				},
			},
		}
	}
	if len(routes) == 0 {
		routes = []*routev3.Route{
			{
				Match: &routev3.RouteMatch{
					PathSpecifier: &routev3.RouteMatch_Prefix{
						Prefix: "/",
					},
				},
				Action: &routev3.Route_Route{
					Route: &routev3.RouteAction{
						ClusterSpecifier: &routev3.RouteAction_Cluster{
							Cluster: hsoKey,
						},
						HostRewriteSpecifier: &routev3.RouteAction_AutoHostRewrite{
							AutoHostRewrite: wrapperspb.Bool(false),
						},
					},
				},
			},
		}
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Match.PathSpecifier.(*routev3.RouteMatch_Prefix).Prefix < routes[j].Match.PathSpecifier.(*routev3.RouteMatch_Prefix).Prefix
	})
	return routes
}

// getManager returns the HTTPConnectionManager from the listener
func getManager(listener *listenerv3.Listener) (*httpv3.HttpConnectionManager, error) {
	tc, ok := listener.FilterChains[0].Filters[0].ConfigType.(*listenerv3.Filter_TypedConfig)
	if !ok {
		return nil, fmt.Errorf("failed to cast filter config to typed config")
	}
	httpConnManager := &httpv3.HttpConnectionManager{}
	if err := anypb.UnmarshalTo(tc.TypedConfig, httpConnManager, proto.UnmarshalOptions{}); err != nil {
		return nil, fmt.Errorf("failed to unmarshal anypb to http connection manager: %w", err)
	}
	return httpConnManager, nil
}

// updateManager performs in-place update of the HTTPConnectionManager with new routes
func updateManager(hsoKey string, hso *httpaddonv1alpha1.HTTPScaledObject, httpConnManager *httpv3.HttpConnectionManager) error {
	routes := getRoutes(hsoKey, hso)
	rc, ok := httpConnManager.GetRouteSpecifier().(*httpv3.HttpConnectionManager_RouteConfig)
	if !ok {
		return fmt.Errorf("failed to cast route config to routev3.RouteConfiguration")
	}
	if !hso.DeletionTimestamp.IsZero() {
		for i, vh := range rc.RouteConfig.VirtualHosts {
			if vh.Name == hsoKey {
				rc.RouteConfig.VirtualHosts = append(rc.RouteConfig.VirtualHosts[:i], rc.RouteConfig.VirtualHosts[i+1:]...)
				break
			}
		}
		return nil
	}
	routeConfig := rc.RouteConfig
	for _, vh := range routeConfig.VirtualHosts {
		if vh.Name == hsoKey {
			vh.Domains = hso.Spec.Hosts
			vh.Routes = routes
			return nil
		}
	}

	routeConfig.VirtualHosts = append(routeConfig.VirtualHosts, &routev3.VirtualHost{
		Name:    hsoKey,
		Domains: hso.Spec.Hosts,
		Routes:  routes,
	})
	sort.Slice(routeConfig.VirtualHosts, func(i, j int) bool {
		return routeConfig.VirtualHosts[i].Name < routeConfig.VirtualHosts[j].Name
	})
	return nil
}

// updateListener performs in-place update of the listener with updated HTTPConnectionManager
func updateListener(hsoKey string, hso *httpaddonv1alpha1.HTTPScaledObject, listener *listenerv3.Listener) error {
	httpConnManager, err := getManager(listener)
	if err != nil {
		return fmt.Errorf("failed to get http connection manager: %w", err)
	}
	if err := updateManager(hsoKey, hso, httpConnManager); err != nil {
		return fmt.Errorf("failed to update http connection manager: %w", err)
	}
	httpConnManagerAny, err := anypb.New(httpConnManager)
	if err != nil {
		return fmt.Errorf("failed to anypb http connection manager: %w", err)
	}
	listener.FilterChains[0].Filters[0].ConfigType = &listenerv3.Filter_TypedConfig{TypedConfig: httpConnManagerAny}
	return nil
}

// getListener returns the listener from the resources map, if it doesn't exist, creates a new listener with initial configuration
func getListener(resources map[resource.Type][]types.Resource) (*listenerv3.Listener, error) {
	var listener *listenerv3.Listener
	res, ok := resources[resource.ListenerType]
	if !ok {
		httpRouter, err := anypb.New(&routerv3.Router{})
		if err != nil {
			return nil, fmt.Errorf("failed to anypb router: %w", err)
		}

		httpConnManager := &httpv3.HttpConnectionManager{
			StatPrefix: "kedify-proxy",
			CodecType:  httpv3.HttpConnectionManager_AUTO,
			RouteSpecifier: &httpv3.HttpConnectionManager_RouteConfig{
				RouteConfig: &routev3.RouteConfiguration{
					Name:         "kedify-proxy",
					VirtualHosts: []*routev3.VirtualHost{},
				},
			},
			HttpFilters: []*httpv3.HttpFilter{
				{
					Name: wellknown.Router,
					ConfigType: &httpv3.HttpFilter_TypedConfig{
						TypedConfig: httpRouter,
					},
				},
			},
		}
		httpConnManagerAny, err := anypb.New(httpConnManager)
		if err != nil {
			return nil, fmt.Errorf("failed to anypb http connection manager: %w", err)
		}

		listener = &listenerv3.Listener{
			Name: "kedify-proxy",
			Address: &corev3.Address{
				Address: &corev3.Address_SocketAddress{
					SocketAddress: &corev3.SocketAddress{
						Address: "0.0.0.0",
						PortSpecifier: &corev3.SocketAddress_PortValue{
							PortValue: 8080,
						},
					},
				},
			},
			FilterChains: []*listenerv3.FilterChain{
				{
					Filters: []*listenerv3.Filter{
						{
							Name: wellknown.HTTPConnectionManager,
							ConfigType: &listenerv3.Filter_TypedConfig{
								TypedConfig: httpConnManagerAny,
							},
						},
					},
				},
			},
		}
		resources[resource.ListenerType] = []types.Resource{listener}
	} else {
		listener = res[0].(*listenerv3.Listener)
	}
	return listener, nil
}

// getLoadBalancerEndpoint returns the load balancer endpoint for certain address and port
func getLoadBalancerEndpoint[T constraints.Integer](address string, port T) *endpointv3.LbEndpoint {
	return &endpointv3.LbEndpoint{
		HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
			Endpoint: &endpointv3.Endpoint{
				Address: &corev3.Address{
					Address: &corev3.Address_SocketAddress{
						SocketAddress: &corev3.SocketAddress{
							Address: address,
							PortSpecifier: &corev3.SocketAddress_PortValue{
								PortValue: uint32(port),
							},
						},
					},
				},
			},
		},
	}
}

// getClusters returns the clusters from the resources map, if it doesn't exist, creates a new cluster
// with initial configuration for the xDS control plane configuration
func getClusters(resources map[resource.Type][]types.Resource, cp Options) []*clusterv3.Cluster {
	clusterResources, ok := resources[resource.ClusterType]
	if !ok {
		lbEndpoint := getLoadBalancerEndpoint(cp.ControlPlaneHost, cp.ControlPlanePort)
		xdsCluster := &clusterv3.Cluster{
			Name: "xds_cluster",
			ConnectTimeout: &duration.Duration{
				Seconds: 2,
			},
			ClusterDiscoveryType: &clusterv3.Cluster_Type{
				Type: clusterv3.Cluster_STRICT_DNS,
			},
			LbPolicy:                  clusterv3.Cluster_ROUND_ROBIN,
			Http2ProtocolOptions:      &corev3.Http2ProtocolOptions{},
			UpstreamConnectionOptions: &clusterv3.UpstreamConnectionOptions{},
			LoadAssignment: &endpointv3.ClusterLoadAssignment{
				ClusterName: "xds_cluster",
				Endpoints: []*endpointv3.LocalityLbEndpoints{
					{
						LbEndpoints: []*endpointv3.LbEndpoint{lbEndpoint},
					},
				},
			},
		}
		resources[resource.ClusterType] = []types.Resource{xdsCluster}
		return []*clusterv3.Cluster{xdsCluster}
	} else {
		clusters := make([]*clusterv3.Cluster, len(clusterResources))
		for i, r := range clusterResources {
			clusters[i] = r.(*clusterv3.Cluster)
		}
		return clusters
	}
}

// updateClusters updates the clusters for the new HTTPScaledObject
func updateClusters(hsoKey string, cp Options, hso *httpaddonv1alpha1.HTTPScaledObject, clusters []*clusterv3.Cluster) ([]*clusterv3.Cluster, error) {
	if !hso.DeletionTimestamp.IsZero() {
		for i, cluster := range clusters {
			if cluster.Name == hsoKey {
				clusters = append(clusters[:i], clusters[i+1:]...)
				break
			}
		}
		return clusters, nil
	}
	for _, cluster := range clusters {
		if cluster.Name == hsoKey {
			return clusters, nil
		}
	}

	address := fmt.Sprintf("%v.%v.svc.%v", hso.Spec.ScaleTargetRef.Service, hso.Namespace, cp.ClusterDomain)
	lbEndpoint := getLoadBalancerEndpoint(address, hso.Spec.ScaleTargetRef.Port)
	cluster := &clusterv3.Cluster{
		Name: hsoKey,
		ConnectTimeout: &duration.Duration{
			Seconds: int64(math.Ceil(cp.ConnectionTimeout.Seconds())),
		},
		ClusterDiscoveryType: &clusterv3.Cluster_Type{
			Type: clusterv3.Cluster_STRICT_DNS,
		},
		LbPolicy: clusterv3.Cluster_ROUND_ROBIN,
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: hsoKey,
			Endpoints: []*endpointv3.LocalityLbEndpoints{
				{
					LbEndpoints: []*endpointv3.LbEndpoint{lbEndpoint},
				},
			},
		},
	}
	clusters = append(clusters, cluster)
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})
	return clusters, nil
}

// setClusters sets the clusters in the resources map
func setClusters(resources map[resource.Type][]types.Resource, clusters []*clusterv3.Cluster) {
	clusterResources := make([]types.Resource, len(clusters))
	for i, c := range clusters {
		clusterResources[i] = c
	}
	resources[resource.ClusterType] = clusterResources
}

// addResources adds the envoy resources to the snapshot cache based on the input values from the HTTPScaledObjects
// * listeners - only one shared for all of the HTTPScaledObjects
// * listeners.filter_chains - also only one shared for all of the HTTPScaledObjects
// * listeners.filter_chains.filters - also only one shared for all of the HTTPScaledObjects
//
// * listeners.filter_chains.filters.http_connection_manager.route_config.virtual_hosts - one for each HTTPScaledObject
// * listeners.filter_chains.filters.http_connection_manager.route_config.virtual_hosts.routes - one for each HTTPScaledObject.Spec.PathPrefix
//
// * clusters - one for each HTTPScaledObject
func addResources(resources map[resource.Type][]types.Resource, cp Options, hsos ...httpaddonv1alpha1.HTTPScaledObject) error {
	for _, hso := range hsos {
		hsoKey := fmt.Sprintf("%s/%s", hso.Namespace, hso.Name)
		listener, err := getListener(resources)
		if err != nil {
			return fmt.Errorf("failed to get listener: %w", err)
		}
		if err := updateListener(hsoKey, &hso, listener); err != nil {
			return fmt.Errorf("failed to update listener: %w", err)
		}

		clusters := getClusters(resources, cp)
		updatedClusters, err := updateClusters(hsoKey, cp, &hso, clusters)
		if err != nil {
			return fmt.Errorf("failed to update cluster: %w", err)
		}
		setClusters(resources, updatedClusters)
	}
	return nil
}
