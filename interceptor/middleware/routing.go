package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"

	"github.com/kedacore/http-add-on/interceptor/handler"
	httpv1alpha1 "github.com/kedacore/http-add-on/operator/apis/http/v1alpha1"
	"github.com/kedacore/http-add-on/pkg/routing"
	"github.com/kedacore/http-add-on/pkg/util"
)

var (
	kubernetesProbeUserAgent = regexp.MustCompile(`(^|\s)kube-probe/`)
	googleHCUserAgent        = regexp.MustCompile(`(^|\s)GoogleHC/`)

	healthCheckPathAnnotation      = "http.kedify.io/healthcheck-path"
	healthCheckResponseAnnotation  = "http.kedify.io/healthcheck-response"
	healthCheckResponseStatic      = "static"
	healthCheckResponsePassthrough = "passthrough"
)

// EndpointCheckFn is a function that checks if a workload has any active endpoints.
type EndpointCheckFn func(context.Context, string, string) (bool, error)

type Routing struct {
	endpointActive  EndpointCheckFn
	routingTable    routing.Table
	probeHandler    http.Handler
	upstreamHandler http.Handler
	tlsEnabled      bool
}

func NewRouting(ecf EndpointCheckFn, routingTable routing.Table, probeHandler http.Handler, upstreamHandler http.Handler, tlsEnabled bool) *Routing {
	return &Routing{
		endpointActive:  ecf,
		routingTable:    routingTable,
		probeHandler:    probeHandler,
		upstreamHandler: upstreamHandler,
		tlsEnabled:      tlsEnabled,
	}
}

var _ http.Handler = (*Routing)(nil)

func (rm *Routing) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = util.RequestWithLoggerWithName(r, "RoutingMiddleware")

	httpso := rm.routingTable.Route(r)
	if httpso == nil {
		if rm.isProbe(r) {
			rm.probeHandler.ServeHTTP(w, r)
			return
		}

		sh := handler.NewStatic(http.StatusNotFound, nil)
		sh.ServeHTTP(w, r)

		return
	}
	isHealthCheckPath := isHealthCheckPath(r, httpso)
	if isHealthCheckPath {
		healthCheckResponse := httpso.Annotations[healthCheckResponseAnnotation]
		if healthCheckResponse == healthCheckResponseStatic {
			// for static healthchecks, interceptor always responds
			rm.probeHandler.ServeHTTP(w, r)
			return
		} else if healthCheckResponse == "" || healthCheckResponse == healthCheckResponsePassthrough {
			// for passthrough healthchecks or if annotation is not set, interceptor only responds if there are no endpoints
			isActive, err := rm.endpointActive(r.Context(), httpso.GetNamespace(), httpso.Spec.ScaleTargetRef.Service)
			if err != nil {
				util.LoggerFromContext(r.Context()).Error(err, "error checking if endpoint for healthcheck is active")
			}
			if err == nil && !isActive {
				rm.probeHandler.ServeHTTP(w, r)
				return
			}
		} else {
			// unknown healthcheck response type
			util.LoggerFromContext(r.Context()).Error(fmt.Errorf("invalid healthcheck response type"), "invalid healthcheck response type %s", healthCheckResponse)
		}
	}
	r = r.WithContext(util.ContextWithHTTPSO(r.Context(), httpso))
	r = r.WithContext(util.ContextWithHealthCheck(r.Context(), isHealthCheckPath))

	stream, err := rm.streamFromHTTPSO(httpso)
	if err != nil {
		sh := handler.NewStatic(http.StatusInternalServerError, err)
		sh.ServeHTTP(w, r)

		return
	}
	r = r.WithContext(util.ContextWithStream(r.Context(), stream))

	rm.upstreamHandler.ServeHTTP(w, r)
}

func (rm *Routing) streamFromHTTPSO(httpso *httpv1alpha1.HTTPScaledObject) (*url.URL, error) {
	if rm.tlsEnabled {
		return url.Parse(fmt.Sprintf(
			"https://%s.%s:%d",
			httpso.Spec.ScaleTargetRef.Service,
			httpso.GetNamespace(),
			httpso.Spec.ScaleTargetRef.Port,
		))
	}
	//goland:noinspection HttpUrlsUsage
	return url.Parse(fmt.Sprintf(
		"http://%s.%s:%d",
		httpso.Spec.ScaleTargetRef.Service,
		httpso.GetNamespace(),
		httpso.Spec.ScaleTargetRef.Port,
	))
}

func (rm *Routing) isProbe(r *http.Request) bool {
	ua := r.UserAgent()

	return kubernetesProbeUserAgent.Match([]byte(ua)) || googleHCUserAgent.Match([]byte(ua))
}

func isHealthCheckPath(r *http.Request, httpso *httpv1alpha1.HTTPScaledObject) bool {
	return r.URL.Path == httpso.Annotations[healthCheckPathAnnotation]
}
