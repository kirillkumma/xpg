package pg

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/Deimvis-go/logs/logs"
	"github.com/Deimvis-go/xpg/pg/internal/lb"
	"github.com/Deimvis-go/xpg/pg/internal/trace"
	"github.com/Deimvis/go-ext/go1.25/xoptional"
	"github.com/Deimvis/models/utility/go/dmutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var defaultConnectTimeoutMs int64 = 2000

type PgxpoolConfigParams struct {
	fx.In

	ConnConfig   *ConnConfig
	PoolConfig   *PoolConfig
	AfterConnect AfterConnectFn `optional:"true"`

	Logger *zap.SugaredLogger

	QueryStartCounter            *prometheus.CounterVec   `name:"pg_query_start" optional:"true"`
	QueryFinishCounter           *prometheus.CounterVec   `name:"pg_query_finish" optional:"true"`
	QueryDurationHistogram       *prometheus.HistogramVec `name:"pg_query_duration" optional:"true"`
	QueryTimeoutHistogram        *prometheus.HistogramVec `name:"pg_query_timeout" optional:"true"`
	ConnAcquireStartCounter      *prometheus.CounterVec   `name:"pg_conn_acquire_start" optional:"true"`
	ConnAcquireFinishCounter     *prometheus.CounterVec   `name:"pg_conn_acquire_finish" optional:"true"`
	ConnAcquireDurationHistogram *prometheus.HistogramVec `name:"pg_conn_acquire_duration" optional:"true"`
	ConnAcquireTimeoutHistogram  *prometheus.HistogramVec `name:"pg_conn_acquire_timeout" optional:"true"`
	ConnectStartCounter          *prometheus.CounterVec   `name:"pg_connect_start" optional:"true"`
	ConnectFinishCounter         *prometheus.CounterVec   `name:"pg_connect_finish" optional:"true"`
	ConnectDurationHistogram     *prometheus.HistogramVec `name:"pg_connect_duration" optional:"true"`
	ConnectTimeoutHistogram      *prometheus.HistogramVec `name:"pg_connect_timeout" optional:"true"`
	ReleaseCounter               *prometheus.CounterVec   `name:"pg_conn_release" optional:"true"`
	// NOTE: when adding here new field do not forget to add it also into pgfx/pool_config.go,
	//       so that users of pgfx can use it as well.
}

func NewPgxpoolConfig(p PgxpoolConfigParams) *pgxpool.Config {
	// TODO: remove after Tracers become required
	if p.PoolConfig.Tracers == nil {
		p.PoolConfig.Tracers = &PoolTracersConfig{
			Logging: &LoggingTracerConfig{
				Option: dmutil.NewEnabledOption(),
			},
			Prometheus: &PrometheusTracerConfig{
				Option: dmutil.NewEnabledOption(),
			},
		}
	}

	config, err := pgxpool.ParseConfig(BuildConnUrl(p.ConnConfig))
	if err != nil {
		panic(err)
	}
	if p.ConnConfig.IPv4Only != nil && *p.ConnConfig.IPv4Only {
		config.ConnConfig.LookupFunc = lookupFuncIPv4Only
	}
	if p.ConnConfig.IPv6Only != nil && *p.ConnConfig.IPv6Only {
		config.ConnConfig.LookupFunc = lookupFuncIPv6Only
	}
	// TODO: deprecated, will be removed along with HostsLoadBalancing field
	if p.ConnConfig.ServersLoadBalancing == nil && p.ConnConfig.HostsLoadBalancing != nil {
		p.ConnConfig.ServersLoadBalancing = p.ConnConfig.HostsLoadBalancing
	}
	if p.ConnConfig.ServersLoadBalancing != nil {
		// NOTE: waiting for official support: https://github.com/jackc/pgx/issues/2059
		//       until then...
		var hosts []string
		hosts = append(hosts, config.ConnConfig.Host)
		for _, fb := range config.ConnConfig.Fallbacks {
			hosts = append(hosts, fb.Host)
		}
		// it's okay that "servers" load balancing option is implemented with "hosts" load balancer,
		// because pgx load balancer is actually implemented as hosts resolver
		hlb := lb.NewHostsLoadBalancer(hosts, p.ConnConfig.ServersLoadBalancing.Algorithm)
		host2ind := func() func(string) int {
			index := make(map[string]int)
			for i, host := range hosts {
				index[host] = i
			}
			return func(s string) int {
				return index[s]
			}
		}()
		lookupFunc := config.ConnConfig.LookupFunc
		if lookupFunc == nil {
			lookupFunc = net.DefaultResolver.LookupHost
		}
		config.ConnConfig.LookupFunc = wrapLookupFuncWithLB(hlb, host2ind, lookupFunc)
	}
	if p.PoolConfig.MinConns != nil {
		config.MinConns = *p.PoolConfig.MinConns
	}
	if p.PoolConfig.MaxConns != nil {
		config.MaxConns = *p.PoolConfig.MaxConns
	}
	if p.PoolConfig.MaxConnLifetimeS.HasValue() {
		config.MaxConnLifetime = time.Duration(p.PoolConfig.MaxConnLifetimeS.Value()) * time.Second
	}
	if p.PoolConfig.MaxConnLifetimeJitterS != nil {
		config.MaxConnLifetimeJitter = time.Duration(*p.PoolConfig.MaxConnLifetimeJitterS) * time.Second
	}
	connectTimeoutMs := xoptional.ValueOr(p.PoolConfig.ConnectTimeoutMs, defaultConnectTimeoutMs)
	config.ConnConfig.ConnectTimeout = time.Duration(connectTimeoutMs) * time.Millisecond

	tracersCfg := p.PoolConfig.Tracers
	tracers := []trace.Tracer{}
	if tracersCfg.Logging != nil && tracersCfg.Logging.IsEnabled() {
		// TODO: wait when logs.KVCtxLogger will support Clone(opts...) and CallerSkip option,
		// caller skip 2 + ... — because logging tracer + combined tracer
		const pgxPoolCallerSkip = 1
		tmpLg := p.Logger.WithOptions(zap.AddCallerSkip(2 + pgxPoolCallerSkip))
		tracers = append(tracers, trace.NewLoggingTracer(
			logs.ZapAsKVCtxLogger(tmpLg),
		))
	}
	if tracersCfg.Prometheus != nil && tracersCfg.Prometheus.IsEnabled() {
		tracers = append(tracers, trace.NewPrometheusTracer(
			p.QueryStartCounter,
			p.QueryFinishCounter,
			p.QueryDurationHistogram,
			p.QueryTimeoutHistogram,
			p.ConnAcquireStartCounter,
			p.ConnAcquireFinishCounter,
			p.ConnAcquireDurationHistogram,
			p.ConnAcquireTimeoutHistogram,
			p.ConnectStartCounter,
			p.ConnectFinishCounter,
			p.ConnectDurationHistogram,
			p.ConnectTimeoutHistogram,
			p.ReleaseCounter,
		))
	}
	if tracersCfg.DebugVars != nil && tracersCfg.DebugVars.IsEnabled() {
		tracers = append(tracers, trace.NewDebugVarsTracer(
			fmt.Sprintf(tracersCfg.DebugVars.VarsRootNameFormat, newAnonPoolName()),
		))
	}
	config.ConnConfig.Tracer = trace.NewCombinedTracer(tracers...)

	// set read-write connection by default
	config.ConnConfig.ValidateConnect = pgconn.ValidateConnectTargetSessionAttrsReadWrite

	// QueryExecMode choice reasoning.
	// Can't use QueryExecModeCacheStatement due to "prepared statement already exists" error
	// Example: ERROR: prepared statement "stmtcache_71fecb5374841522ac44f3204114da1826cfb563b56defb9" already exists (SQLSTATE 42P05)
	// Reason: pgbouncer transaction pooling prohibits prepared statements because prepared statement's state corresponds to a session.
	// Can't use QueryExecModeCacheDescribe for the same reason.
	// Can't use pgx.QueryExecModeDescribeExec due to "unnamed prepared statement doesn't exist" error
	// Example: ERROR: unnamed prepared statement does not exist (SQLSTATE 26000)
	// Reason: between prepare request and actual execution request pgbouncer transaction pooling may give different connections.
	// Will use pgx.QueryExecModeExec, but note that it requires explicit type mapping from Golang to PostgreSQL types.
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if p.AfterConnect != nil {
		config.AfterConnect = p.AfterConnect
	}

	return config
}

func lookupFuncIPv4Only(ctx context.Context, host string) (addrs []string, err error) {
	if host == "" {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return []string{host}, nil
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	var ipv4s []string
	for _, ip := range ips {
		if ipv4 := ip.IP.To4(); ipv4 != nil {
			ipv4s = append(ipv4s, ipv4.String())
		}
	}
	return ipv4s, nil
}

func lookupFuncIPv6Only(ctx context.Context, host string) (addrs []string, err error) {
	if host == "" {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return []string{host}, nil
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	var ipv6s []string
	for _, ip := range ips {
		if ipv4 := ip.IP.To4(); ipv4 == nil {
			ipv6s = append(ipv6s, ip.String())
		}
	}
	return ipv6s, nil
}

// wrapLookupFuncWithLB is a workaround for hosts client-side load balancing
// since pgx doesn't support it natively:
// https://github.com/jackc/pgx/issues/2059.
// https://github.com/jackc/pgx/issues/819.
//
// May use it with read-only connections only with
// ValidateConnectTargetSessionAttrsPreferStandby being set.
// NEVER use it with read-write connections.
//
// Limitations:
// It does not run perfectly.
// Since load balancing is global,
// it is possible that connection creation
// will check the same server on each attempt.
// It leads to case when read-only connection
// creation may try only leader server.
// Therefore, ValidateConnectTargetSessionAttrsPreferStandby MUST be used,
// so this case will end up connecting to leader server.
// The similar may happen for read-write connection,
// therefore this load balancing MUST NOT be used for read-write connections.
//
// Actually, if one provides connect seed in context
// (see CtxWithConnectSeed), then this load balancer
// would synchronize results within single connect operation
// (e.g. round robin load balancer will be guaranteed
// to return 1 of each host, if len(hosts) attempts will be made
// within this connect operation; but it ends up
// being random selection for read-only server)
func wrapLookupFuncWithLB(hlb lb.HostsLoadBalancer, host2ind func(string) int, fn pgconn.LookupFunc) pgconn.LookupFunc {
	type seedableLb interface {
		lb.DeterministicHostsLoadBalancer
		lb.ForwardPrecomputedHostsLoadBalancer
	}
	shlb, shlbOk := hlb.(seedableLb)
	return func(ctx context.Context, host__ignored string) (addrs []string, err error) {
		// ignore given host argument and replace it with host from load balancer

		if shlbOk {
			seed, ok := CtxConnectSeed(ctx)
			if ok {
				hostInd := host2ind(host__ignored)
				hlb_ := shlb.WithSeed(seed).(seedableLb)
				if hostInd > 0 {
					hlb_.AdvanceForward(uint64(hostInd) - 1)
				}
				host := hlb_.Next()
				return fn(ctx, host)
			}
		}

		host := hlb.Next()
		return fn(ctx, host)
	}
}

func newAnonPoolName() string {
	ind := anonPoolCnt.Add(1) - 1
	return fmt.Sprintf("anon%02d", ind)
}

var (
	anonPoolCnt atomic.Int64
)
