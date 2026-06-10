package pg

import (
	"errors"

	"github.com/Deimvis-go/xpg/pg/internal/lb"
)

// TODO: move to pgconncfg package

type ConnConfig struct {
	Servers  []ServerLocation `yaml:"servers"`
	User     string           `yaml:"user"`
	Password string           `yaml:"password"`
	Database string           `yaml:"database" validate:"required"`
	// TODO: support other parameters: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
	IPv4Only             *bool                 `yaml:"ipv4_only"`
	IPv6Only             *bool                 `yaml:"ipv6_only"`
	ServersLoadBalancing *ServersLoadBalancing `yaml:"servers_load_balancing" validate:"omitnil"`

	// TODO: DEPRECATED, use Servers field instead
	Host string `yaml:"host""`
	Port int    `yaml:"port"`
	// TODO: DEPRECATED, user ServersLoadBalancing field instead
	HostsLoadBalancing *ServersLoadBalancing `yaml:"hosts_load_balancing" validate:"omitnil"`
}

type ServerLocation struct {
	Host string `yaml:"host" validate:"required"`
	Port int    `yaml:"port" validate:"gt=0,lte=65535"`
}

type ServersLoadBalancing struct {
	Algorithm lb.Algo `yaml:"algorithm" validate:"required"`
}

func (cfg ConnConfig) ValidateSelf() error {
	if cfg.IPv4Only != nil && *cfg.IPv4Only &&
		cfg.IPv6Only != nil && *cfg.IPv6Only {
		return errors.New("ipv4_only and ipv6_only in conn config cannot both be true")
	}
	return nil
}
