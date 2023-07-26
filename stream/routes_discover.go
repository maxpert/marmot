package stream

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

func parseRemoteLeafOpts() []*server.RemoteLeafOpts {
	leafServers := server.RoutesFromStr(*cfg.LeafServerFlag)
	if len(leafServers) != 0 {
		leafServers = flattenRoutes(leafServers, true)
	}

	return lo.Map[*url.URL, *server.RemoteLeafOpts](
		leafServers,
		func(u *url.URL, _ int) *server.RemoteLeafOpts {
			hub := u.Query().Get("hub") == "true"
			r := &server.RemoteLeafOpts{
				URLs: []*url.URL{u},
				Hub:  hub,
			}

			return r
		})
}

func flattenRoutes(urls []*url.URL, waitDNSEntries bool) []*url.URL {
	ret := make([]*url.URL, 0)
	for _, u := range urls {
		if u.Scheme == "dns" {
			ret = append(ret, queryDNSRoutes(u, waitDNSEntries)...)
			continue
		}

		ret = append(ret, u)
	}

	return ret
}

func queryDNSRoutes(u *url.URL, waitDNSEntries bool) []*url.URL {
	minPeerStr := u.Query().Get("min")
	intervalStr := u.Query().Get("interval_ms")

	minPeers, err := strconv.Atoi(minPeerStr)
	if err != nil {
		minPeers = 2
	}

	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		interval = 1000
	}

	log.Info().
		Str("url", u.String()).
		Int("min_peers", minPeers).
		Int("interval", interval).
		Bool("wait_dns_entries", waitDNSEntries).
		Msg("Starting DNS A/AAAA peer discovery")

	if waitDNSEntries {
		minPeers = 0
	}

	for {
		peers, err := getDirectNATSAddresses(u)
		if err != nil {
			log.Error().
				Err(err).
				Str("url", u.String()).
				Msg("Unable to discover peer URLs")
			time.Sleep(time.Duration(interval) * time.Millisecond)
			continue
		}

		urls := strings.Join(
			lo.Map[*url.URL, string](peers, func(i *url.URL, _ int) string { return i.String() }),
			", ",
		)
		log.Info().Str("urls", urls).Msg("Peers discovered")

		if len(peers) >= minPeers {
			return peers
		} else {
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}
	}
}

func getDirectNATSAddresses(u *url.URL) ([]*url.URL, error) {
	v4, v6, err := queryDNS(u.Hostname())
	if err != nil {
		return nil, err
	}
	var ret []*url.URL
	for _, ip := range v4 {
		peerUrl := fmt.Sprintf("nats://%s:%s/", ip, u.Port())
		peer, err := url.Parse(peerUrl)
		if err != nil {
			log.Warn().
				Str("peer_url", peerUrl).
				Msg("Unable to parse URL, might be due to bad DNS entry")
			continue
		}
		ret = append(ret, peer)
	}

	for _, ip := range v6 {
		peerUrl := fmt.Sprintf("nats://[%s]:%s/", ip, u.Port())
		peer, err := url.Parse(peerUrl)
		if err != nil {
			log.Warn().
				Str("peer_url", peerUrl).
				Msg("Unable to parse URL, might be due to bad DNS entry")
			continue
		}
		ret = append(ret, peer)
	}

	return ret, nil
}

func queryDNS(domain string) ([]string, []string, error) {
	ips, err := net.LookupIP(domain)
	if err != nil {
		return nil, nil, err
	}

	var ipv4 []string
	var ipv6 []string
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			ipv4 = append(ipv4, v4.String()) // A record
		} else if v6 := ip.To16(); v6 != nil {
			ipv6 = append(ipv6, v6.String()) // AAAA record
		}
	}

	return ipv4, ipv6, nil
}
