package shardedmemcache

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
)

const (
	defaultMemcachedPort = "11211"
	defaultFlushPeriod   = time.Second * 5
)

type memcacheImpl interface {
	Delete(key string) error
	Set(item *memcache.Item) error
	Get(key string) (*memcache.Item, error)
	Close() error
}

// HashRing is a struct representing the hash ring.
type HashRing struct {
	buckets        []uint64
	nodeToBucket   map[string]uint64
	bucketToNode   map[uint64]string
	virtualBuckets int
	nodeList       map[string]struct{}
	mc             map[string]memcacheImpl
	mu             sync.RWMutex
	hostToLookup   string
	lookupFunc     func(host string) ([]string, error)
	connOpenerFunc func(host string) memcacheImpl
}

// New creates new hash ring with default implementation.
func New(vBuckets int, host string) (*HashRing, error) {
	hr := &HashRing{
		buckets:        make([]uint64, 0),
		nodeToBucket:   make(map[string]uint64),
		bucketToNode:   make(map[uint64]string),
		virtualBuckets: vBuckets,
		mc:             make(map[string]memcacheImpl),
		nodeList:       make(map[string]struct{}),
		hostToLookup:   host,
		lookupFunc:     lookup,
		connOpenerFunc: openConn,
	}

	nodes, err := hr.lookupFunc(hr.hostToLookup)
	if err != nil {
		return nil, err
	}

	hr.updateNodes(nodes)
	go hr.updateNodeList(defaultFlushPeriod) // TODO: make a variable from env

	return hr, nil
}

// NewWithParams creates a new hash ring with virtual buckets.
func NewWithParams(
	virtualBuckets int,
	hostToLookup string,
	lookupFunc func(host string) ([]string, error),
	connOpenerFunc func(host string) memcacheImpl,
) (*HashRing, error) {
	hr := &HashRing{
		buckets:        make([]uint64, 0),
		nodeToBucket:   make(map[string]uint64),
		bucketToNode:   make(map[uint64]string),
		virtualBuckets: virtualBuckets,
		mc:             make(map[string]memcacheImpl),
		nodeList:       make(map[string]struct{}),
		hostToLookup:   hostToLookup,
		lookupFunc:     lookupFunc,
		connOpenerFunc: connOpenerFunc,
	}

	if lookupFunc == nil {
		hr.lookupFunc = lookup
	}

	if connOpenerFunc == nil {
		hr.connOpenerFunc = openConn
	}

	return hr, nil
}

// AddNode adds a node to the hash ring with the specified number of virtual buckets.
func (hr *HashRing) AddNode(node string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.virtualBuckets; i++ {
		virtualNode := fmt.Sprintf("%s-%d", node, i)
		bucket := xxhash.Sum64String(virtualNode)
		if len(hr.buckets) > 0 {
			bucket = bucket % uint64(len(hr.buckets))
		}
		hr.nodeToBucket[virtualNode] = bucket
		hr.bucketToNode[bucket] = virtualNode
		hr.buckets = append(hr.buckets, bucket)
	}

	sort.Slice(hr.buckets, func(i, j int) bool {
		return hr.buckets[i] < hr.buckets[j]
	})

	hr.nodeList[node] = struct{}{}
}

// RemoveNode removes a node from the hash ring along with its virtual buckets.
func (hr *HashRing) RemoveNode(node string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.virtualBuckets; i++ {
		virtualNode := fmt.Sprintf("%s-%d", node, i)
		if bucket, exists := hr.nodeToBucket[virtualNode]; exists {
			delete(hr.nodeToBucket, virtualNode)
			delete(hr.bucketToNode, bucket)
			for j, b := range hr.buckets {
				if b == bucket {
					hr.buckets = append(hr.buckets[:j], hr.buckets[j+1:]...)
					break
				}
			}
		}
	}

	delete(hr.nodeList, node)
}

// GetBucket finds the bucket for a given key using binary search.
func (hr *HashRing) GetBucket(key string) uint64 {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	hash := xxhash.Sum64String(key)
	index := sort.Search(len(hr.buckets), func(i int) bool {
		return hr.buckets[i] >= hash
	})
	if index == len(hr.buckets) {
		return hr.buckets[0]
	}
	return hr.buckets[index]
}

// GetNodeByKey returns the node for reading and writing memcached data based on the key.
func (hr *HashRing) GetNodeByKey(key string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	bucket := hr.GetBucket(key)
	return hr.bucketToNode[bucket]
}

// lookup performs DNS lookup on a given FQDN, returning IP list as strings
func lookup(host string) ([]string, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, errors.Wrap(err, "lookup error")
	}

	nodes := make([]string, 0, len(ips))
	for _, ip := range ips {
		nodes = append(nodes, ip.String())
	}

	return nodes, nil
}

func (hr *HashRing) getConnByVirtualNode(node string) memcacheImpl {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	parentNode := getParentNodeName(node)
	return hr.mc[parentNode]
}

// UpdateNodeList performs DNS lookup to update the list of nodes from the specified host and rebalance.
func (hr *HashRing) updateNodeList(delay time.Duration) {
	t := time.NewTicker(delay)
	for {
		select {
		case <-t.C:
			nodes, err := hr.lookupFunc(hr.hostToLookup)
			if err != nil {
				// some logging perhaps
				slog.Error("UpdateNodeList lookup failed", "err", err)
			}
			hr.updateNodes(nodes)
		}
	}
}

func openConn(node string) memcacheImpl {
	port := os.Getenv("SHARDED_MEMCACHED_PORT")
	if len(port) < 1 {
		port = defaultMemcachedPort
	}

	return memcache.New(fmt.Sprintf("%s:%s", node, port)) // TODO: make this in config
}

func getParentNodeName(node string) string {
	return strings.Split(node, "-")[0]
}

// UpdateNodes updates the list of nodes in the hash ring and rebalances virtual buckets.
func (hr *HashRing) updateNodes(nodes []string) {
	nodesMap := make(map[string]struct{}, len(nodes))
	nodeToAdd := make([]string, 0)
	nodeToRemove := make([]string, 0)

	for _, node := range nodes {
		nodesMap[node] = struct{}{}
	}

	for node := range hr.nodeList {
		_, ok := nodesMap[node]
		if !ok {
			nodeToRemove = append(nodeToRemove, node)
		}
	}

	for node := range nodesMap {
		_, ok := hr.nodeList[node]
		if !ok {
			nodeToAdd = append(nodeToAdd, node)
		}
	}

	for _, node := range nodeToRemove {
		err := hr.mc[node].Close()
		delete(hr.mc, node)
		if err != nil {
			slog.Error("could not close conn to memcached", "err", err)
		}
		hr.RemoveNode(node)
	}

	for _, node := range nodeToAdd {
		hr.nodeList[node] = struct{}{}
		// we're opening separate connection for each node, since we don't implement ServerPicker
		hr.mc[node] = hr.connOpenerFunc(node)
		hr.AddNode(node)
	}
}

// SetByKey sets a value in the memcached instance associated with the key and alive time (in seconds or UNIX TS).
func (hr *HashRing) SetByKey(key string, value []byte, expiration int32) error {
	node := hr.GetNodeByKey(key)
	conn := hr.getConnByVirtualNode(node)
	return conn.Set(&memcache.Item{Key: key, Value: value, Expiration: expiration})
}

// GetByKey retrieves a value from the memcached instance associated with the key.
func (hr *HashRing) GetByKey(key string) ([]byte, error) {
	node := hr.GetNodeByKey(key)
	conn := hr.getConnByVirtualNode(node)
	item, err := conn.Get(key)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

// DeleteByKey deletes a value from the memcached instance associated with the key.
func (hr *HashRing) DeleteByKey(key string) error {
	node := hr.GetNodeByKey(key)
	conn := hr.getConnByVirtualNode(node)
	return conn.Delete(key)
}
