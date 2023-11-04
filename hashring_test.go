package shardedmemcache

import (
	"fmt"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var (
	hostList = []string{
		"192.168.1.1",
		"192.168.1.2",
		"192.168.1.3",
		"192.168.1.4",
	}

	lookupFunc = func(host string) ([]string, error) {
		return hostList, nil
	}

	connOpenerFunc = func(host string) memcacheImpl {
		return &memcacheFakeService{
			store: make(map[string]*memcache.Item),
		}
	}
)

type memcacheFakeService struct {
	store map[string]*memcache.Item
}

func (m *memcacheFakeService) Delete(key string) error {
	delete(m.store, key)
	return nil
}

func (m *memcacheFakeService) Set(item *memcache.Item) error {
	if item == nil {
		return errors.New("item is nil")
	}
	m.store[item.Key] = item

	return nil
}

func (m *memcacheFakeService) Get(key string) (*memcache.Item, error) {
	data, ok := m.store[key]
	if !ok {
		return nil, errors.New("empty data in fake node")
	}

	return data, nil
}

func (m *memcacheFakeService) Close() error {
	return nil
}

func TestAddNode(t *testing.T) {
	fmt.Println("start")
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	hr.updateNodes(hostList)

	initialNodeCount := len(hr.nodeList)
	hr.AddNode("Node1")
	assert.Equal(t, initialNodeCount+1, len(hr.nodeList))
}

func TestRemoveNode(t *testing.T) {
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	hr.AddNode("Node1")
	initialNodeCount := len(hr.nodeList)
	hr.RemoveNode("Node1")
	assert.Equal(t, initialNodeCount-1, len(hr.nodeList))
}

func TestGetNodeByKey(t *testing.T) {
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	hr.AddNode("Node1")

	key := "myKey"
	node := hr.GetNodeByKey(key)
	assert.Equal(t, "Node1", getParentNodeName(node))
}

func TestSetByKey(t *testing.T) {
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	hr.updateNodes([]string{"Node1"})

	key := "myKey1"
	value := []byte("Hello, Memcached!")
	expiration := int32(3600)
	err = hr.SetByKey(key, value, expiration)
	assert.Nil(t, err)
}

func TestGetByKey(t *testing.T) {
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	hr.updateNodes([]string{"Node1"})

	key := "myKey1"
	value := []byte("Hello, Memcached!")
	expiration := int32(3600)
	err = hr.SetByKey(key, value, expiration)
	assert.Nil(t, err)

	retrievedValue, err := hr.GetByKey(key)
	assert.Nil(t, err)
	assert.Equal(t, value, retrievedValue)
}

func TestDeleteByKey(t *testing.T) {
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	hr.updateNodes([]string{"Node1"})

	key := "myKey1"
	value := []byte("Hello, Memcached!")
	expiration := int32(3600)
	err = hr.SetByKey(key, value, expiration)
	assert.Nil(t, err)

	err = hr.DeleteByKey(key)
	assert.Nil(t, err)
}

func TestUpdateNodes(t *testing.T) {
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	nodes := []string{"Node1", "Node2"}
	hr.updateNodes(nodes)
	assert.Equal(t, len(nodes), len(hr.nodeList))
}

func TestGetBucket(t *testing.T) {
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	hr.updateNodes([]string{"Node1"})

	key := "myKey1"
	bucket := hr.GetBucket(key)
	assert.NotEmpty(t, bucket)
}

func TestUpdateNodeList(t *testing.T) {
	hr, err := NewWithParams(100, "example.com", lookupFunc, connOpenerFunc)
	assert.Nil(t, err)

	hr.updateNodes([]string{"Node1"})
	go hr.updateNodeList(time.Second)
	time.Sleep(time.Second * 2)

	assert.Equal(t, len(hostList), len(hr.nodeList))
}

func TestDataDistribution(t *testing.T) {
	hr, err := NewWithParams(1, "example.com", lookupFunc, connOpenerFunc)
	if err != nil {
		t.Errorf("Error creating hashring: %v", err)
	}

	hr.updateNodes([]string{"Node1", "Node2", "Node3"})

	// Create a list of data
	data := []struct {
		key   string
		value []byte
	}{
		{"Key1", []byte("Value1")},
		{"Key2", []byte("Value2")},
		{"Key3", []byte("Value3")},
		{"Key4", []byte("Value4")},
		{"Key5", []byte("Value5")},
		{"Key6", []byte("Value6")},
	}

	// Store data and count the distribution across nodes
	distribution := make(map[string]int)
	for _, item := range data {
		err := hr.SetByKey(item.key, item.value, 60)
		if err != nil {
			t.Errorf("Error setting key: %v", err)
		}
		node := hr.GetNodeByKey(item.key)
		distribution[node]++
	}

	// Check the distribution is roughly equal
	threshold := len(data) / 3 // Approximately equal distribution
	for node, count := range distribution {
		if count < threshold {
			t.Errorf("Data distribution for %s is less than expected: %d", node, count)
		}
	}
}
