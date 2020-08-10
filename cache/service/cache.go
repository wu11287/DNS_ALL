package service

import (
	"errors"
	"github.com/hashicorp/golang-lru"
)

var (
	Size = 5000
)

type Cache struct {
	Cache lru.Cache
}

func NewCache() Cache {
	return Cache{}
}

func (c *Cache) Add(v int, key, value interface{}) error {
	vCacheI, ok := c.Cache.Get(v)
	if !ok {
		cache, err := lru.New(Size)
		if err != nil {
			return err
		}
		c.Cache.Add(v, cache)
	}
	if vCache, ok := vCacheI.(lru.Cache); ok {
		vCache.Add(key, value)
		return nil
	} else {
		return errors.New("[Cache.Add] Get V-Cache failed")
	}
}

func (c *Cache) Get(v int, key interface{}) (interface{}, error) {
	vCacheI, ok := c.Cache.Get(v)
	if !ok {
		return nil, errors.New("[Cache.Get] V-Cache is not exist")
	}
	if vCache, ok := vCacheI.(lru.Cache); ok {
		v, vOk := vCache.Get(key)
		if !vOk {
			return nil, errors.New("[Cache.Get] value is not exist")
		} else {
			return v, nil
		}
	} else {
		return nil, errors.New("[Cache.Get] Get V-Cache failed")
	}
}
