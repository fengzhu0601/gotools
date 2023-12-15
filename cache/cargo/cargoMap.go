package cargo

import (
	"reflect"
	"sync"
)

type metaM map[uint32]*meta

type CargoMap struct {
	status CargoStatus
	metaM  metaM
	lock   sync.RWMutex
}

// key集合(双主键)
type cargoMapKey struct {
	Sid       uint32
	SecondKey uint32
}

func (c *CargoMap) CollectChangedObjs(sid uint32, updateMetas *[]interface{}, deleteKeys *[]interface{}, syncDb bool) uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var objSize uint32 = 0
	for secondKey, meta := range c.metaM {
		if meta.dbFlag&FLAG_DELETE != 0 {
			*deleteKeys = append(*deleteKeys, &cargoMapKey{Sid: sid, SecondKey: secondKey})
			objSize++
		} else if meta.dbFlag&FLAG_UPDATE != 0 {
			*updateMetas = append(*updateMetas, meta.obj)
			objSize++
		}
	}
	if syncDb {
		c.status = STATUS_SYNC
	}

	return objSize
}

func (c *CargoMap) AfterSyncDB(isSuccess bool) {
	if c.status == STATUS_SYNC {
		// 处于同步状态的数据，才处理数据库同步后的操作
		if !isSuccess {
			// 同步数据库失败，则变回变更状态，等待下次同步
			c.status = STATUS_CHANGE
		} else {
			// 同步成功，同步状态变成普通状态
			c.status = STATUS_NORMAL
			for _, meta := range c.metaM {
				meta.dbFlag = FLAG_NONE
			}
		}
	}
}

func (c *CargoMap) CollectAllObjs(objs *[]interface{}) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, meta := range c.metaM {
		if meta.obj != nil {
			*objs = append(*objs, meta.obj)
		}
	}
}

func (c *CargoMap) GetSingleObj(keys ...uint32) interface{} {
	return c.getObj(keys[0])
}

func (c *CargoMap) GetSomeObjs(keys ...uint32) []interface{} {
	list := make([]interface{}, 0)
	keySize := len(keys)
	for secondKey, meta := range c.metaM {
		if (keySize == 0 || secondKey == keys[0]) && meta.obj != nil {
			list = append(list, meta.obj)
		}
	}
	return list
}

func (c *CargoMap) getObj(secondKey uint32) interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()
	meta, exit := c.metaM[secondKey]
	if !exit {
		return nil
	}
	return meta.obj
}

func (c *CargoMap) Replace(obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	secondKey := uint32(reflect.ValueOf(obj).Elem().Field(1).Uint())
	r, exit := c.metaM[secondKey]
	if !exit {
		newMeta := &meta{}
		c.metaM[secondKey] = newMeta
		r = newMeta
	}
	r.Update(obj)
	c.status = STATUS_CHANGE
}

func (c *CargoMap) DeleteObj(obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	secondKey := uint32(reflect.ValueOf(obj).Elem().Field(1).Uint())
	r, exit := c.metaM[secondKey]
	if !exit {
		return
	}
	r.DeleteObj()
	c.status = STATUS_CHANGE
}

func (c *CargoMap) DeleteObjs() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, r := range c.metaM {
		r.DeleteObj()
	}
	c.status = STATUS_CHANGE
}

func (c *CargoMap) GetNextUid() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var uid uint32 = 1
	for k := range c.metaM {
		if k >= uid {
			uid = k + 1
		}
	}
	return uid
}

func (c *CargoMap) CargoInit() {
	c.metaM = make(metaM)
}

func (c *CargoMap) LoadDBData(element reflect.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()
	secondKey := uint32(element.Elem().Field(1).Uint())
	c.metaM[secondKey] = &meta{obj: element.Interface()}
}
