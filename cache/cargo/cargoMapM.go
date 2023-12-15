package cargo

import (
	"reflect"
	"sync"
)

type metaMM map[uint32]metaM

type CargoMapM struct {
	status CargoStatus
	metaMM metaMM
	lock   sync.RWMutex
}

// key集合(三主键)
type cargoMapMKey struct {
	Sid       uint32
	SecondKey uint32
	ThirdKey  uint32
}

func (c *CargoMapM) CollectChangedObjs(sid uint32, updateMetas *[]interface{}, deleteKeys *[]interface{}, syncDb bool) uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var objSize uint32 = 0
	for secondKey, metaM := range c.metaMM {
		for thirdKey, meta := range metaM {
			if meta.dbFlag&FLAG_DELETE != 0 {
				*deleteKeys = append(*deleteKeys, &cargoMapMKey{Sid: sid, SecondKey: secondKey, ThirdKey: thirdKey})
				objSize++
			} else if meta.dbFlag&FLAG_UPDATE != 0 {
				*updateMetas = append(*updateMetas, meta.obj)
				objSize++
			}
		}
	}
	if syncDb {
		c.status = STATUS_SYNC
	}
	return objSize
}

func (c *CargoMapM) AfterSyncDB(isSuccess bool) {
	if c.status == STATUS_SYNC {
		// 处于同步状态的数据，才处理数据库同步后的操作
		if !isSuccess {
			// 同步数据库失败，则变回变更状态，等待下次同步
			c.status = STATUS_CHANGE
		} else {
			// 同步成功，同步状态变成普通状态
			c.status = STATUS_NORMAL
			for _, metaM := range c.metaMM {
				for _, meta := range metaM {
					meta.dbFlag = FLAG_NONE
				}
			}
		}
	}
}

func (c *CargoMapM) CollectAllObjs(objs *[]interface{}) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, metaM := range c.metaMM {
		for _, meta := range metaM {
			if meta.obj != nil {
				*objs = append(*objs, meta.obj)
			}
		}
	}
}

func (c *CargoMapM) GetSingleObj(keys ...uint32) interface{} {
	return c.getObj(keys[0], keys[1])
}

func (c *CargoMapM) GetSomeObjs(keys ...uint32) []interface{} {
	list := make([]interface{}, 0)
	keySize := len(keys)
	for secondKey, metaM := range c.metaMM {
		if keySize < 1 || secondKey == keys[0] {
			for thirdKey, meta := range metaM {
				if (keySize < 2 || thirdKey == keys[1]) && meta.obj != nil {
					list = append(list, meta.obj)
				}
			}
		}
	}
	return list
}

func (c *CargoMapM) getObj(secondKey uint32, thirdKey uint32) interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()
	metaM, exit := c.metaMM[secondKey]
	if !exit {
		return nil
	}
	meta, exit := metaM[thirdKey]
	if !exit {
		return nil
	}
	return meta.obj
}

func (c *CargoMapM) Replace(obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	secondKey := uint32(reflect.ValueOf(obj).Elem().Field(1).Uint())
	thirdKey := uint32(reflect.ValueOf(obj).Elem().Field(2).Uint())
	metM, exit := c.metaMM[secondKey]
	c.status = STATUS_CHANGE
	if !exit {
		newMetaM := metaM{}
		c.metaMM[secondKey] = newMetaM
		newMetaM[thirdKey] = &meta{}
		newMetaM[thirdKey].Update(obj)
		return
	}
	met, exit := metM[thirdKey]
	if !exit {
		metM[thirdKey] = &meta{}
		metM[thirdKey].Update(obj)
		return
	}
	met.Update(obj)
}

func (c *CargoMapM) DeleteObj(obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	secondKey := uint32(reflect.ValueOf(obj).Elem().Field(1).Uint())
	thirdKey := uint32(reflect.ValueOf(obj).Elem().Field(2).Uint())
	metaM, exit := c.metaMM[secondKey]
	if !exit {
		return
	}
	meta, exit := metaM[thirdKey]
	if !exit {
		return
	}
	meta.DeleteObj()
	c.status = STATUS_CHANGE
}

func (c *CargoMapM) DeleteObjs() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, metaM := range c.metaMM {
		for _, meta := range metaM {
			meta.DeleteObj()
		}
	}
	c.status = STATUS_CHANGE
}

func (c *CargoMapM) GetNextUid() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var uid uint32 = 1
	for k := range c.metaMM {
		if k >= uid {
			uid = k + 1
		}
	}
	return uid
}

func (c *CargoMapM) CargoInit() {
	c.metaMM = make(metaMM)
}

func (c *CargoMapM) LoadDBData(element reflect.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()
	secondKey := uint32(element.Elem().Field(1).Uint())
	thirdKey := uint32(element.Elem().Field(2).Uint())
	c.metaMM = metaMM{}
	c.metaMM[secondKey] = metaM{}
	c.metaMM[secondKey][thirdKey] = &meta{obj: element.Interface()}
}
