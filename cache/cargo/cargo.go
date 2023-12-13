package cargo

import (
	"reflect"
)

type Cargo struct {
	status CargoStatus
	meta   *meta
}

// key集合(单主键)
type cargoKey struct {
	Sid uint32
}

func (c *Cargo) CollectChangedObjs(sid uint32, updateMetas *[]interface{}, deleteKeys *[]interface{}, syncDb bool) (objNum uint32) {
	if c.meta.dbFlag&FLAG_DELETE != 0 {
		*deleteKeys = append(*deleteKeys, &cargoKey{Sid: sid})
		objNum = 1
	} else if c.meta.dbFlag&FLAG_UPDATE != 0 {
		*updateMetas = append(*updateMetas, c.meta.obj)
		objNum = 1
	}
	if syncDb {
		c.status = STATUS_SYNC
	}
	return
}

func (c *Cargo) AfterSyncDB(isSuccess bool) {
	if c.status == STATUS_SYNC {
		// 处于同步状态的数据，才处理数据库同步后的操作
		if !isSuccess {
			// 同步数据库失败，则变回变更状态，等待下次同步
			c.status = STATUS_CHANGE
		} else {
			// 同步成功，同步状态变成普通状态
			c.status = STATUS_NORMAL
			c.meta.dbFlag = FLAG_NONE
		}
	}
}

func (c *Cargo) CollectAllObjs(objs *[]interface{}) {
	if c.meta.obj != nil {
		*objs = append(*objs, c.meta.obj)
	}
}

func (c *Cargo) GetSingleObj(_ ...uint32) interface{} {
	return c.meta.obj
}

func (c *Cargo) GetSomeObjs(_ ...uint32) []interface{} {
	list := make([]interface{}, 0)
	if c.meta.obj != nil {
		list = append(list, c.meta.obj)
	}
	return list
}

func (c *Cargo) DeleteObj(obj interface{}) {
	c.meta.DeleteObj()
	c.status = STATUS_CHANGE
}

func (c *Cargo) DeleteObjs() {
	c.meta.DeleteObj()
	c.status = STATUS_CHANGE
}

func (c *Cargo) Replace(obj interface{}) {
	c.meta.Update(obj)
	c.status = STATUS_CHANGE
}

func (c *Cargo) GetNextUid() uint32 {
	return 0
}

func (c *Cargo) CargoInit() {
	c.meta = &meta{}
}

func (c *Cargo) LoadDBData(element reflect.Value) {
	c.meta = &meta{obj: element.Interface()}
}

func (c *Cargo) CleanChange() {
	c.status = STATUS_NORMAL
}
