package cache

import "reflect"

// 容器载体接口
type CargoInt interface {
	// 载体初始化
	CargoInit()
	// 载体载入数据库数据
	LoadDBData(reflect.Value)
	// 收集变更的obj数据
	CollectChangedObjs(uint32, *[]interface{}, *[]interface{}, bool) uint32
	// 同步数据库后调用
	AfterSyncDB(isSuccess bool)
	// 获取所有obj
	CollectAllObjs(*[]interface{})
	// 更新或插入某个obj
	Replace(interface{})
	// 获取单个obj
	GetSingleObj(keys ...uint32) interface{}
	// 获取单个obj
	GetSomeObjs(keys ...uint32) []interface{}
	// 删除某个obj
	DeleteObj(interface{})
	// 删除说有obj
	DeleteObjs()
	// 获取下个Uid
	GetNextUid() uint32
}
