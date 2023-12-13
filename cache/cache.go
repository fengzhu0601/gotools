package cache

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/fengzhu0601/goproject/go_tool/logger"

	gormlog "gorm.io/gorm/logger"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type containerMap map[reflect.Type]*Container

type DBConfig struct {
	DBHost      string
	DBPort      int
	DBUser      string
	DBPass      string
	DBName      string
	DBEncode    string
	DBPool_size int
	DBTimeout   int
	UpdateGap   int   // 每次批量更新间隔(秒)
	UpdateSize  int   // 每次批量更新的数据量
	GCSeconds   int64 // 玩家下线n秒后进行内存回收
	RWAnalyse   bool  // 是否启动读写分析(开启有性能损耗)
}

type Cache struct {
	containers    containerMap // 容器集合
	containerList []*Container
	dbConfig      *DBConfig
	dbCon         *gorm.DB
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewCache(dbConfig *DBConfig) (*Cache, error) {
	cache := &Cache{
		containers:    make(containerMap),
		containerList: make([]*Container, 0),
		dbConfig:      dbConfig}
	err := cache.initDB()
	if err != nil {
		return nil, err
	}
	go cache.loopUpdate()
	return cache, nil
}

// cache更新策略：
// 1.每UpdateGap秒更新一次;
// 2.每次更新时按顺序选一个容器，读取容器里最多UpdateSize条变更记录，批量写入数据库
// 3.如果更新数量是UpdateSize，说明容器还有剩余待更新内容，下次继续更新上次的容器;
// 4.如果更新数量少于UpdateSize，下次更新会跳到下一个容器进行更新
func (cache *Cache) loopUpdate() {
	ctx, cancel := context.WithCancel(context.Background())
	cache.ctx = ctx
	cache.cancel = cancel
	updateIndex := 0
	var updateGap time.Duration = time.Duration(cache.dbConfig.UpdateGap) * time.Second
	timer := time.NewTimer(updateGap)
	for {
		select {
		case <-cache.ctx.Done():
			return
		case <-timer.C:
			if updateIndex >= len(cache.containerList) {
				updateIndex = 0
			} else {
				container := cache.containerList[updateIndex]
				// logger.Error("start update", container.objType)
				if container.updater.batchUpdate() == true {
					updateIndex++
				}
			}
			timer.Reset(updateGap)
		}
	}
}

// 初始化一个指定类型的容器，对应数据库一个表格;
// 当数据库断开链接而cache中没有数据，而容器又非preload预加载时，会抛出异常;
// 使用该container的gorutine，应该做recover处理，或者把容器设置成preload;
func (cache *Cache) InitContainer(objType reflect.Type, preload bool) {
	logger.Error("InitContainer", objType, len(cache.containerList))
	container := NewContainer(cache, objType, preload)
	cache.containers[objType] = container
	cache.containerList = append(cache.containerList, container)
}

// 从指定类型容器中，获取某个玩家的所有数据的CargoInt (玩家模块初始化，加载数据并共享到玩家结构体中)
func (cache *Cache) GetCargo(objType reflect.Type, sid uint32) CargoInt {
	return cache.containers[objType].getCargo(sid, false)
}

// 获取所有数据集合
func (cache *Cache) GetAllObjs(objType reflect.Type) []interface{} {
	return cache.containers[objType].getAllObjs()
}

// 获取某个玩家的单个数据(需要填满key)
func (cache *Cache) Lookup(objType reflect.Type, sid uint32, keys ...uint32) interface{} {
	return cache.containers[objType].Lookup(sid, keys...)
}

// 获取某个玩家的多个数据(自动根据key数量查找相应范围)
func (cache *Cache) LookupObjs(objType reflect.Type, sid uint32, keys ...uint32) []interface{} {
	return cache.containers[objType].LookupObjs(sid, keys...)
}

// 插入或更新某个数据
func (cache *Cache) Replace(objType reflect.Type, obj interface{}) {
	cache.containers[objType].Replace(obj)
}

// 删除某个数据
func (cache *Cache) Delete(objType reflect.Type, obj interface{}) {
	cache.containers[objType].Delete(obj)
}

// 删除某玩家的所有数据(多key)
func (cache *Cache) DeleteObjs(objType reflect.Type, sid uint32) {
	cache.containers[objType].DeleteObjs(sid)
}

// 不经过数据库，预先初始化数据载体(新玩家登陆时用，数据库一般没有新玩家的数据，调用这个方法，可以免去容器查询数据库的过程)
func (cache *Cache) PreInitObjs(sid uint32) {
	for _, container := range cache.containers {
		container.preInitCargo(sid)
	}
}

// 获取下一个Uid
func (cache *Cache) GetNextUid(objType reflect.Type, sid uint32) uint32 {
	return cache.containers[objType].GetNextUid(sid)
}

// 马上把所有数据刷到数据库(服务器关闭时用)
func (cache *Cache) FlushAll() {
	for _, container := range cache.containers {
		for {
			if container.updater.batchUpdate() == true {
				break
			}
		}
	}
}

// 设置玩家数据的内存回收标志
func (cache *Cache) SetGC(sid uint32) {
	for _, container := range cache.containerList {
		if !container.preload {
			container.SetGC(sid)
		}
	}
}

// 去除玩家数据的内存回收标志
func (cache *Cache) UnSetGC(sid uint32) {
	for _, container := range cache.containerList {
		if !container.preload {
			container.UnSetGC(sid)
		}
	}
}

// 数据库连接初始化
func (cache *Cache) initDB() error {
	dbCfg := cache.dbConfig
	dbArgs := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", dbCfg.DBUser, dbCfg.DBPass, dbCfg.DBHost, dbCfg.DBPort, dbCfg.DBName)
	namingStrategy := schema.NamingStrategy{
		SingularTable: true,
	}
	d, err := gorm.Open(mysql.Open(dbArgs), &gorm.Config{
		NamingStrategy:         namingStrategy,
		SkipDefaultTransaction: true,
		Logger: gormlog.New(
			Writer{},
			gormlog.Config{
				SlowThreshold:             500 * time.Millisecond, // Slow SQL threshold
				LogLevel:                  gormlog.Warn,           // Log level
				IgnoreRecordNotFoundError: true,                   // Ignore ErrRecordNotFound error for logger
				Colorful:                  false,                  // Disable color
			},
		),
	})
	if err != nil {
		return err
	}
	cache.dbCon = d
	return nil
}

// 自定义writer，输出gorm警报到日志
type Writer struct {
}

func (w Writer) Printf(format string, args ...interface{}) {
	slowSql := fmt.Sprintf(format, args...)
	maxSize := 500
	if len(slowSql) > maxSize {
		slowSql = slowSql[0:maxSize]
	}
	logger.Error(slowSql)
}
