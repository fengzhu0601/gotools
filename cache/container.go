package cache

import (
	"github.com/fengzhu0601/gotools/cache/cargo"
	"github.com/fengzhu0601/gotools/logger"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type cellMap map[uint32]*Cell

type Container struct {
	cache     *Cache       // 所属的cache主体
	objType   reflect.Type // 数据类型
	cargoType reflect.Type // 载体类型
	preload   bool         // 是否预加载
	cells     sync.Map     // 载体集合
	cellLock  sync.Mutex   // cell锁
	selector  *selector    // db select 协程
	updater   *updater     // db update 协程

	dbLoadNum   uint64 // db加载的Obj总数
	dbUpdateNum uint64 // db更新的Obj总数
	dbDeleteNum uint64 // db删除的Obj总数
	gcCellNum   uint64 // gc的cell数量
	gcObjNum    uint64 // gc的obj数量
	cellReads   int64  // cell读次数
	cellWrites  int64  // cell写次数
}

// 新建容器
func NewContainer(cache *Cache, objType reflect.Type, preload bool) *Container {
	container := &Container{
		cache:     cache,
		objType:   objType,
		cargoType: getCargoType(objType),
		preload:   preload,
		// cells:     make(cellMap),
	}
	selector := newSelector(container)
	updater := newUpdater(container)
	container.selector = selector
	container.updater = updater
	obj := reflect.New(objType).Interface()
	err := cache.dbCon.Migrator().AutoMigrate(obj)
	if err != nil {
		panic(err)
	}
	container.doPreload()
	selector.startRun()
	return container
}

// 通过obj的primaryKey数量获取对应的cargo类型
func getCargoType(objType reflect.Type) reflect.Type {
	keyNum := 0
	for j := 0; j < objType.NumField(); j++ {
		field := objType.Field(j)
		tag := field.Tag.Get("gorm")
		if tag == "primaryKey" {
			keyNum += 1
		}
	}
	switch keyNum {
	case 1:
		// 单主键
		cargoPtr := (*cargo.Cargo)(nil)
		return reflect.TypeOf(cargoPtr).Elem()
	case 2:
		// 双主键
		cargoPtr := (*cargo.CargoMap)(nil)
		return reflect.TypeOf(cargoPtr).Elem()
	default:
		// 其他数量主键暂不支持
		logger.Debug("cant find cargoType, keyNum", keyNum, objType)
		panic("cargo keyNum error")
	}
}

// 不经过数据库，直接初始化容器(新玩家登陆时用，数据库一般没有新玩家的数据，调用这个方法，可以免去容器查数据库的过程)
func (c *Container) preInitCargo(sid uint32) {
	_, exit := c.cellLoad(sid)
	if !exit && !c.preload {
		newCargo := reflect.New(c.cargoType).Interface().(CargoInt)
		newCargo.CargoInit()
		c.cellStore(sid, &Cell{cargo: newCargo})
	}
}

// 从容器中获取某个玩家的所有数据集合
//
// willChange表示获取数据后，是否将要改变数据。true的时候，设置一下cell的status为变更状态
func (c *Container) getCargo(sid uint32, willChange bool) CargoInt {
	cell, exit := c.cellLoad(sid)
	if !exit {
		if c.preload {
			// 预加载的表格，不需要搜索数据库。直接初始化载体即可
			// 预加载数据或者已加载数据，在数据库断开连接的情况下，也能先在缓存上增删改查。数据库连接上后，再同步数据到数据库即可
			newCargo := reflect.New(c.cargoType).Interface().(CargoInt)
			newCargo.CargoInit()
			// 锁上后开始添加新的数据载体
			c.cellLock.Lock()
			defer c.cellLock.Unlock()
			// 多个进程可能会同时等待锁，解锁后可能上一个进程已添加数据，需要再检查一次是否已经有数据
			cell2, exit1 := c.cellLoad(sid)
			if !exit1 {
				newCell := &Cell{cargo: newCargo}
				if willChange {
					newCell.status = STATUS_CHANGE
				}
				c.cellStore(sid, newCell)
				return newCargo
			}
			if willChange {
				cell2.status = STATUS_CHANGE
			}
			return cell2.cargo
		} else {
			// 不存在载体的非预加载数据，需要从数据库中加载
			// 这里有个阻塞，从数据库中加载数据到cells中
			// 数据库出错时，会抛出panic，需要在进程的调用入口加入recover处理(一般只有player需要做处理，其他协程调用的都是预加载数据)
			c.selector.load(sid)
			cell1, exit1 := c.cellLoad(sid)
			if !exit1 {
				return nil
			}
			if willChange {
				cell1.status = STATUS_CHANGE
			}
			return cell1.cargo
		}
	}
	if willChange {
		cell.status = STATUS_CHANGE
	}
	return cell.cargo
}

// 获取所有obj
func (c *Container) getAllObjs() []interface{} {
	objs := make([]interface{}, 0)
	c.cells.Range(
		func(k any, v any) bool {
			v.(*Cell).cargo.CollectAllObjs(&objs)
			return true
		})
	return objs
}

// 获取prof信息
func (c *Container) getProfInfo() *ContainerProf {
	prof := &ContainerProf{CellName: c.objType.Name()}
	updateObjs := make([]interface{}, 0)
	deleteKeys := make([]interface{}, 0)
	c.cells.Range(
		func(k any, v any) bool {
			cell := v.(*Cell)
			sid := k.(uint32)
			cell.cargo.CollectChangedObjs(sid, &updateObjs, &deleteKeys, true)
			prof.CellNum++
			if cell.isChange() {
				prof.ChangeCellNum++
			}
			if c.preload == false && cell.status == STATUS_NORMAL && cell.releaseTime > 0 {
				prof.GCellNum++
			}
			return true
		})
	prof.ObjNum = uint32(len(c.getAllObjs()))
	prof.UpdateObjNum = uint32(len(updateObjs))
	prof.DeleteObjNum = uint32(len(deleteKeys))
	prof.DBLoadNum = c.dbLoadNum
	prof.DBUpdateNum = c.dbUpdateNum
	prof.DBDeleteNum = c.dbDeleteNum
	prof.GcCellNum = c.gcCellNum
	prof.CellReads = c.cellReads
	prof.CellWrites = c.cellWrites
	prof.ObjMemory = prof.ObjNum * uint32(c.objType.Size()) / 1024
	return prof
}

// 获取某个玩家的一批objs
func (c *Container) LookupObjs(sid uint32, keys ...uint32) []interface{} {
	cargo := c.getCargo(sid, false)
	return cargo.GetSomeObjs(keys...)
}

// 获取某个玩家的单个obj
func (c *Container) Lookup(sid uint32, keys ...uint32) interface{} {
	cargo := c.getCargo(sid, false)
	return cargo.GetSingleObj(keys...)
}

// 更新或插入某个obj
func (c *Container) Replace(obj interface{}) bool {
	sid := uint32(reflect.ValueOf(obj).Elem().Field(0).Uint())
	cargo := c.getCargo(sid, true)
	cargo.Replace(obj)
	return true
}

// 删除某个obj
func (c *Container) Delete(obj interface{}) bool {
	sid := uint32(reflect.ValueOf(obj).Elem().Field(0).Uint())
	cargo := c.getCargo(sid, true)
	cargo.DeleteObj(obj)
	return true
}

// 删除某个obj
func (c *Container) DeleteObjs(sid uint32) bool {
	cargo := c.getCargo(sid, true)
	cargo.DeleteObjs()
	return true
}

// 双主键获取下一个Uid
func (c *Container) GetNextUid(sid uint32) uint32 {
	cargo := c.getCargo(sid, false)
	return cargo.GetNextUid()
}

// 设置玩家数据的内存回收标志
func (c *Container) SetGC(sid uint32) {
	cell, exit := c.cellLoad(sid)
	if exit {
		cell.releaseTime = time.Now().Unix() + c.cache.dbConfig.GCSeconds
	}
}

// 去除玩家数据的内存回收标志
func (c *Container) UnSetGC(sid uint32) {
	cell, exit := c.cellLoad(sid)
	if exit {
		cell.releaseTime = 0
	}
}

func (c *Container) cellLoad(sid uint32) (*Cell, bool) {
	if c.cache.dbConfig.RWAnalyse {
		atomic.AddInt64(&c.cellReads, 1)
	}
	cell, exit := c.cells.Load(sid)
	if exit {
		return cell.(*Cell), exit
	}
	return nil, exit
}

func (c *Container) cellStore(sid uint32, cell *Cell) {
	if c.cache.dbConfig.RWAnalyse {
		atomic.AddInt64(&c.cellWrites, 1)
	}
	c.cells.Store(sid, cell)
}

// 扫描容器中变更的obj集合
func (c *Container) scanChangeObjs(num uint32) ([]interface{}, []interface{}) {
	updateObjs := make([]interface{}, 0)
	deleteKeys := make([]interface{}, 0)

	var scanNum uint32 = 0
	c.cells.Range(func(k any, v any) bool {
		cell := v.(*Cell)
		sid := k.(uint32)
		if scanNum >= num {
			return false
		}
		if !cell.isChange() {
			return true
		}
		scanNum += cell.cargo.CollectChangedObjs(sid, &updateObjs, &deleteKeys, true)
		cell.status = STATUS_SYNC
		return true
	})

	return updateObjs, deleteKeys
}

// 同步数据库后的操作
func (c *Container) afterSyncDb(success bool) {
	// c.cellLock.Lock()
	// defer c.cellLock.Unlock()
	now := time.Now().Unix()
	c.cells.Range(func(k any, v any) bool {
		cell := v.(*Cell)
		if cell.status == STATUS_SYNC {
			cell.cargo.AfterSyncDB(success)
			if success {
				cell.status = STATUS_NORMAL
			} else {
				cell.status = STATUS_CHANGE
			}
		}
		if c.preload == false && cell.status == STATUS_NORMAL && cell.releaseTime > 0 && cell.releaseTime < now {
			// 非预加载的数据，到期后从内存释放
			c.gcCellNum++
			if c.cache.dbConfig.RWAnalyse {
				atomic.AddInt64(&c.cellWrites, 1)
			}
			c.cells.Delete(k)
		}
		return true
	})
}

// 批量加载数据库数据到cells中
func (c *Container) loadDBData(sidList []uint32, datas reflect.Value) {

	for _, sid := range sidList {
		_, exit := c.cellLoad(sid)
		if !exit {
			// 无论数据库中有没有数据，只要搜索都需要初始化载体。防止缓存穿透
			newCargo := reflect.New(c.cargoType).Interface().(CargoInt)
			newCargo.CargoInit()
			c.cellStore(sid, &Cell{cargo: newCargo})
		}
	}

	len := datas.Len()
	for i := 0; i < len; i++ {
		element := datas.Index(i)
		sid := uint32(element.Elem().Field(0).Uint())
		cell, exit := c.cellLoad(sid)
		if !exit {
			logger.Error("cell not exit", sid)
		} else {
			cell.cargo.LoadDBData(element)
		}
	}
	c.dbLoadNum += uint64(len)
}

// 预加载数据
func (c *Container) doPreload() {
	if !c.preload {
		return
	}

	loadStartTime := time.Now()
	sliceT := reflect.SliceOf(reflect.PtrTo(c.objType))
	slice := reflect.New(sliceT)
	sliceInt := slice.Interface()
	c.cache.dbCon.Model(sliceInt).Find(sliceInt)
	datas := slice.Elem()
	len := datas.Len()
	loadTime := time.Since(loadStartTime)

	insertStartTime := time.Now()
	for i := 0; i < len; i++ {
		element := datas.Index(i)
		sid := uint32(element.Elem().Field(0).Uint())
		cell, exit := c.cellLoad(sid)
		if !exit {
			newCargo := reflect.New(c.cargoType).Interface().(CargoInt)
			newCargo.CargoInit()
			newCell := &Cell{cargo: newCargo}
			newCell.cargo.LoadDBData(element)
			c.cellStore(sid, newCell)
		} else {
			cell.cargo.LoadDBData(element)
		}
	}
	insertTime := time.Since(insertStartTime)

	logger.Info("cache doPreload insert cells:", c.objType, "size:", len, "loadTIme:", loadTime, "insertTime:", insertTime)

}
