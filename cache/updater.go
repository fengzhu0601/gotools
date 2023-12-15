package cache

import (
	"github.com/fengzhu0601/gotools/cache/bulk"
	"github.com/fengzhu0601/gotools/logger"
)

type updater struct {
	container    *Container // 所属容器
	updateTriger chan byte  // 等待加载数据的请求列表
}

func newUpdater(c *Container) *updater {
	return &updater{
		container:    c,
		updateTriger: make(chan byte, 10),
	}
}

func (u *updater) batchUpdate() bool {
	updateSize := u.container.cache.dbConfig.UpdateSize
	updateObjs, deleteKeys := u.container.scanChangeObjs(uint32(updateSize))
	// 更新,删除变更记录
	err := u.replace(updateObjs)
	if err != nil {
		logger.Error("cache update error:", u.container.objType, len(updateObjs), err)
		u.container.afterSyncDb(false)
		return true
	}
	updateNum := len(updateObjs)
	u.container.dbUpdateNum += uint64(updateNum)
	err = u.delete(deleteKeys)
	if err != nil {
		logger.Error("cache delete error:", u.container.objType, len(deleteKeys), err)
		u.container.afterSyncDb(false)
		return true
	}
	deleteNum := len(deleteKeys)
	u.container.dbDeleteNum += uint64(deleteNum)
	allUpdate := false
	if updateNum+deleteNum < updateSize {
		allUpdate = true
	}
	u.container.afterSyncDb(true)
	// 还有其他内容，继续批量更新
	return allUpdate
}

// 利用replace语句进行批量更新
func (u *updater) replace(updateObjs []interface{}) error {
	return bulk.BulkUpdate(u.container.cache.dbCon, updateObjs)
}

// 从数据库中批量删除
func (u *updater) delete(deleteKeys []interface{}) error {
	return bulk.BulkDelete(u.container.cache.dbCon, u.container.objType, deleteKeys)
}
