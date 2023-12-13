package cache

// 单个玩家数据集的单元
type Cell struct {
	status      CellStatus // 数据状态
	releaseTime int64      // 释放时间戳(玩家下线时设置，到期后updater会把数据从内存中移除)
	cargo       CargoInt   // 数据载体接口
}

// 单元状态
type CellStatus byte

const (
	STATUS_NORMAL CellStatus = 0 // 无变化
	STATUS_CHANGE CellStatus = 1 // 有变更
	STATUS_SYNC   CellStatus = 2 // 正在同步数据库
)

func (c *Cell) isChange() bool {
	return c.status != STATUS_NORMAL
}
