package cargo

// 元数据的状态
type MetaFlag byte

const (
	FLAG_NONE   MetaFlag = 0 // 无变化
	FLAG_UPDATE MetaFlag = 1 // 待更新
	FLAG_DELETE MetaFlag = 2 // 待删除
)

// 整个载体的状态
type CargoStatus byte

const (
	STATUS_NORMAL CargoStatus = 0 // 无变化
	STATUS_CHANGE CargoStatus = 1 // 有变更
	STATUS_SYNC   CargoStatus = 2 // 正在同步数据库
)
