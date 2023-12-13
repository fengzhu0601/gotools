package cache

import (
	"html/template"
	"net/http"
	"os"
)

var htmlPath string

type ContainerProf struct {
	CellName      string // Tab名称
	CellNum       uint32 // Cell总数
	ChangeCellNum uint32 // 有变动的Cell数量
	GCellNum      uint32 // 待回收的Cell数量
	GcCellNum     uint64 // 回收的Cell总数
	ObjNum        uint32 // Obj总数
	ObjMemory     uint32 // 内存占用(K)
	UpdateObjNum  uint32 // 待更新Obj数量
	DeleteObjNum  uint32 // 待删除Obj数量
	DBLoadNum     uint64 // db加载的Obj总数
	DBUpdateNum   uint64 // db更新的Obj总数
	DBDeleteNum   uint64 // db删除的Obj总数
	CellReads     int64  // cell读次数
	CellWrites    int64  // cell写次数
}

func (c *Cache) PrintCache(w http.ResponseWriter, r *http.Request) {
	t, err := template.ParseFiles(htmlPath + "cache.html")
	if err != nil {
		logger.Error("ParseFiles error", err)
		return
	}

	pprofList := make([]*ContainerProf, 0)
	for _, container := range c.containerList {
		prof := container.getProfInfo()
		pprofList = append(pprofList, prof)
	}

	err = t.Execute(w, pprofList)
	if err != nil {
		logger.Error("Execute error", err)
		return
	}
}

func init() {
	paths := []string{"./html/", "../html/", "../../html/"}
	for _, path := range paths {
		if pathExists(path) {
			htmlPath = path
			break
		}
	}
}

// 判断所给路径文件/文件夹是否存在
func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
