package cache

import (
	"fmt"
	"github.com/fengzhu0601/goproject/go_tool/logger"
	"reflect"
	"time"
)

type selectReq struct {
	sid      uint32        // 主键
	backChan chan struct{} // 回复chan
}

type selector struct {
	container *Container        // 所属容器
	loading   bool              // 是否加载中
	waitList  chan *selectReq   // 等待加载数据的请求列表
	doList    chan []*selectReq // 正在批量加载数据的请求列表
}

func newSelector(c *Container) *selector {
	return &selector{
		container: c,
		waitList:  make(chan *selectReq, 100),
		doList:    make(chan []*selectReq, 10),
	}
}

// 把一个请求放入waitList中，并等待结果
func (s *selector) load(sid uint32) bool {
	backChan := make(chan struct{})
	newReq := &selectReq{
		sid:      sid,
		backChan: backChan,
	}
	s.waitList <- newReq
	t := time.NewTimer(3 * time.Second)
	select {
	case <-backChan:
		break
	case <-t.C:
		panic(fmt.Sprintf("load db timeout, objType:%s sid:%d", s.container.objType, sid))
	}
	t.Stop()
	return true
}

// 每个selector运行两个gorutine
// 第一个负责接收并打包请求
// 第二个负责处理打包好的请求列表，批量从数据库加载数据
func (s *selector) startRun() {
	go func() {
		var reqList []*selectReq
		for {
			req := <-s.waitList
			if req != nil {
				reqList = append(reqList, req)
			}
			// 如果加载协程就绪，就把打包好的请求发送
			if !s.loading && len(reqList) > 0 {
				s.loading = true
				pass := make([]*selectReq, len(reqList))
				copy(pass, reqList)
				s.doList <- pass
				reqList = reqList[0:0]
			}
		}
	}()

	go func() {
		var sidList []uint32
		for {
			reqDoList := <-s.doList
			// 处理打包好的请求列表
			for _, req := range reqDoList {
				sidList = append(sidList, req.sid)
			}
			err := s.loadFromDB(sidList)
			if err != nil {
				logger.Debug("loadFromDB error", s.container.objType, len(sidList))
			} else {
				for _, req := range reqDoList {
					// 通知所有请求者，数据加载完成
					close(req.backChan)
				}
				sidList = sidList[0:0]
				s.loading = false
				s.waitList <- nil
			}
		}
	}()
}

// 从db批量加载数据
func (s *selector) loadFromDB(sidList []uint32) error {
	sliceT := reflect.SliceOf(reflect.PtrTo(s.container.objType))
	slice := reflect.New(sliceT)
	sliceInt := slice.Interface()
	tx := s.container.cache.dbCon.Model(sliceInt).Where("sid in (?)", sidList).Find(sliceInt)
	if tx.Error != nil {
		return tx.Error
	}
	// time.Sleep(1 * time.Second)
	datas := slice.Elem()
	// 批量数据载入到容器中
	// logger.Debug("loadFromDB ", s.container.objType, len(sidList))
	s.container.loadDBData(sidList, datas)
	return nil
}
