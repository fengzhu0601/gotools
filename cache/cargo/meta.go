package cargo

// meta记录(包含每一条元数据和数据状态标识)
type meta struct {
	dbFlag MetaFlag    // 数据库更新标识
	obj    interface{} // 存储对象
}

// 更新meta对象
func (r *meta) Update(i interface{}) {
	r.obj = i
	r.dbFlag = FLAG_UPDATE
}

// 删除meta对象
func (r *meta) DeleteObj() {
	if r.obj != nil {
		r.obj = nil
		r.dbFlag = FLAG_DELETE
	}
}
