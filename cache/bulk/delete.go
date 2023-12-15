package bulk

import (
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// 批量删除
// "DELETE FROM `items` where (`sid`, `cfg_id`) IN ((1,2),(3,4));"
func BulkDelete(db *gorm.DB, t reflect.Type, keys []interface{}) error {
	if len(keys) == 0 {
		return nil
	}
	keyNum := keyNum(keys[0])
	tableName := getTableNameByType(t)
	tableName = escapeTabName(tableName)

	_, aTags := getTags(t)

	escapeTags := make([]string, keyNum)
	for i := 0; i < keyNum; i++ {
		escapeTags[i] = escapeSqlName(aTags[i])
	}
	fields := strings.Join(escapeTags, ", ")

	objPlaceholders := keyNum
	batchSize := MaximumPlaceholders / objPlaceholders
	tx := db.Begin()
	for i := 0; i < len(keys)/batchSize+1; i++ {
		maxBatchIndex := (i + 1) * batchSize
		if maxBatchIndex > len(keys) {
			maxBatchIndex = len(keys)
		}

		valueArgs := keySliceValues(keys[i*batchSize:maxBatchIndex], keyNum)

		phStrs := make([]string, maxBatchIndex-i*batchSize)
		placeholderStrs := "(?" + strings.Repeat(", ?", keyNum-1) + ")"
		for j := range keys[i*batchSize : maxBatchIndex] {
			phStrs[j] = placeholderStrs
		}

		smt := fmt.Sprintf("DELETE FROM %s where (%s) IN (%s)", tableName, fields, strings.Join(phStrs, ","))
		err := tx.Exec(smt, valueArgs...).Error

		logger.Debug("db delete ", smt, valueArgs)

		if err != nil {
			tx.Rollback()
			return err
		}

	}

	return tx.Commit().Error
}

func getTableNameByType(t reflect.Type) string {

	if _, ok := t.MethodByName(funcTableName); ok {
		v := reflect.ValueOf(t).MethodByName(funcTableName).Call(nil)
		if len(v) > 0 {
			return v[0].String()
		}
	}
	name := ""
	if t.Kind() == reflect.Ptr {
		name = t.Elem().Name()
	} else {
		name = t.Name()
	}

	return toSnakeCase(name)
}

func keyNum(obj interface{}) int {
	return reflect.ValueOf(obj).Elem().NumField()
}

func keySliceValues(keys []interface{}, keyNum int) []interface{} {

	availableValues := make([]interface{}, len(keys)*keyNum)

	c := 0
	for i := range keys {
		v := reflect.ValueOf(keys[i]).Elem()
		for j := 0; j < keyNum; j++ {
			availableValues[c] = v.Field(j).Interface()
			c += 1
		}
	}

	return availableValues
}
