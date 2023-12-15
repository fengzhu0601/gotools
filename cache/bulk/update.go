package bulk

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/fengzhu0601/gotools/logger"
	"gorm.io/gorm"
)

const (
	MaximumPlaceholders = 65536 // 最大占位符数量
	gormTag             = "gorm"
	columnPrefix        = "column:"
	funcTableName       = "TableName"
)

// 批量更新
// "REPLACE INTO `items` (`sid`, `cfg_id`, `stack_num`) VALUES (1, 2, 3),(1, 3, 4);"
func BulkUpdate(db *gorm.DB, bulks []interface{}) error {
	if len(bulks) == 0 {
		return nil
	}
	return BulkUpdateWithTableName(db, getTableName(bulks[0]), bulks)
}

func BulkUpdateWithTableName(db *gorm.DB, tableName string, bulks []interface{}) error {
	tableName = escapeTabName(tableName)

	tags, aTags := getTags(reflect.TypeOf(bulks[0]).Elem())

	escapeTags := make([]string, len(aTags))
	for i := range aTags {
		escapeTags[i] = escapeSqlName(aTags[i])
	}
	fields := strings.Join(escapeTags, ", ")

	objPlaceholders := len(aTags)

	batchSize := MaximumPlaceholders / objPlaceholders

	tx := db.Begin()

	bulksLen := len(bulks)

	for i := 0; i < bulksLen/batchSize+1; i++ {
		maxBatchIndex := (i + 1) * batchSize
		if maxBatchIndex > bulksLen {
			maxBatchIndex = bulksLen
		}

		valueArgs := sliceValues(bulks[i*batchSize:maxBatchIndex], tags, aTags)

		phStrs := make([]string, maxBatchIndex-i*batchSize)
		placeholderStrs := "(?" + strings.Repeat(", ?", len(aTags)-1) + ")"
		for j := range bulks[i*batchSize : maxBatchIndex] {
			phStrs[j] = placeholderStrs
		}

		smt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s", tableName, fields, strings.Join(phStrs, ","))
		err := tx.Exec(smt, valueArgs...).Error

		// logger.Debug("db update ", tableName, bulksLen)

		if err != nil {
			logger.Error("db update error ", tableName, bulksLen, err)
			tx.Rollback()
			return err
		}

	}

	return tx.Commit().Error
}

func getTableName(t interface{}) string {
	st := reflect.TypeOf(t)
	if _, ok := st.MethodByName(funcTableName); ok {
		v := reflect.ValueOf(t).MethodByName(funcTableName).Call(nil)
		if len(v) > 0 {
			return v[0].String()
		}
	}

	name := ""
	if t := reflect.TypeOf(t); t.Kind() == reflect.Ptr {
		name = t.Elem().Name()
	} else {
		name = t.Name()
	}

	return toSnakeCase(name)
}

func getTags(t reflect.Type) ([]string, []string) {
	re := regexp.MustCompile(fmt.Sprintf("(?i)%s[a-z0-9_\\-]+", columnPrefix))
	tags := make([]string, t.NumField())

	for j := 0; j < t.NumField(); j++ {

		field := t.Field(j)
		tag := field.Tag.Get(gormTag)
		if tag == "-" {
			continue
		}

		tag = re.FindString(tag)
		if strings.HasPrefix(tag, columnPrefix) {
			tag = strings.TrimPrefix(tag, columnPrefix)
		} else {
			tag = toSnakeCase(field.Name)
		}

		tags[j] = tag
	}

	availableTags := []string{}
	for i := range tags {
		if tags[i] != "" {
			availableTags = append(availableTags, tags[i])
		}
	}

	return tags, availableTags
}

func sliceValues(objs []interface{}, tags, aTags []string) []interface{} {

	updateSize := len(aTags)

	availableValues := make([]interface{}, len(objs)*updateSize)

	c := 0
	for i := range objs {
		v := reflect.ValueOf(objs[i]).Elem()

		for j := 0; j < v.NumField(); j++ {
			if tags[j] != "" {
				availableValues[c] = v.Field(j).Interface()
				c += 1
			}
		}
	}

	return availableValues
}
