package spider

import uuid "github.com/satori/go.uuid"

func GetUUID() string {
	//uid := uuid.Must(uuid.NewV4(), nil)
	//return uid.String()
	return uuid.NewV4().String()
}
