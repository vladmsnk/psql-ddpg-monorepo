package discovery

import "errors"

var (
	ErrInstanceAlreadyExists = errors.New("instance already exists")
)

type InstanceStatus int64

const (
	InstanceStatusUnspecified InstanceStatus = 0
	InstanceStatusNew         InstanceStatus = 1
	InstanceStatusRunning     InstanceStatus = 2
)

type CollectorInstance struct {
	Id     string         `json:"id"`
	Name   string         `json:"name"`
	Host   string         `json:"host"`
	Port   int            `json:"port"`
	Status InstanceStatus `json:"status"`
}
