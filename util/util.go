package util

import "strconv"

// -
//	taskQueueTag
//	任务队列标识
var taskQueueTag = ""

func SetTaskQueueTag(tag string) {
	taskQueueTag = tag
}

// Output
func Output(args ...int64) string {

	if len(args) < 1 {
		return taskQueueTag + "*"
	}

	format := taskQueueTag

	for _, v := range args {
		format += ":" + strconv.FormatInt(v, 10)
	}

	return format

}
