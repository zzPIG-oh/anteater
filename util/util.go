package util

import "fmt"

// -
//	taskQueueTag
//	任务队列标识
var taskQueueTag = ""

func SetTaskQueueTag(tag string) {
	taskQueueTag = tag
	// "mix:user:resource"
}

// Output
func Output(args ...int64) string {

	if len(args) < 1 {
		return taskQueueTag + "*"
	}

	format := ""
	for range args {
		format += ":%d"
	}

	return fmt.Sprintf(taskQueueTag+format, args)

}
