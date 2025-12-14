package utils

import "time"

func GetTimeStamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}