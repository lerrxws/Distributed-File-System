package dfs

import "strings"

func getParentPath(path string) string {
	path = strings.ReplaceAll(path, "\\", "/")

	if path == "/" {
		return "/"
	}

	path = strings.TrimSuffix(path, "/")

	idx := strings.LastIndex(path, "/")
	if idx == -1 {
		return "/"
	}

	parent := path[:idx+1]
	return parent
}