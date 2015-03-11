package util

import (
	"os"
	"strings"
)

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

// return schemaname, tablename
func SplitToSchemaOrTable(st string) (string, string) {
	if st != "" {
		tmp := strings.Split(st, ".")
		if len(tmp) == 1 {
			return "", tmp[0]
		} else if len(tmp) == 2 {
			return tmp[0], tmp[1]
		}
		return "", ""
	}
	return "", ""
}
