package cast

import (
	"log"
	"strconv"
)

func Atoi(s string, defVal int32) int32 {
	res, err := strconv.Atoi(s)
	if err != nil {
		log.Printf("Atoi %v err %v", s, err)
		return defVal
	}
	return int32(res)
}

func ParseInt(s string, defVal int64) int64 {
	res, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Printf("ParseInt %v err %v", s, err)
		return defVal
	}
	return res
}

func Itoa(n int) string {
	return strconv.Itoa(n)
}

func FormatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}
