package kvdb

import (
	"strconv"
)

type cmdStrHandler func(tx *TX, args []string) string

var (
	OK           = "ok"
	SyntaxError  = "syntax error"
	ExecuteError = "function execute failed"
	NotFound     = "key value not found"
)

var SupportedCommand = map[string]cmdStrHandler{
	"put":       putString,
	"putTTL":    putTTLString,
	"get":       getString,
	"delete":    deleteString,
	"getDelete": getDeleteString,
}

func putString(tx *TX, args []string) string {
	if len(args) != 2 {
		return SyntaxError
	}
	err := tx.PutString(args[0], args[1])
	if err != nil {
		return ExecuteError
	}
	return OK
}

func putTTLString(tx *TX, args []string) string {
	if len(args) != 3 {
		return SyntaxError
	}
	ttl, err := strconv.Atoi(args[2])
	if err != nil {
		return ExecuteError
	}
	err = tx.PutTTLString(args[0], args[1], uint64(ttl))
	if err != nil {
		return ExecuteError
	}
	return OK
}

func getString(tx *TX, args []string) string {
	if len(args) != 1 {
		return SyntaxError
	}
	res, err := tx.GetString(args[0])
	if err != nil {
		return ExecuteError
	}
	if res == "" {
		return NotFound
	}
	return res
}

func deleteString(tx *TX, args []string) string {
	if len(args) != 1 {
		return SyntaxError
	}
	err := tx.DeleteString(args[0])
	if err != nil {
		return ExecuteError
	}
	return OK
}

func getDeleteString(tx *TX, args []string) string {
	if len(args) != 1 {
		return SyntaxError
	}
	res, err := tx.GetString(args[0])
	if err != nil {
		return ExecuteError
	}
	if res == "" {
		return NotFound
	}
	err = tx.DeleteString(args[0])
	if err != nil {
		return ExecuteError
	}
	return res
}
