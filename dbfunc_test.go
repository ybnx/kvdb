package kvdb

import (
	"fmt"
	"github.com/tidwall/btree"
	"testing"
)

func TestDBPut(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Put("haha", "heihei")
	err = db.Put("woo", "caoo")
	val, err := db.Get("woo")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val[0]))
	err = db.Put("del", "this")
	if err != nil {
		t.Error(err)
	}
}

func TestDBGetPut(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	val, err := db.GetPut("haha", "hehe")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val[0]))
}

func TestSub(t *testing.T) {
	t.Run("put", TestDBPut)
	t.Run("get", TestDBGetPut)
}

func TestDBGet(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	val1, err := db.Get("haha")
	if err != nil {
		t.Error(err)
	}
	val2, err := db.Get("woo")
	if err != nil {
		t.Error(err)
	}
	val3, err := db.Get("del")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(val1[0]))
	fmt.Println(string(val2[0]))
	fmt.Println(string(val3[0]))
}

func TestDBDelete(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Delete("del")
	if err != nil {
		t.Error(err)
	}
}

func TestDBGetDelete(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	val, err := db.GetDelete("haha")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(val[0]))
}

func TestDBPut2(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Put("user", "admin")
	err = db.Put("user1", "feige")
	err = db.Put("user2", "coke")
	err = db.Put("user3", "three")
	err = db.Put("user10086", "oooooo")
	if err != nil {
		t.Error(err)
	}
}

func TestDBRange(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	vals, err := db.Range("user", "user3")
	if err != nil {
		t.Error(err)
	}
	for _, val := range vals {
		fmt.Println(string(val))
	}
}

func TestDBPrefix(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	vals, err := db.Prefix("user")
	if err != nil {
		t.Error(err)
	}
	for _, val := range vals {
		fmt.Println(string(val))
	}
}

func TestDBSuffix(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	vals, err := db.Suffix("10086")
	if err != nil {
		t.Error(err)
	}
	for _, val := range vals {
		fmt.Println(string(val))
	}
}

func TestTxPrefix(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	var vals [][]byte
	err = db.Update(func(tx *TX) error {
		var err error
		err = tx.Put("user9", "passwd9")
		vals, err = tx.Prefix("user")
		return err
	})
	if err != nil {
		t.Error(err)
	}
	for _, val := range vals {
		fmt.Println(string(val))
	}
}

func TestTx(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Update(func(tx *TX) error {
		err = tx.Put("shen", "me")
		if err != nil {
			panic(err)
		}
		err = tx.Put("nihao", "aaa")
		if err != nil {
			panic(err)
		}
		err = tx.Put("test", "struct")
		if err != nil {
			panic(err)
		}
		return err
	})
	if err != nil {
		panic(err)
	}
}

func TestTx2(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.View(func(tx *TX) error {
		var var1 [][]byte
		var1, err = tx.Get("shen")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(var1[0]))
		var var2 [][]byte
		var2, err = tx.Get("test")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(var2[0]))
		var var3 [][]byte
		var3, err = tx.Get("nihao")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(var3[0]))
		fmt.Println()
		return err
	})
	if err != nil {
		panic(err)
	}
}

func TestBtree(t *testing.T) {
	hints := btree.New(hintLess)
	hints.Set(NewHint([]byte("woo"), 3))
	fmt.Println(hints.Get(&Hint{key: []byte("woo")}).(*Hint).offset)
	hints.Set(NewHint([]byte("woo"), 4))
	fmt.Println(hints.Get(&Hint{key: []byte("woo")}).(*Hint).offset)
}
