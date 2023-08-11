package kvdb

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/flower-corp/rosedb"
	"github.com/nutsdb/nutsdb"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

type Student struct {
	Name string
	Age  int
}

func TestBefore(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	err = db.Update(func(tx *TX) error {
		err := tx.Put("woooooooooooooooooooooooooooo", "caoooooooooooooooooooooooooooooo")
		err = tx.Put("foo", "bar")
		err = tx.Put("del", "this")
		err = tx.Put("swap", Student{Name: "oke", Age: 3})
		err = tx.Put("pi", 3.14)
		err = tx.Delete("del")
		return err
	})
	if err != nil {
		t.Error(err)
	}
	db.Close()
}

func TestOpen(t *testing.T) {
	programInfo()
	start1 := time.Now()
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		programInfo()
		t.Error(err)
	}
	elapse1 := time.Since(start1)
	fmt.Println(elapse1)
	programInfo()
	start2 := time.Now()
	db.Close()
	elapse2 := time.Since(start2)
	fmt.Println(elapse2)
	programInfo()
}

func TestUpdate(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	err = db.Update(func(tx *TX) error {
		err = tx.Put("woooooooooooooooooooooooooooo", "caoooooooooooooooooooooooooooooo")
		if err != nil {
			t.Error(err)
		}
		err = tx.Put("foo", "bar")
		if err != nil {
			t.Error(err)
		}
		err = tx.Put("del", "this")
		if err != nil {
			t.Error(err)
		}
		err = tx.Put("swap", Student{Name: "oke", Age: 3})
		if err != nil {
			t.Error(err)
		}
		err = tx.Put("pi", 3.14)
		if err != nil {
			t.Error(err)
		}
		err = tx.Delete("del")
		if err != nil {
			t.Error(err)
		}

		var foo string
		val1, err := tx.Get("foo")
		err = DecodeValue(val1[0], &foo)
		fmt.Printf("%#v %T\n", foo, foo)
		var swap Student
		val2, err := tx.Get("swap")
		err = DecodeValue(val2[0], &swap)
		fmt.Printf("%#v %T\n", swap, swap)
		var pi float64
		val3, err := tx.Get("pi")
		err = DecodeValue(val3[0], &pi)
		fmt.Printf("%#v %T\n", pi, pi)
		var woo string
		val4, err := tx.Get("woooooooooooooooooooooooooooo")
		err = DecodeValue(val4[0], &woo)
		fmt.Printf("%#v %T\n", woo, woo)
		var del string
		val5, err := tx.Get("del")
		err = DecodeValue(val5[0], &del)
		fmt.Printf("%#v %T\n", del, del)
		return err
	})
	if err != nil {
		t.Error(err)
	}
	fmt.Println(runtime.NumGoroutine())
	db.Close()
	fmt.Println(runtime.NumGoroutine())
}

func TestView(t *testing.T) {
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	err = db.View(func(tx *TX) error {
		var foo string
		val1, err := tx.Get("foo")
		err = DecodeValue(val1[0], &foo)
		fmt.Printf("%#v %T\n", foo, foo)
		var swap Student
		val2, err := tx.Get("swap")
		err = DecodeValue(val2[0], &swap)
		fmt.Printf("%#v %T\n", swap, swap)
		var pi float64
		val3, err := tx.Get("pi")
		err = DecodeValue(val3[0], &pi)
		fmt.Printf("%#v %T\n", pi, pi)
		var woo string
		val4, err := tx.Get("woooooooooooooooooooooooooooo")
		err = DecodeValue(val4[0], &woo)
		fmt.Printf("%#v %T\n", woo, woo)
		var del string
		val5, err := tx.Get("del")
		err = DecodeValue(val5[0], &del)
		fmt.Printf("%#v %T\n", del, del)
		return err
	})
	if err != nil {
		t.Error(err)
	}
	db.Close()
}

func BenchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		panic(err)
	}
	err = db.Update(func(tx *TX) error {
		var err error
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("foo%v", i))
			val := []byte(fmt.Sprintf("bar%v", i))
			err = tx.Put(key, val)
		}
		return err
	})
	if err != nil {
		b.Error(err)
	}

}

func TestBench(t *testing.T) {
	start1 := time.Now()
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	elapse1 := time.Since(start1)
	fmt.Println(elapse1)
	programInfo()

	start2 := time.Now()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100000; i++ {
			key := []byte(fmt.Sprintf("foo%v", i))
			val := []byte(fmt.Sprintf("bar%v", i))
			err = tx.Put(key, val)
		}
		return err
	})
	if err != nil {
		t.Error(err)
	}
	elapse2 := time.Since(start2)
	fmt.Println(elapse2)
	programInfo()

	start3 := time.Now()
	db.Close()
	elapse3 := time.Since(start3)
	fmt.Println(elapse3)
	programInfo()
}

func TestConcurrencyTime(t *testing.T) {
	programInfo()
	start := time.Now()
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}

	//for i := 1; i <= 100; i++ {
	//	go func(i int) {
	//		defer wg.Done()
	//		err = db.Update(func(tx *TX) error {
	//			key := []byte(fmt.Sprintf("foo%v", i))
	//			val := []byte(fmt.Sprintf("bar%v", i))
	//			err = tx.Put(key, val)
	//			return err
	//		})
	//		if err != nil {
	//			t.Error(err)
	//		}
	//	}(i)
	//}

	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			defer wg.Done()
			err = db.Update(func(tx *TX) error {
				for j := 1; j <= 1000; j++ {
					num := i*1000 + j
					key := fmt.Sprintf("foo%v", num)
					val := fmt.Sprintf("bar%v", num)
					err = tx.Put(key, val)
				}
				return err
			})
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	//err = db.View(func(tx *TX) error {
	//	for i := 725634; i < 725642; i++ {
	//		key := fmt.Sprintf("foo%v", i)
	//		tval := fmt.Sprintf("bar%v", i)
	//		var valbt []byte
	//		valbt, err = tx.Get(key)
	//		valstr := string(valbt)
	//		if tval != valstr {
	//			t.Log(fmt.Sprintf("not equal key:%v tval:%v val:%v", key, tval, valstr))
	//		}
	//		fmt.Print(valstr, " ")
	//	}
	//	return err
	//})
	//if err != nil {
	//	t.Error(err)
	//}

	db.Close()
	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestConcurrencyTime2(t *testing.T) {

	opt := DefaultOption()

	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			defer wg.Done()
			err = db.Update(func(tx *TX) error {
				for j := 1; j <= 10000; j++ {
					num := i*10000 + j
					key := fmt.Sprintf("foo%v", num)
					val := fmt.Sprintf("bar%v", num)
					err = tx.Put(key, val)
				}
				return err
			})
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	db.Close()

	file, err := os.Create("E:\\golangProject\\demo1\\test7\\log\\mem.pprof")
	if err != nil {
		panic(err)
	}
	err = pprof.WriteHeapProfile(file)
	if err != nil {
		panic(err)
	}
	file.Close()

}

func TestTime(t *testing.T) {
	programInfo()
	start := time.Now()
	opt := DefaultOption()
	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 10000000; i++ {
			//num := rand.Intn(1000000)
			key := fmt.Sprintf("foo%v", i)
			val := fmt.Sprintf("bar%v", i)
			err = tx.Put(key, val)
		}
		return err
	})
	if err != nil {
		t.Error(err)
	}
	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestGet(t *testing.T) {
	start := time.Now()
	opt := DefaultOption()

	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	err = db.View(func(tx *TX) error {
		for i := 51785; i < 51792; i++ {
			//num := rand.Intn(1000000)
			key := fmt.Sprintf("foo%v", i)
			tval := fmt.Sprintf("bar%v", i)
			var valbt [][]byte
			valbt, err = tx.Get(key)
			valstr := string(valbt[0])
			t.Log(fmt.Sprintf("key:%v tval:%v val:%v\n", key, tval, valstr))
			//if tval != valstr {
			//	t.Error(fmt.Sprintf("not equal key:%v tval:%v val:%v\n", key, tval, valstr))
			//}
		}
		return err
	})
	if err != nil {
		t.Error(err)
	}

	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestConcurGet(t *testing.T) {
	programInfo()
	start := time.Now()
	opt := DefaultOption()

	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			defer wg.Done()
			err = db.View(func(tx *TX) error {
				for j := 1; j <= 1000; j++ {
					num := i*1000 + j
					key := fmt.Sprintf("foo%v", num)
					tval := fmt.Sprintf("bar%v", num)
					var valbt [][]byte
					valbt, err = tx.Get(key)
					valstr := string(valbt[0])
					if tval != valstr {
						t.Error(fmt.Sprintf("not equal key:%v tval:%v val:%v\n", key, tval, valstr))
					}
				}
				return err
			})
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestConcurrency(t *testing.T) {
	programInfo()
	start := time.Now()
	opt := DefaultOption()
	//
	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	//var wg sync.WaitGroup
	//wg.Add(1000)
	//for i := 0; i < 1000; i++ {
	//	go func(i int) {
	//		defer wg.Done()
	//		err = db.Update(func(tx *TX) error {
	//			for j := 1; j <= 1000; j++ {
	//				num := i*1000 + j
	//				key := "okoeswaphahaha"
	//				val := fmt.Sprintf("bar%v", num)
	//				err = tx.Put(key, val)
	//			}
	//			return err
	//		})
	//		if err != nil {
	//			t.Error(err)
	//		}
	//	}(i)
	//}
	//wg.Wait()

	err = db.Update(func(tx *TX) error {
		for i := 0; i < 1000000; i++ {
			//num := rand.Intn(1000000)
			key := "okoeswaphahaha"
			val := fmt.Sprintf("bar%v", i)
			err = tx.Put(key, val)
		}
		return err
	})
	if err != nil {
		t.Error(err)
	}

	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestBolt(t *testing.T) {
	start := time.Now()

	db, err := bolt.Open("E:\\golangProject\\demo1\\test7\\log\\db.txt", 0644, nil) //打开或创建数据库
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		b := tx.Bucket([]byte("test"))
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("foo%v", i)
			val := fmt.Sprintf("bar%v", i)
			err = b.Put([]byte(key), []byte(val))
		}
		return err
	})

	if err != nil { //创建桶失败
		panic(err)
	}

	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestNuts(t *testing.T) {
	start := time.Now()
	opt := nutsdb.DefaultOptions
	opt.EntryIdxMode = nutsdb.HintKeyValAndRAMIdxMode
	//opt.RWMode = nutsdb.MMap
	db, err := nutsdb.Open(opt, nutsdb.WithDir("E:\\golangProject\\demo1\\test7\\log"))
	if err != nil {
		t.Error(err)
	}
	err = db.Update(func(tx *nutsdb.Tx) error {
		for i := 0; i < 1000000; i++ {
			//num := rand.Intn(1000000)
			key := []byte(fmt.Sprintf("foo%v", i))
			val := []byte(fmt.Sprintf("bar%v", i))
			err = tx.Put("nuts", key, val, 0)
		}
		return err
	})
	if err != nil {
		t.Error(err)
	}
	err = db.Close()
	if err != nil {
		t.Error(err)
	}
	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestRose(t *testing.T) {
	start := time.Now()
	db, err := rosedb.Open(rosedb.DefaultOptions("E:\\golangProject\\demo1\\test7\\log"))
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 1000000; i++ {
		num := rand.Intn(1000000)
		key := []byte(fmt.Sprintf("foo%v", num))
		val := []byte(fmt.Sprintf("bar%v", num))
		err = db.Set(key, val)
	}
	if err != nil {
		t.Error(err)
	}
	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestTime2(t *testing.T) {
	start := time.Now()
	opt := DefaultOption()

	db, err := Open(opt)

	if err != nil {
		t.Error(err)
	}
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100000; i++ {
			key := []byte(fmt.Sprintf("foo%v", i))
			val := []byte(fmt.Sprintf("bar%v", i))
			err = tx.Put(key, val)
		}
		return err
	})
	if err != nil {
		t.Error(err)
	}
	err = db.View(func(tx *TX) error {
		for i := 0; i < 100000; i++ {
			key := []byte(fmt.Sprintf("foo%v", i))
			var val [][]byte
			val, err = tx.Get(key)
			var str string
			err = DecodeValue(val[0], &str)
			if str != fmt.Sprintf("bar%v", i) {
				fmt.Printf("%#v %#v\n", string(key), str)
				//t.Error("not equal")
			}
		}
		return err
	})
	db.Close()
	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}

func TestTTL(t *testing.T) {
	programInfo()
	start := time.Now()
	opt := DefaultOption()

	db, err := Open(opt)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	err = db.Update(func(tx *TX) error {
		err = tx.PutTTL("del", "this", 2)
		time.Sleep(5 * time.Second)
		err = tx.Put("woo", "cao")
		time.Sleep(5 * time.Second)

		var val1 [][]byte
		val1, err = tx.Get("del")
		var str1 string
		err = DecodeValue(val1[0], &str1)
		fmt.Println(str1)

		var val2 [][]byte
		val2, err = tx.Get("woo")
		var str2 string
		err = DecodeValue(val2[0], &str2)
		fmt.Println(str2)

		return err
	})
	elapse := time.Since(start)
	fmt.Println(elapse)
	programInfo()
}
