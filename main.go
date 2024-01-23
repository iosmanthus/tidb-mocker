package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"

	"github.com/tikv/client-go/v2/txnkv"
)

var (
	// PDAddrs is the address list of PD.
	pdAddrs = flag.String("pd", "127.0.0.1:2379", "pd addrs")
	anno    = flag.String("anno", "", "annotation")
)

func importMeta(wg *sync.WaitGroup, client *txnkv.Client, idx int) {
	defer wg.Done()
	for i := 0; i < 100; i++ {
		txn, err := client.Begin()
		if err != nil {
			panic(err)
		}
		for j := 0; j < 10000; j++ {
			err = txn.Set([]byte(fmt.Sprintf("m%s%d%d%d", *anno, i, j, idx)), []byte("value"))
			if err != nil {
				panic(err)
			}
		}
		err = txn.Commit(context.Background())
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	flag.Parse()
	addr := strings.Split(*pdAddrs, ",")
	client, err := txnkv.NewClient(addr)
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go importMeta(wg, client, i)
	}
	wg.Wait()
}
