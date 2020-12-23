package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

func main() {
	srcAddr := flag.String("src", "", "Source host:port")
	srcPw := flag.String("srcpw", "", "Source password")
	srcDb := flag.Int("srcdb", 0, "Source db number")
	destAddr := flag.String("dest", "", "Destination host:port")
	destPw := flag.String("destpw", "", "Destination password")
	destDb := flag.Int("destdb", 0, "Destination db number")
	concur := flag.Int("c", 50, "Concurrency")
	statusNum := flag.Int("s", 1000, "Print status every x keys (0 = do not print status)")
	flag.Parse()

	if *srcAddr == "" {
		fmt.Printf("-src flag is required\n")
		os.Exit(1)
	}
	if *destAddr == "" {
		fmt.Printf("-dest flag is required\n")
		os.Exit(1)
	}

	fmt.Printf("rediscopy src=%s dest=%s concur=%d\n", *srcAddr, *destAddr, *concur)
	fmt.Printf("WARNING: this will delete ALL keys from dest: %s\n", *destAddr)
	fmt.Printf("Type YES to continue: ")
	stdin := bufio.NewReader(os.Stdin)
	val, _ := stdin.ReadString('\n')
	val = strings.TrimSpace(val)
	if val != "YES" {
		fmt.Println("Aborting")
		os.Exit(0)
	}

	srcClient := redis.NewClient(&redis.Options{
		Addr:     *srcAddr,
		Password: *srcPw,
		DB:       *srcDb,
	})

	destClient := redis.NewClient(&redis.Options{
		Addr:     *destAddr,
		Password: *destPw,
		DB:       *destDb,
	})

	fmt.Printf("Running FLUSHALL on dest to delete all keys\n")
	_, err := destClient.FlushAll(ctx).Result()
	if err != nil {
		fmt.Printf("ERROR running FLUSHALL: %v\n", err)
		fmt.Printf("Aborting copy\n")
		os.Exit(1)
	}

	fmt.Printf("Loading keys from src\n")
	keys, err := srcClient.Keys(ctx, "*").Result()
	if err != nil {
		fmt.Printf("ERROR loading keys: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Loaded %d keys - beginning copy\n", len(keys))

	wg := &sync.WaitGroup{}
	keyCh := make(chan string)

	for i := 0; i < *concur; i++ {
		wg.Add(1)
		go worker(wg, keyCh, srcClient, destClient)
	}
	total := 0
	for i, k := range keys {
		keyCh <- k
		if i > 0 && *statusNum > 0 && i%*statusNum == 0 {
			fmt.Printf("Copied %d keys - last key: %s\n", i, k)
		}
		total++
	}
	close(keyCh)
	wg.Wait()

	fmt.Printf("Copy complete. Total keys: %d\n", total)
}

func worker(wg *sync.WaitGroup, keyCh chan string, srcClient *redis.Client, destClient *redis.Client) {
	defer wg.Done()
	for k := range keyCh {
		v, err := srcClient.Dump(ctx, k).Result()
		if err == nil {
			ttl := time.Duration(0)
			pttl, err := srcClient.PTTL(ctx, k).Result()
			if err == nil {
				ttl = pttl
			}
			err = destClient.Restore(ctx, k, ttl, v).Err()
			if err != nil {
				fmt.Printf("ERROR restoring key: %s err: %v\n", k, err)
			}
		} else {
			if err == redis.Nil {
				fmt.Printf("Skipping missing key: %v\n", k)
			} else {
				fmt.Printf("ERROR dumping key: %s err: %v\n", k, err)
			}
		}
	}
}
