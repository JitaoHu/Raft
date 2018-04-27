package raftkv

import "testing"
import "strconv"
import "time"
import "fmt"
import "math/rand"
import "log"
import "strings"
import "sync/atomic"
import "os"

const electionTimeout = 1 * time.Second

//用Get检查是否收到期望的value。
func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// 运行客户端
func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.makeClient(cfg.All()) //调用makeClient函数创建客户端
	fn(me, ck, t)
	ok = true
	cfg.deleteClient(ck)
}

////创建指定个数的客户端并发地发出操作请求
func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cfg, cli, ca[cli], fn)
	}
	log.Printf("spawn_clients_and_wait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		log.Printf("spawn_clients_and_wait: client %d is done\n", cli)
		if ok == false {
			t.Fatalf("failure")
		}
	}
}

// 如果旧的value是以前的，那么预测之后的value。
func NextValue(prev string, val string) string {
	return prev + val
}

// 检查客户端的append值。
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		off1 := strings.LastIndex(v, wanted)
		if off1 != off {
			fmt.Printf("off1 %v off %v\n", off1, off)
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		if off <= lastoff {
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		lastoff = off
	}
}

// 检查并发客户端的append值。
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element %v in Append result", wanted)
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element %v in Append result", wanted)
			}
			lastoff = off
		}
	}
}

// 使分区。
func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {
		a := make([]int, cfg.n)
		for i := 0; i < cfg.n; i++ {
			a[i] = (rand.Int() % 2)
		}
		pa := make([][]int, 2)
		for i := 0; i < 2; i++ {
			pa[i] = make([]int, 0)
			for j := 0; j < cfg.n; j++ {
				if a[j] == i {
					pa[i] = append(pa[i], j)
				}
			}
		}
		cfg.partition(pa[0], pa[1])
		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
	}
}

/*
nclients表示并发客户端个数，unreliable代表RPC调用的可靠性，
crash表示是否发生节点down了的情况，partitions表示是否发生网络分区，
maxraftstate代表log的最大长度
*/
func GenericTest(t *testing.T, tag string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {
	const nservers = 5
	cfg := make_config(t, tag, nservers, unreliable, maxraftstate) //初始化服务器
	defer cfg.cleanup()
	logfile, err := os.OpenFile("/raft.log", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("%s\r\n", err.Error())
		os.Exit(-1)
	}
	defer logfile.Close()
	logger := log.New(logfile, "\r\n", log.Ldate|log.Ltime|log.Llongfile)
	ck := cfg.makeClient(cfg.All()) //初始化客户端
	//设置done_clients和done_partitioner变量的值为0，用于控制后面goroutine中的循环
	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 1; i++ {
		log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		//fn函数为匿名函数，其主要功能是先调用Put函数执行put操作，
		//然后不断地调用Append函数来执行append操作，
		//在此过程中会调用Get函数来执行get操作检查上一次操作的值是否正确被写入

		//goroutine执行spawn_clients_and_wait函数，创建指定个数的客户端并发地发出操作请求
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			myck.Put(key, last)
			for atomic.LoadInt32(&done_clients) == 0 {
				if (rand.Int() % 1000) < 500 { //保证随机性
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					logger.Printf("%d: client new append %v\n", cli, nv)
					myck.Append(key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					logger.Printf("%d: client new get %v\n", cli, key)
					v := myck.Get(key)
					//logger.Printf("value is %v\n", v)
					if v != last {
						logger.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			}
		})

		if partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit

		if partitions {
			logger.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cfg.ConnectAll()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		if crash {
			log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i)
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				cfg.StartServer(i)
			}
			cfg.ConnectAll()
		}

		logger.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			logger.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			if j < 10 {
				logger.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			}
			key := strconv.Itoa(i)
			logger.Printf("Check %v for client %d\n", j, i)
			v := ck.Get(key)
			checkClntAppends(t, i, v, j)
		}

		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint
			if cfg.LogSize() > 2*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

//1个客户端，其他都为false。
func TestBasic(t *testing.T) {
	fmt.Printf("Test: One client ...\n")
	GenericTest(t, "basic", 1, false, false, false, -1)
}

//5个客户端，并发发送rpc，其他为false。
func TestConcurrent(t *testing.T) {
	fmt.Printf("Test: concurrent clients ...\n")
	GenericTest(t, "concur", 5, false, false, false, -1)
}

//5个客户端，rpc不可靠，其他为false。
func TestUnreliable(t *testing.T) {
	fmt.Printf("Test: unreliable ...\n")
	GenericTest(t, "unreliable", 5, true, false, false, -1)
}

//3个服务器，5个客户端，并发的发送同一个key值，检查日志是否正常。
func TestUnreliableOneKey(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, "onekey", nservers, true, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Concurrent Append to same key, unreliable ...\n")

	ck.Put("k", "")

	const nclient = 5
	const upto = 10
	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
		n := 0
		for n < upto {
			myck.Append("k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
			n++
		}
	})

	var counts []int
	for i := 0; i < nclient; i++ {
		counts = append(counts, upto)
	}

	vx := ck.Get("k")
	checkConcurrentAppends(t, vx, counts)

	fmt.Printf("  ... Passed\n")
}

// 一个分区。
func TestOnePartition(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, "partition", nservers, false, -1)
	defer cfg.cleanup()
	ck := cfg.makeClient(cfg.All())

	ck.Put("1", "13")

	fmt.Printf("Test: Progress in majority ...\n")

	p1, p2 := cfg.make_partition()
	cfg.partition(p1, p2)

	ckp1 := cfg.makeClient(p1)  // connect ckp1 to p1
	ckp2a := cfg.makeClient(p2) // connect ckp2a to p2
	ckp2b := cfg.makeClient(p2) // connect ckp2b to p2

	ckp1.Put("1", "14")
	check(t, ckp1, "1", "14")

	fmt.Printf("  ... Passed\n")

	done0 := make(chan bool)
	done1 := make(chan bool)

	fmt.Printf("Test: No progress in minority ...\n")
	go func() {
		ckp2a.Put("1", "15")
		done0 <- true
	}()
	go func() {
		ckp2b.Get("1") // different clerk in p2
		done1 <- true
	}()

	select {
	case <-done0:
		t.Fatalf("Put in minority completed")
	case <-done1:
		t.Fatalf("Get in minority completed")
	case <-time.After(time.Second):
	}

	check(t, ckp1, "1", "14")
	ckp1.Put("1", "16")
	check(t, ckp1, "1", "16")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after heal ...\n")

	cfg.ConnectAll()
	cfg.ConnectClient(ckp2a, cfg.All())
	cfg.ConnectClient(ckp2b, cfg.All())

	time.Sleep(electionTimeout)

	select {
	case <-done0:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Put did not complete")
	}

	select {
	case <-done1:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Get did not complete")
	default:
	}

	check(t, ck, "1", "15")

	fmt.Printf("  ... Passed\n")
}

//多个分区，1个客户端，其他为false。
func TestManyPartitionsOneClient(t *testing.T) {
	fmt.Printf("Test: many partitions ...\n")
	GenericTest(t, "manypartitions", 1, false, false, true, -1)
}

//1个客户端，有服务器宕机，其他为false。
func TestManyPartitionsManyClients(t *testing.T) {
	fmt.Printf("Test: many partitions, many clients ...\n")
	GenericTest(t, "manypartitionsclnts", 5, false, false, true, -1)
}

//1个客户端，有服务器宕机，其他为false。
func TestPersistOneClient(t *testing.T) {
	fmt.Printf("Test: persistence with one client ...\n")
	GenericTest(t, "persistone", 1, false, true, false, -1)
}

//5个客户端，并发发送rpc，有服务器宕机，其他都为false。
func TestPersistConcurrent(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients ...\n")
	GenericTest(t, "persistconcur", 5, false, true, false, -1)
}

//5个客户端，并发发送rpc，有服务器宕机且rpc不可靠，其他为false。
func TestPersistConcurrentUnreliable(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients, unreliable ...\n")
	GenericTest(t, "persistconcurunreliable", 5, true, true, false, -1)
}

//5个客户端，并发发送rpc，有服务器宕机且网络分区，其他为false。
func TestPersistPartition(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients and repartitioning servers...\n")
	GenericTest(t, "persistpart", 5, false, true, true, -1)
}

//5个客户端，并发发送rpc，有服务器宕机，rpc不可靠且网络分区。s
func TestPersistPartitionUnreliable(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients and repartitioning servers, unreliable...\n")
	GenericTest(t, "persistpartunreliable", 5, true, true, true, -1)
}
