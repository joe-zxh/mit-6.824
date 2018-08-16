package raftkv

import "testing"
import "strconv"
import "time"
import "fmt"
import "math/rand"
import "log"
import "strings"
import "sync/atomic"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second

// 检查对应key的value是否正确。
func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// a client runs the function f and then signals when it is done 通过一个clerk来运行一个函数，并在运行完后，返回ok
func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.makeClient(cfg.All())
	fn(me, ck, t)
	ok = true
	cfg.deleteClient(ck)
}

// spawn(产生大量) ncli clients and wait until they are all done 产生大量的clerk来运行一个函数，只要有一个clerk运行失败，那么返回false. ncli是client的个数
func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cfg, cli, ca[cli], fn)
	}
	// log.Printf("spawn_clients_and_wait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		// log.Printf("spawn_clients_and_wait: client %d is done\n", cli)
		if ok == false {
			t.Fatalf("failure")
		}
	}
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

// check that for a specific client all known appends are present in a value, and in order
// 检查一个client添加的value是否正确。(存在、pattern唯一、且次序正确，才算正确)
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(v, wanted)
		if off < 0 { // 找不到对应的pattern
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		off1 := strings.LastIndex(v, wanted) //检查在value中有且仅有一个这样的pattern(字符串里面的"j"保证了唯一性)
		if off1 != off { //出现多个pattern
			fmt.Printf("off1 %v off %v\n", off1, off)
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		if off <= lastoff { // 次序错了
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		lastoff = off
	}
}

// check that all known appends are present in a value, and are in order for each concurrent client.
// 和checkClntAppends类似，只不过，这里 对每一个client都检查 append的正确性。
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

// repartition the servers periodically
func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 { // 原子操作，其他goroutine不会对done进行读或写的操作。
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

// Basic test is as follows: one or more clients submitting Append/Get operations to set of servers for some period of time. 一个或多个客户端向一些服务器提交append/get操作。
// After the period is over, test checks that all appended values are present and in order  for a particular key. 然后，对特定的key，检查它的value的正确性。
// If unreliable is set, RPCs may fail.  If crash is set, the servers crash after the period is over and restart. 参数unreliabled会让RPC断掉。crash会让服务器断掉。
// If partitions is set, the test repartitions the network concurrently with the clients and servers. partition会在一段时间后让网络重新分组。
// If maxraftstate is a positive number, the size of the state for Raft (i.e., log size) shouldn't exceed 2*maxraftstate. 2*maxraftstate是最大的log的长度。
func GenericTest(t *testing.T, tag string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {
	const nservers = 5
	cfg := make_config(t, tag, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	done_partitioner := int32(0) // 用来标志是否 停止重新分组。
	done_clients := int32(0) // 用来标志client是否进行操作(0表示继续操作，1表示停止操作。5s后设为1)
	ch_partitioner := make(chan bool) // 这个用来标志partition分组是否完成了
	clnts := make([]chan int, nclients) // 这个用来记录client在这段时间内一共做了多少次append操作的。如果<10个会有一个警告的提示
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli) // key就是对应的服务器的index值。
			myck.Put(key, last)
			for atomic.LoadInt32(&done_clients) == 0 {
				if (rand.Int() % 1000) < 500 { //一半的时间进行append操作
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					myck.Append(key, nv)
					last = NextValue(last, nv)
					j++
				} else { //一半的时间进行get操作，检查是否append成功
					// log.Printf("%d: client new get %v\n", cli, key)
					v := myck.Get(key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
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
			// log.Printf("wait for partitioner\n")
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
			// log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i)
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			// log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				cfg.StartServer(i)
			}
			cfg.ConnectAll()
		}

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ { //检查client的操作结果是否正确
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			if j < 10 {
				log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			}
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
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

func TestJoe(t *testing.T) {
	fmt.Println(randstring(5))
	fmt.Println(randstring(5))
	fmt.Println(randstring(5))
}

func TestBasic(t *testing.T) {
	fmt.Printf("Test: One client ...\n")
	GenericTest(t, "basic", 1, false, false, false, -1)
}

func TestConcurrent(t *testing.T) {
	fmt.Printf("Test: concurrent clients ...\n")
	GenericTest(t, "concur", 5, false, false, false, -1)
}

func TestUnreliable(t *testing.T) {
	fmt.Printf("Test: unreliable ...\n")
	GenericTest(t, "unreliable", 5, true, false, false, -1)
}

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

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
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

func TestManyPartitionsOneClient(t *testing.T) {
	fmt.Printf("Test: many partitions ...\n")
	GenericTest(t, "manypartitions", 1, false, false, true, -1)
}

func TestManyPartitionsManyClients(t *testing.T) {
	fmt.Printf("Test: many partitions, many clients ...\n")
	GenericTest(t, "manypartitionsclnts", 5, false, false, true, -1)
}

func TestPersistOneClient(t *testing.T) {
	fmt.Printf("Test: persistence with one client ...\n")
	GenericTest(t, "persistone", 1, false, true, false, -1)
}

func TestPersistConcurrent(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients ...\n")
	GenericTest(t, "persistconcur", 5, false, true, false, -1)
}

func TestPersistConcurrentUnreliable(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients, unreliable ...\n")
	GenericTest(t, "persistconcurunreliable", 5, true, true, false, -1)
}

func TestPersistPartition(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients and repartitioning servers...\n")
	GenericTest(t, "persistpart", 5, false, true, true, -1)
}

func TestPersistPartitionUnreliable(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients and repartitioning servers, unreliable...\n")
	GenericTest(t, "persistpartunreliable", 5, true, true, true, -1)
}

//
// if one server falls behind, then rejoins, does it
// recover by using the InstallSnapshot RPC?
// also checks that majority discards committed log entries
// even if minority doesn't respond.
//
func TestSnapshotRPC(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	cfg := make_config(t, "snapshotrpc", nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: InstallSnapshot RPC ...\n")

	ck.Put("a", "A")
	check(t, ck, "a", "A")

	// a bunch of puts into the majority partition.
	cfg.partition([]int{0, 1}, []int{2})
	{
		ck1 := cfg.makeClient([]int{0, 1})
		for i := 0; i < 50; i++ {
			ck1.Put(strconv.Itoa(i), strconv.Itoa(i))
		}
		time.Sleep(electionTimeout)
		ck1.Put("b", "B")
	}

	// check that the majority partition has thrown away
	// most of its log entries.
	if cfg.LogSize() > 2*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
	}

	// now make group that requires participation of
	// lagging server, so that it has to catch up.
	cfg.partition([]int{0, 2}, []int{1})
	{
		ck1 := cfg.makeClient([]int{0, 2})
		ck1.Put("c", "C")
		ck1.Put("d", "D")
		check(t, ck1, "a", "A")
		check(t, ck1, "b", "B")
		check(t, ck1, "1", "1")
		check(t, ck1, "49", "49")
	}

	// now everybody
	cfg.partition([]int{0, 1, 2}, []int{})

	ck.Put("e", "E")
	check(t, ck, "c", "C")
	check(t, ck, "e", "E")
	check(t, ck, "1", "1")

	fmt.Printf("  ... Passed\n")
}

func TestSnapshotRecover(t *testing.T) {
	fmt.Printf("Test: persistence with one client and snapshots ...\n")
	GenericTest(t, "snapshot", 1, false, true, false, 1000)
}

func TestSnapshotRecoverManyClients(t *testing.T) {
	fmt.Printf("Test: persistence with several clients and snapshots ...\n")
	GenericTest(t, "snapshotunreliable", 20, false, true, false, 1000)
}

func TestSnapshotUnreliable(t *testing.T) {
	fmt.Printf("Test: persistence with several clients, snapshots, unreliable ...\n")
	GenericTest(t, "snapshotunreliable", 5, true, false, false, 1000)
}

func TestSnapshotUnreliableRecover(t *testing.T) {
	fmt.Printf("Test: persistence with several clients, failures, and snapshots, unreliable ...\n")
	GenericTest(t, "snapshotunreliablecrash", 5, true, true, false, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartition(t *testing.T) {
	fmt.Printf("Test: persistence with several clients, failures, and snapshots, unreliable and partitions ...\n")
	GenericTest(t, "snapshotunreliableconcurpartitions", 5, true, true, true, 1000)
}
