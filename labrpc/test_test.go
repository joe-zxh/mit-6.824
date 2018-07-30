package labrpc

import "testing"
import "strconv"
import "sync"
import "runtime"
import "time"
import "fmt"

type JunkArgs struct {
	X int
}
type JunkReply struct {
	X string
}

type JunkServer struct {
	mu   sync.Mutex
	log1 []string
	log2 []int
}

func (js *JunkServer) Handler1(args string, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log1 = append(js.log1, args)
	*reply, _ = strconv.Atoi(args)
}

func (js *JunkServer) Handler2(args int, reply *string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log2 = append(js.log2, args)
	*reply = "handler2-" + strconv.Itoa(args)
}

func (js *JunkServer) Handler3(args int, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	time.Sleep(20 * time.Second) //睡个20s，用来测试 客户端的请求在服务端阻塞的情况(TestKilled)
	*reply = -args
}

// args is a pointer
func (js *JunkServer) Handler4(args *JunkArgs, reply *JunkReply) {
	reply.X = "pointer"
}

// args is a not pointer
func (js *JunkServer) Handler5(args JunkArgs, reply *JunkReply) {
	reply.X = "no pointer"
}

//测试基本的客户端发送请求，然后服务器响应请求
func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	{
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "handler2-111" {
			t.Fatalf("wrong reply from Handler2")
		}
	}

	{
		reply := 0
		e.Call("JunkServer.Handler1", "9099", &reply)
		if reply != 9099 {
			t.Fatalf("wrong reply from Handler1")
		}
	}
}

// 应该是为了测试是否能够向服务器传递指针类型的参数
func TestTypes(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	{
		var args JunkArgs
		var reply JunkReply
		// args must match type (pointer or not) of handler.
		e.Call("JunkServer.Handler4", &args, &reply)
		if reply.X != "pointer" {
			t.Fatalf("wrong reply from Handler4")
		}
	}

	{
		var args JunkArgs
		var reply JunkReply
		// args must match type (pointer or not) of handler.
		e.Call("JunkServer.Handler5", args, &reply)
		if reply.X != "no pointer" {
			t.Fatalf("wrong reply from Handler5")
		}
	}
}

//
// does net.Enable(endname, false) really disconnect a client?
// 测试enable的功能，对于一个客户端 要：1.连接了服务器 2.调成可用enabled 时，才能使用
//
func TestDisconnect(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")

	{
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "" {
			t.Fatalf("unexpected reply from Handler2")
		}
	}

	rn.Enable("end1-99", true)

	{
		reply := 0
		e.Call("JunkServer.Handler1", "9099", &reply)
		if reply != 9099 {
			t.Fatalf("wrong reply from Handler1")
		}
	}
}

//
// test net.GetCount() 用来获取某个服务器累计的请求数的函数
//
func TestCounts(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(99, rs)

	rn.Connect("end1-99", 99)
	rn.Enable("end1-99", true)

	for i := 0; i < 17; i++ {
		reply := ""
		e.Call("JunkServer.Handler2", i, &reply)
		wanted := "handler2-" + strconv.Itoa(i)
		if reply != wanted {
			t.Fatalf("wrong reply %v from Handler1, expecting %v", reply, wanted)
		}
	}

	n := rn.GetCount(99)
	if n != 17 {
		t.Fatalf("wrong GetCount() %v, expected 17\n", n)
	}
}

//
// test RPCs from concurrent ClientEnds
// 测试多个客户端并发操作时，服务器的返回值是否正确、rpc的发送以及接收的总数量是否正确
//
func TestConcurrentMany(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	ch := make(chan int)

	nclients := 20 //一共20个客户端
	nrpcs := 10 //每个客户端发送10条rpc 给同一个服务器
	for ii := 0; ii < nclients; ii++ {
		go func(i int) {
			n := 0
			defer func() { ch <- n }() //最后把n(10，如果不出错的话)放进通道里面

			e := rn.MakeEnd(i)
			rn.Connect(i, 1000)
			rn.Enable(i, true)

			for j := 0; j < nrpcs; j++ {
				arg := i*100 + j
				reply := ""
				e.Call("JunkServer.Handler2", arg, &reply)
				wanted := "handler2-" + strconv.Itoa(arg)
				if reply != wanted {
					t.Fatalf("wrong reply %v from Handler1, expecting %v", reply, wanted)
				}
				n += 1
			}
		}(ii)
	}

	total := 0
	for ii := 0; ii < nclients; ii++ { //测试 每个客户端是否都发送了nrpcs条rpc
		x := <-ch
		total += x
	}

	if total != nclients*nrpcs {
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, nclients*nrpcs)
	}

	n := rn.GetCount(1000) //测试服务器接收到的rpc数量
	if n != total {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, total)
	}
}

//
// test unreliable
//
func TestUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	rn.Reliable(false)

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	ch := make(chan int)

	nclients := 300
	for ii := 0; ii < nclients; ii++ {
		go func(i int) {
			n := 0
			defer func() { ch <- n }()

			e := rn.MakeEnd(i)
			rn.Connect(i, 1000)
			rn.Enable(i, true)

			arg := i * 100
			reply := ""
			ok := e.Call("JunkServer.Handler2", arg, &reply)

			// unreliable时，如果服务端有回复，测试服务端回复的正确性。
			if ok {
				wanted := "handler2-" + strconv.Itoa(arg)
				if reply != wanted {
					t.Fatalf("wrong reply %v from Handler1, expecting %v", reply, wanted)
				}
				n += 1
			}
		}(ii)
	}

	total := 0
	for ii := 0; ii < nclients; ii++ {
		x := <-ch
		total += x
	}

	// unreliable 时，客户端的请求会有一定概率挂掉，所以最后发送的请求 != 收到的回复，但收到的回复为0的概率很小。
	if total == nclients || total == 0 {
		t.Fatalf("all RPCs succeeded despite unreliable")
	}
}

//
// test concurrent RPCs from a single ClientEnd
//
func TestConcurrentOne(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	e := rn.MakeEnd("c")
	rn.Connect("c", 1000)
	rn.Enable("c", true)

	ch := make(chan int)

	nrpcs := 20
	for ii := 0; ii < nrpcs; ii++ { //对每一个rpc都开一个goroutine来执行相应的操作
		go func(i int) {
			n := 0
			defer func() { ch <- n }()

			arg := 100 + i
			reply := ""
			e.Call("JunkServer.Handler2", arg, &reply)
			wanted := "handler2-" + strconv.Itoa(arg)
			if reply != wanted { //测试服务端返回的结果的正确性
				t.Fatalf("wrong reply %v from Handler2, expecting %v", reply, wanted)
			}
			n += 1
		}(ii)
	}

	total := 0
	for ii := 0; ii < nrpcs; ii++ {
		x := <-ch
		total += x
	}

	if total != nrpcs { //测试完成的rpc的数量(接收到服务端的结果的rpc)
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, nrpcs)
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	if len(js.log2) != nrpcs { //测试发送到服务器的rpc的数量(服务器还没返回结果时)
		t.Fatalf("wrong number of RPCs delivered")
	}

	n := rn.GetCount(1000)
	if n != total { //测试服务器接收到的rpc数量是否和客户端发送的数量相同
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, total)
	}
}

//
// regression: an RPC that's delayed during Enabled=false
// should not delay subsequent RPCs (e.g. after Enabled=true).
//
func TestRegression1(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	e := rn.MakeEnd("c")
	rn.Connect("c", 1000)

	// start some RPCs while the ClientEnd is disabled.
	// they'll be delayed.
	rn.Enable("c", false)
	ch := make(chan bool)
	nrpcs := 20
	for ii := 0; ii < nrpcs; ii++ {
		go func(i int) {
			ok := false
			defer func() { ch <- ok }()

			arg := 100 + i
			reply := ""
			// this call ought to return false. 在ProcessReq里面如果客户端的enabled=false就会返回false
			e.Call("JunkServer.Handler2", arg, &reply)
			ok = true
		}(ii)
	}

	time.Sleep(100 * time.Millisecond)

	// now enable the ClientEnd and check that an RPC completes quickly.
	t0 := time.Now()
	rn.Enable("c", true)
	{
		arg := 99
		reply := ""
		e.Call("JunkServer.Handler2", arg, &reply)
		wanted := "handler2-" + strconv.Itoa(arg)
		if reply != wanted {
			t.Fatalf("wrong reply %v from Handler2, expecting %v", reply, wanted)
		}
	}
	dur := time.Since(t0).Seconds()

	if dur > 0.03 {
		t.Fatalf("RPC took too long (%v) after Enable", dur)
	}

	for ii := 0; ii < nrpcs; ii++ {
		<-ch
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	if len(js.log2) != 1 { //enable的时候只发送了一条请求，如果不等于1，说明unenabled时候发送的请求也被处理了
		t.Fatalf("wrong number (%v) of RPCs delivered, expected 1", len(js.log2))
	}

	n := rn.GetCount(1000)
	if n != 1 { //只有enable，服务器才会dispatch，只有dispatch，count才会+1
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, 1)
	}
}

//
// if an RPC is stuck in a server, and the server handle3用来模拟在server中被阻塞的现象(sleep了20s)
// is killed with DeleteServer(), does the RPC
// get un-stuck?
//
func TestKilled(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	doneCh := make(chan bool)
	go func() { //因为Handler3是sleep了20s之后，才开始返回。用来模拟请求在服务端被阻塞的情况
		reply := 0
		ok := e.Call("JunkServer.Handler3", 99, &reply)
		doneCh <- ok
	}()

	time.Sleep(1000 * time.Millisecond)

	select {
	case <-doneCh:
		t.Fatalf("Handler3 should not have returned yet")
	case <-time.After(100 * time.Millisecond):
	}

	rn.DeleteServer("server99")

	select {
	case x := <-doneCh:
		if x != false {
			t.Fatalf("Handler3 returned successfully despite DeleteServer()")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Handler3 should return after DeleteServer()")
	}
}

// 只是测一下100,000个rpc用的时间(1个客户端,1个服务器)
func TestBenchmark(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	t0 := time.Now()
	n := 100000
	for iters := 0; iters < n; iters++ {
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "handler2-111" {
			t.Fatalf("wrong reply from Handler2")
		}
	}
	fmt.Printf("%v for %v\n", time.Since(t0), n)
	// march 2016, rtm laptop, 22 microseconds per RPC
}
