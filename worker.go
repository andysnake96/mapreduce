package mapReduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"strings"
)

const PORTBASE = 1234

const MAP = "MAP"
const REDUCE = "REDUCE"

func rpcInit(off_port int, kindWork string) {
	//INIT AN RPC SERVER UNDER PORT BASE + off_port
	//differentiate rpc server work by kind work string witch has to be MAP Or REDUCE costant
	//TODO INFERENCE KINDWORK BY WAITGROUP REFERENCE (passed nil on reduce ... =
	/*
		 TODO sync after thread==RPC SERVER termination...
			MAP=>terminate after first request served
			REDUCE => multi req (by paper requested version :( ) => unknown when to exit by rpc calls
					=> ?

		 TODO REDUCE SYNC -> in memory
				-> channel done, barrier. = waitgroup + master multi add per reducer...
	*/

	//Create an instance of structs which implements map and reduce interfaces
	map_ := new(_map)
	reduce_ := new(_reduce)

	//REGISTER MAP AND REDUCE METHODS
	// Register a new rpc server and the struct we created above.
	// Only structs which implement Arith interface are allowed to register themselves
	server := rpc.NewServer()

	err := server.RegisterName("Map", map_)
	if err != nil {
		log.Fatal("Format of service Map is not correct: ", err)
	}
	err = server.RegisterName("Reduce", reduce_)
	if err != nil {
		log.Fatal("Format of service Reduce is not correct: ", err)
	}
	port := PORTBASE + off_port

	// Listen for incoming tcp packets on port by specified offset of port base.
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	if e != nil {
		log.Fatal("Listen error on port ", port, e)
	}
	defer l.Close()

	// Link rpc server to the socket, and allow rpc server to accept
	// rpc requests coming from that socket.

	//<MAP - REDUCE DIFFERENT LOGIC
	//reduce case => block until ?? exit
	if strings.Compare(kindWork, MAP) != 0 {
		server.Accept(l) //blocked until listener error //TODO EXPAND ACCEPT
	}

	//map case => block only for first call, then exit
	conn, err := l.Accept()
	defer conn.Close()
	if err != nil {
		log.Print("rpc.Serve: accept:", err.Error())
		return
	}
	server.ServeConn(conn) //block until connected client hangs up
	fmt.Println("SERVED RPC REQ")

}
