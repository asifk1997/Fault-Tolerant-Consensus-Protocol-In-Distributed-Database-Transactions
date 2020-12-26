package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/perlin-network/noise"
	"github.com/perlin-network/noise/kademlia"
)
var mutex = &sync.Mutex{}
// SafeCounter is safe to use concurrently.
type SafeCounter struct {
	mu sync.Mutex
	v  map[string]int
}

// Inc increments the counter for the given key.
func (c *SafeCounter) Inc(key string) {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.v[key]++
	c.mu.Unlock()
}

// Value returns the current value of the counter for the given key.
func (c *SafeCounter) Value(key string) int {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mu.Unlock()
	return c.v[key]
}
var c = SafeCounter{v: make(map[string]int)}
//////////////particpant node variables start/////////////////////////
var lastTimeOutRecieved time.Time = time.Now()
var myCurrentDispatcher string
var dispatcherStatus string ="alive"
var votingCount int = 0
var maxNoForThisVote int = 0
var myNoForThisVote int = 0
var voterArray []string
var myAddress string = ":9003"
const t = 50
var listOfAddresses []string
var listOfParticipantAddress []string

type transaction struct {
	transactionString string
	commitStatus string
	generatedBy string
	generatedTime string
}

var transactionArray string = ""
var sent bool = false
var sent2 bool = false
///////////////////////////////////////

/////////////dispatcher node variables start//////////////////////////
var copiedBy = map[string][]string{}
//////////////////////////////////////
var currentRole string = "validator"

func setCurrentRole(newRole string) {
	currentRole = newRole
}

func getCurrentRole() string {
	return currentRole
}
func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
func votingBegins(){
	voterArray=nil
	maxNoForThisVote=0
	sent = false
}
func runContinuously() {
	for {
		n:=t*3
		if getCurrentRole()=="dispatcher"{
			n=20 *t
		}else if getCurrentRole()=="validator"{
			n=40 *t
		} else if getCurrentRole()=="IDLE"{
			n=100 *t
		} else {
			n=15 *t
		}
		time.Sleep(time.Duration(n)* time.Millisecond)
		chat(globalNode,globalOverlay,"t")
		if getCurrentRole()=="dispatcher"{
			sendTimeOutsToAllNodes()
			chatWithAllParticipants(globalNode,globalOverlay,"heartbeat")
		}else if getCurrentRole()=="validator"{
			if time.Now().After(lastTimeOutRecieved){
				log.Println("didnt recieve timeout")
				x :=rand.Intn(100)
				//x=90
				if x > 85 {
					votingBegins()
					setCurrentRole("candidate")
					//b := []string{myAddress}
					log.Println("generated number ",x)
					votingCount++
					chat(globalNode,globalOverlay,"iamcandidateforround"+strconv.Itoa(votingCount)+"number"+strconv.Itoa(x))
				}
			}
		} else if getCurrentRole()=="candidate"{
			log.Println("i am candidate doing nothing")
			setCurrentRole("validator")
		}else if getCurrentRole()=="IDLE"{
			log.Println("was waiting for someone to become dipatcher for too long")
			setCurrentRole("validator")
			maxNoForThisVote=0
		}
	}
}
func sendTimeOutsToAllNodes(){
	//
	log.Println("sending timeouts to all the nodes")
	chatWithAllValidators(globalNode,globalOverlay,"heartbeat"+strconv.Itoa(votingCount))
}
type chatMessage struct {
	contents string
}

func (m chatMessage) Marshal() []byte {
	return []byte(m.contents)
}

func unmarshalChatMessage(buf []byte) (chatMessage, error) {
	return chatMessage{contents: strings.ToValidUTF8(string(buf), "")}, nil
}

// check panics if err is not nil.
func check(err error) {
	if err != nil {
		panic(err)
	}
}

// printedLength is the total prefix length of a public key associated to a chat users ID.
const printedLength = 8
////////////////////global vars/////////////////////
var globalNode *noise.Node
var globalOverlay *kademlia.Protocol
///////////////////////////////////////////////////
// An example chat application on Noise.
func main() {
	// Parse flags/options.
	//pflag.Parse()
	listOfAddresses = []string{":9001",":9002",":9003"}
	listOfParticipantAddress = []string{":9006",":9007"}
	var temp []string
	for i :=0;i<len(listOfAddresses);i++{
		if listOfAddresses[i]!=myAddress{
			temp = append(temp, listOfAddresses[i])
		}
	}
	listOfAddresses = temp
	// Create a new configured node.
	address,_ := strconv.Atoi(myAddress[1:])
	node, err := noise.NewNode(
		//noise.WithNodeBindHost(*hostFlag),
		noise.WithNodeBindPort(uint16(address)),
		//noise.WithNodeAddress(*addressFlag),
	)
	check(err)

	// Release resources associated to node at the end of the program.
	defer node.Close()

	// Register the chatMessage Go type to the node with an associated unmarshal function.
	node.RegisterMessage(chatMessage{}, unmarshalChatMessage)

	// Register a message handler to the node.
	node.Handle(handle)

	// Instantiate Kademlia.
	events := kademlia.Events{
		OnPeerAdmitted: func(id noise.ID) {
			log.Printf("Learned about a new peer %s(%s).\n", id.Address, id.ID.String()[:printedLength])
		},
		OnPeerEvicted: func(id noise.ID) {
			log.Printf("Forgotten a peer %s(%s).\n", id.Address, id.ID.String()[:printedLength])
		},
	}

	overlay := kademlia.New(kademlia.WithProtocolEvents(events))

	// Bind Kademlia to the node.
	node.Bind(overlay.Protocol())

	// Have the node start listening for new peers.
	check(node.Listen())

	// Print out the nodes ID and a help message comprised of commands.
	help(node)

	// Ping nodes to initially bootstrap and discover peers from.
	globalNode = node
	globalOverlay = overlay
	bootstrap(node,listOfAddresses)

	// Attempt to discover peers if we are bootstrapped to any nodes.
	discover(overlay)

	// Accept chat message inputs and handle chat commands in a separate goroutine.
	go input(func(line string) {
		chat(node, overlay, line)
	})
	go runContinuously()
	// Wait until Ctrl+C or a termination call is done.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// Close stdin to kill the input goroutine.
	check(os.Stdin.Close())

	// Empty println.
	println()
}

// input handles inputs from stdin.
func input(callback func(string)) {
	r := bufio.NewReader(os.Stdin)

	for {
		buf, _, err := r.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			check(err)
		}

		line := string(buf)
		if len(line) == 0 {
			continue
		}

		callback(line)
	}
}

// handle handles and prints out valid chat messages from peers.
func handle(ctx noise.HandlerContext) error {
	if ctx.IsRequest() {
		return nil
	}

	obj, err := ctx.DecodeMessage()
	if err != nil {
		return nil
	}

	msg, ok := obj.(chatMessage)
	if !ok {
		return nil
	}

	if len(msg.contents) == 0 {
		return nil
	}
	if msg.contents != "t"{
		log.Printf("%s(%s)> %s\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.contents)
	}


	if strings.HasPrefix(msg.contents,"heartbeat"){
		lastTimeOutRecieved = time.Now().Add(time.Millisecond*(20*t))
	} else if strings.HasPrefix(msg.contents,"iamcandidateforround"){
		if getCurrentRole()=="dispatcher"{
			chatToParticularNode(globalNode,globalOverlay,"negativevote",ctx.ID().Address)
		}
		y :=msg.contents
		y = strings.Replace(y,"iamcandidateforround","",-1)
		res1 := strings.Split(y, "number")
		vote, _ := strconv.Atoi(res1[0])
		no,_ := strconv.Atoi(res1[1])

		b := []string{}
		b = append(b, ctx.ID().Address)
		//if vote > votingCount {
		if no > maxNoForThisVote{
			maxNoForThisVote = no
			log.Println("i gave positive vote to ", ctx.ID().Address)
			chatToParticularNode(globalNode,globalOverlay,"positivevote", ctx.ID().Address)
		} else {
			log.Println("i gave negative vote to ",b,"my current max ",maxNoForThisVote)
			chatToParticularNode(globalNode,globalOverlay,"negativevote", ctx.ID().Address)
			//chat(globalNode,globalOverlay,"iamcandidateforround"+res1[0]+"number"+strconv.Itoa(maxNoForThisVote))
		}
		//}
		log.Println("updated",y,res1,vote,no)
	} else if strings.HasPrefix(msg.contents,"congratulationsnewdispatcher"){
		currentRole="dispatcher"
		maxNoForThisVote=0
		chatWithAllValidators(globalNode,globalOverlay,"iamanewdispatcher")
	} else if msg.contents=="positivevote"{
		voterArray = append(voterArray, ctx.ID().Address)
		log.Println("got positive vote",voterArray)
		if len(voterArray) >= len(listOfAddresses)/2{
			if sent == false{
				sent=true
				log.Println("got positive vote if")
				//voterArray = append(voterArray, myAddress )
				v := rand.Intn(len(voterArray))
				newDispatcher := voterArray[v]
				b := []string{newDispatcher}
				if newDispatcher == myAddress{
					setCurrentRole("dispatcher")
					myCurrentDispatcher=""
					maxNoForThisVote=0
					chatWithAllValidators(globalNode,globalOverlay,"iamanewdispatcher")
				}else{
					log.Println("informing new coordinator",b)
					chatToParticularNode(globalNode,globalOverlay,"congratulationsnewdispatcher"+newDispatcher,newDispatcher)
				}
			}

		}
	} else if msg.contents=="iamanewdispatcher"{
		myCurrentDispatcher = ctx.ID().Address
		maxNoForThisVote=0
		voterArray = nil
		log.Println("myCurrentDispatcher is ",myCurrentDispatcher)
		lastTimeOutRecieved = time.Now().Add(time.Millisecond*(50*t))
		setCurrentRole("validator")
	} else if msg.contents=="negativevote"{
		log.Println("i got negative vote going in idle state")
		setCurrentRole("IDLE")
	}else if strings.HasPrefix(msg.contents,"transaction"){
		tr,ms := parseTransaction(msg.contents)
		if ms == "ready"{
			fmt.Println("got ready msg from ",ctx.ID().Address)
			mutex.Lock()
			if !transactionAlreadyAdded(tr){
				fmt.Println("transaction added ",tr)
				transactionArray+=tr
			}
			mutex.Unlock()
			v:= strings.Replace(msg.contents,"ready","copy",1)
			chatWithAllValidators(globalNode,globalOverlay,v)
		}else if ms == "copy"{
			u,_ := parseTransaction(msg.contents)
			mutex.Lock()
			if !transactionAlreadyAdded(u){
				fmt.Println("transaction added ",u)
				transactionArray+=u
			}
			mutex.Unlock()
			v:=strings.Replace(msg.contents,"copy","copied",1)
			chatToParticularNode(globalNode,globalOverlay,v,myCurrentDispatcher)
		} else if ms == "copied"{
			u, _ := parseTransaction(msg.contents)
			c.Inc(u)
			if c.Value(u) >= (len(listOfAddresses)-1)*len(listOfParticipantAddress)/2{
				if sent2==false{
					sent2=true
					v:=strings.Replace(msg.contents,"copied","commit",1)
					chatWithAllParticipants(globalNode,globalOverlay,v)
					v2 :=strings.Replace(msg.contents,"copied","commited",1)
					chatWithAllValidators(globalNode,globalOverlay,v2)
				}
			}
			time.AfterFunc(100*time.Millisecond, func() { sent2 = false })
		} else if ms == "commited"{
			fmt.Println("commited transaction msg to validator",tr)
			//fmt.Println(listTransactions)
		} else if ms=="commit"{
			fmt.Println("commit transaction msg to participant",tr)
			//fmt.Println(listTransactions)
		}
	}
	return nil
}

func transactionAlreadyAdded(u string)(bool){
	ts := strings.Split(transactionArray,"transaction")
	p:= strings.Replace(u,"transaction","",1)
	fmt.Println("all transactions",ts)
	fmt.Println("current transaction",p)
	for i:=0;i<len(ts);i++{
		if ts[i]==p{
			return true
		}
	}
	return false
}

func parseTransaction(line string) (string,string){
	lines := strings.Split(line,"_")
	res1 := strings.LastIndex(line, "_")
	t:= line[:res1]
	return t,lines[4]
}
// help prints out the users ID and commands available.
func help(node *noise.Node) {
	log.Printf("Your ID is %s(%s). Type '/discover' to attempt to discover new "+
		"peers, or '/peers' to list out all peers you are connected to.\n",
		node.ID().Address,
		node.ID().ID.String()[:printedLength],
	)
}

// bootstrap pings and dials an array of network addresses which we may interact with and  discover peers from.
func bootstrap(node *noise.Node, addresses []string) {
	for _, addr := range addresses {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := node.Ping(ctx, addr)
		cancel()

		if err != nil {
			log.Printf("Failed to ping bootstrap node (%s). Skipping... [error: %s]\n", addr, err)
			continue
		}
	}
}

// discover uses Kademlia to discover new peers from nodes we already are aware of.
func discover(overlay *kademlia.Protocol) {
	ids := overlay.Discover()

	var str []string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.ID.String()[:printedLength]))
	}

	if len(ids) > 0 {
		log.Printf("Discovered %d peer(s): [%v]\n", len(ids), strings.Join(str, ", "))
	} else {
		log.Printf("Did not discover any peers.\n")
	}
}

// peers prints out all peers we are already aware of.
func peers(overlay *kademlia.Protocol) {
	ids := overlay.Table().Peers()

	var str []string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.ID.String()[:printedLength]))
	}

	log.Printf("You know %d peer(s): [%v]\n", len(ids), strings.Join(str, ", "))
}

// chat handles sending chat messages and handling chat commands.
func chat(node *noise.Node, overlay *kademlia.Protocol, line string) {
	switch line {
	case "/discover":
		discover(overlay)
		return
	case "/peers":
		peers(overlay)
		return
	case "/tr":
		printTransactionArray()
		return
	default:
	}

	if strings.HasPrefix(line, "/") {
		help(node)
		return
	}
	if strings.HasPrefix(line,"+") || strings.HasPrefix(line,"-"){
		//t := transaction{transactionString: line,commitStatus: false}

		return
	}
	for _, id := range overlay.Table().Peers() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := node.SendMessage(ctx, id.Address, chatMessage{contents: line})
		cancel()

		if err != nil {
			log.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
				id.Address,
				id.ID.String()[:printedLength],
				err,
			)
			continue
		}
	}
}
func printTransactionArray()  {
	fmt.Println(transactionArray)
}
// chat handles sending chat messages and handling chat commands.
func chatToParticularNode(node *noise.Node, overlay *kademlia.Protocol, line string,addresses string) {
	log.Println("chat to particular node",addresses," msg ",line)
	switch line {
	case "/discover":
		discover(overlay)
		return
	case "/peers":
		peers(overlay)
		return
	default:
	}

	if strings.HasPrefix(line, "/") {
		help(node)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := node.SendMessage(ctx, addresses, chatMessage{contents: line})
	cancel()

	if err != nil {
		log.Printf("Failed to send message to %s. Skipping... [error: %s]\n",
			addresses,
			err,
		)
	}
}

// chat handles sending chat messages and handling chat commands.
func chatWithAllValidators(node *noise.Node, overlay *kademlia.Protocol, line string) {

	for _, id := range listOfAddresses {
		if id==myAddress{
			continue
		}
		alive := false
		for _, id2 := range overlay.Table().Peers() {
			if id == id2.Address{
				alive = true
				break
			}
		}
		if alive{
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := node.SendMessage(ctx, id, chatMessage{contents: line})
			cancel()

			if err != nil {
				log.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					id,
					//id.ID.String()[:printedLength],
					err,
				)
				continue
			}
		}
	}
}

// chat handles sending chat messages and handling chat commands.
func chatWithAllParticipants(node *noise.Node, overlay *kademlia.Protocol, line string) {

	for _, id := range listOfParticipantAddress {
		if id==myAddress{
			continue
		}
		alive := false
		for _, id2 := range overlay.Table().Peers() {
			if id == id2.Address{
				alive = true
				break
			}
		}
		if alive{
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := node.SendMessage(ctx, id, chatMessage{contents: line})
			cancel()

			if err != nil {
				log.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					id,
					//id.ID.String()[:printedLength],
					err,
				)
				continue
			}
		}
	}
}