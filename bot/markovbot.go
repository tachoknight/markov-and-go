package main

import (
	"container/heap"
	"database/sql"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

// Our mutex to prevent crashing
var sMutex = &sync.Mutex{}

// An Item is something we manage in a priority queue.
type Item struct {
	value    string
	priority int32
	index    int
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	fmt.Println("Pushing ", item.value)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	fmt.Println("Popping ", item.value)
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value string, priority int32) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

var pq = make(PriorityQueue, 0)

// So we prevent the program from ending
var wg = &sync.WaitGroup{}

// Since this is a read-only program, we only
// need to know the number of words once, as we
// assume we're reading from a static copy
var wordCount int32

var punctuation = [...]string{"!", "?", "."}

func init() {
	rand.Seed(time.Now().Unix())
}

func getWordCount() {
	err := db.QueryRow("select count(*) from parent").Scan(&wordCount)
	if err != nil {
		fmt.Println("Hmm, got an error: ", err)
		return
	}
}

func getCountForWordID(id int32) int32 {
	childrenCount := 0
	err := db.QueryRow("select count(*) from children where parent_id = ?", id).Scan(&childrenCount)
	if err != nil {
		fmt.Println("Hmm, got an error: ", err)
		return 0
	}

	return int32(childrenCount)
}

func getRandomNum(max int32) int32 {
	if max == 0 {
		return 0
	}

	return rand.Int31n(max)
}

func getChildIDForID(parentID int32) int32 {
	return getRandomNum(getCountForWordID(parentID))
}

func getWord(id int32) string {
	if id == 0 {
		return "."
	}

	fmt.Println("Getting word for id ", id)

	word := ""
	err := db.QueryRow("select word from parent where id = ?", id).Scan(&word)
	if err != nil {
		fmt.Println("Hmm, got an error: ", err)
		return ""
	}

	return word
}

func buildSentence(startNum int32) (string, int32) {
	var sentence = ""
	var sentenceParts []string

	sentenceParts = append(sentenceParts, getWord(startNum))
	childWord := ""
	childID := startNum
	for childWord != "." {
		childID = getChildIDForID(childID)
		childWord = getWord(childID)
		sentenceParts = append(sentenceParts, childWord)
	}

	for _, word := range sentenceParts {
		if word != "." {
			sentence = sentence + " " + word
		} else {
			sentence = sentence + punctuation[getRandomNum(3)]
		}
	}

	return sentence, int32(len(sentenceParts))
}

func addSentence(randSeed int32) {
	defer wg.Done()

	newSeed := randSeed
	for {
		s, c := buildSentence(newSeed)

		i := &Item{
			value:    s,
			priority: c,
		}
		heap.Push(&pq, i)

		newSeed = getRandomNum(wordCount)
	}
}

func getSentence() {
	defer wg.Done()
	for {
		for pq.Len() > 0 {
			item := heap.Pop(&pq).(*Item)
			fmt.Println(item.value)
		}
	}
}

func main() {
	var err error

	//
	// Open the database...
	//
	db, err = sql.Open("sqlite3", "../markov.db?cache=shared")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	heap.Init(&pq)

	getWordCount()
	fmt.Println("Going to work with", wordCount, "words.")

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(i)
		time.Sleep(time.Duration(getRandomNum(20)))

		go addSentence(getRandomNum(wordCount))
	}

	go getSentence()
	wg.Add(1)

	wg.Wait()

	fmt.Println("Hello world")
}
