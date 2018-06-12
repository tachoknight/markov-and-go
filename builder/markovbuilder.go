package main

import (
	"archive/zip"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/neurosnap/sentences/english"
	lex "github.com/timtadh/lexmachine"
	"github.com/timtadh/lexmachine/machines"
)

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Q U E U E  S T U F F
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type WorkRequest struct {
	FileName string
	Delay    time.Duration
}

type Worker struct {
	ID          int
	Work        chan WorkRequest
	WorkerQueue chan chan WorkRequest
	QuitChan    chan bool
}

// 100 is the buffer size
var WorkQueue = make(chan WorkRequest, 100)

var WorkerQueue chan chan WorkRequest

func Collector(fileName string) {
	delay, _ := time.ParseDuration("0s")

	work := WorkRequest{FileName: fileName, Delay: delay}

	// Push the work onto the queue.
	WorkQueue <- work
	log.Println("Work request queued for ", fileName)

	return
}

// This function "starts" the worker by starting a goroutine, that is
// an infinite "for-select" loop.
func (w *Worker) Start() {
	go func() {
		for {
			// Add ourselves into the worker queue.
			w.WorkerQueue <- w.Work

			select {
			case work := <-w.Work:
				// Receive a work request.
				log.Printf("worker%d: Received work request for %s\n", w.ID, work.FileName)

				time.Sleep(work.Delay)
				processFile(w.ID, work.FileName)

				log.Printf("worker%d: Finished work request for %s\n", w.ID, work.FileName)
			case <-w.QuitChan:
				// We have been asked to stop.
				log.Printf("worker%d stopping\n", w.ID)
				return
			}
		}
	}()
}

// NewWorker creates, and returns a new Worker object. Its only argument
// is a channel that the worker can add itself to whenever it is done its
// work.
func NewWorker(id int, workerQueue chan chan WorkRequest) Worker {
	// Create, and return the worker.
	worker := Worker{
		ID:          id,
		Work:        make(chan WorkRequest),
		WorkerQueue: workerQueue,
		QuitChan:    make(chan bool)}

	return worker
}

// Stop tells the worker to stop listening for work requests.
//
// Note that the worker will only stop *after* it has finished its work.
func (w *Worker) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}

func StartDispatcher(nworkers int) {
	// First, initialize the channel we are going to but the workers' work channels into.
	WorkerQueue = make(chan chan WorkRequest, nworkers)

	// Now, create all of our workers.
	for i := 0; i < nworkers; i++ {
		log.Println("Starting worker", i+1)
		worker := NewWorker(i+1, WorkerQueue)
		worker.Start()
	}

	go func() {
		for {
			select {
			case work := <-WorkQueue:
				log.Println("Received work requeust")
				go func() {
					worker := <-WorkerQueue

					log.Println("Dispatching work request")
					worker <- work
				}()
			}
		}
	}()
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// M A P  S T U F F
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var cMutex = &sync.Mutex{}

var parentID uint64
var parentMap = make(map[string]uint64)

// map of id to map of id to count
var childrenMap = make(map[uint64]map[uint64]uint64)

func getMapIDForWord(word string) uint64 {
	i, ok := parentMap[word]
	if ok == false {
		i = atomic.AddUint64(&parentID, 1)
		parentMap[word] = i
	}

	return i
}

func addMapRelationship(parent, child string) {
	cMutex.Lock()
	parentID := getMapIDForWord(parent)
	childID := getMapIDForWord(child)

	pI, ok := childrenMap[parentID]
	if ok == false {
		// New relationship
		newChildMap := make(map[uint64]uint64)
		newChildMap[childID] = 1
		childrenMap[parentID] = newChildMap
	} else {
		cI, ok := pI[childID]
		if ok == false {
			pI[childID] = 1
		} else {
			cI++
			pI[childID] = cI
		}
	}
	cMutex.Unlock()
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// W O R D  S T U F F
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type token struct {
	Type        int         // the token type
	Value       interface{} // a value associate with the token
	Lexeme      []byte      // the string that was matched
	TC          int         // the index (text counter) in the string
	StartLine   int
	StartColumn int
	EndLine     int
	EndColumn   int
}

var literals []string       // The tokens representing literal strings
var keywords []string       // The keyword tokens
var tokens []string         // All of the tokens (including literals and keywords)
var tokenIds map[string]int // A map from the token names to their int ids
var lexer *lex.Lexer        // The lexer object. Use this to construct a Scanner

// WORD is the position of the "WORD" item in the tokens array below
const WORD = 0

func initTokens() {
	// These are the punctuation characters we're interested in, all others are
	// ignored
	literals = []string{
		",",
		".",
		";",
		":",
		"-",
		"'",
		"(",
		")",
		"?",
		"&",
		"!",
		"\"",
		"--",
	}

	// Set up our classifications
	tokens = []string{
		"WORD",
		"NUMBER",
	}
	tokens = append(tokens, literals...)
	tokenIds = make(map[string]int)
	for i, tok := range tokens {
		tokenIds[tok] = i
	}
}

var db *sql.DB

func tokenize(name string) lex.Action {
	return func(s *lex.Scanner, m *machines.Match) (interface{}, error) {
		return s.Token(tokenIds[name], string(m.Bytes), m), nil
	}
}

func skip(*lex.Scanner, *machines.Match) (interface{}, error) {
	return nil, nil
}

// Creates the lexer object and compiles the NFA.
func initLexer() (*lex.Lexer, error) {
	lexer := lex.NewLexer()

	for _, lit := range literals {
		r := "\\" + strings.Join(strings.Split(lit, ""), "\\")
		lexer.Add([]byte(r), tokenize(lit))
	}
	for _, name := range keywords {
		lexer.Add([]byte(strings.ToLower(name)), tokenize(name))
	}

	// Words we don't want
	lexer.Add([]byte("([e|E]Book?.)"), skip)
	lexer.Add([]byte("([e|E][T|t]ext?.)"), skip)
	lexer.Add([]byte("(http?.)"), skip)
	lexer.Add([]byte("(tm)"), skip)
	lexer.Add([]byte("(HTML)"), skip)
	lexer.Add([]byte("(ASCII)"), skip)
	// Our main regex
	lexer.Add([]byte(`([a-z]|[A-Z])([a-z]|[A-Z]|[0-9]|_)*`), tokenize("WORD"))
	// Skip new lines, carriage returns, etc..
	lexer.Add([]byte("( |\t|\n|\r)+"), skip)
	// Skip characters we don't care about in regular text
	lexer.Add([]byte("\\*|\\<|\\>|\\@|\\/|\\~|\\[|\\]|\\{|\\}|\\||\\_|\\^|\\$|\\#|\\%|\\+|\\="), skip)
	// Skip URLs
	lexer.Add([]byte("www.([a-z]|[A-Z])([a-z]|[A-Z]|[0-9]|_)*.([a-z]|[A-Z])([a-z]|[A-Z]|[0-9]|_)*"), skip)
	// Ignore numbers
	lexer.Add([]byte("[0-9]\\."), skip)
	lexer.Add([]byte("[0-9]"), skip)

	err := lexer.Compile()
	if err != nil {
		return nil, err
	}
	return lexer, nil
}

func init() {
	initTokens()
	var err error
	lexer, err = initLexer()
	if err != nil {
		panic(err)
	}
}

func unzip(src string, dest string) ([]string, error) {
	var filenames []string

	r, err := zip.OpenReader(src)

	if err != nil {
		return filenames, err
	}
	defer r.Close()

	for _, f := range r.File {
		rc, err := f.Open()

		if err != nil {
			return filenames, err
		}
		defer rc.Close()

		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)
		filenames = append(filenames, fpath)

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
		} else {
			// Make File
			if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
				return filenames, err
			}

			outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return filenames, err
			}

			_, err = io.Copy(outFile, rc)

			// Close the file without defer to close before next iteration of loop
			outFile.Close()

			if err != nil {
				return filenames, err
			}
		}
	}

	return filenames, nil
}

func saveToDb() {
	log.Println("--> Now saving to the database...")
	defer log.Println("<-- Finished saving to the database!")

	// Now let's do some transactions...
	tx, err := db.Begin()

	log.Println("\tAdding parents...")
	for key, value := range parentMap {
		if err != nil {
			log.Printf("Hmm, could not start a transaction for working with word %s:\n\t%v", key, err)
		}

		newWordSQL := "insert into parent (id, word) values (?, ?)"
		_, err = tx.Exec(newWordSQL, value, key)
		if err != nil {
			log.Printf("Hmm, got an error when trying to insert the word into the parent table for %s:\n\t%v", key, err)
			tx.Rollback()
		}
	}

	log.Println("\tFinished adding parents...now adding children")

	for ckey, cvalue := range childrenMap {
		for c2key, c2value := range cvalue {
			newRelationshipSQL := "insert into children (parent_id, child_id, relationship_count) values (?, ?, ?)"
			_, err = tx.Exec(newRelationshipSQL, ckey, c2key, c2value)
			if err != nil {
				log.Printf("Hmm, got an error when trying to insert the children records into the children table for %d/%d/%d:\n\t%v", ckey, c2key, c2value, err)
				tx.Rollback()
			}
		}
	}

	log.Println("\tFinished adding children")

	// Did we make it all the way here? Sweet, we're good then.
	tx.Commit()
}

func processFile(workerID int, zipFile string) {
	wg.Add(1)
	defer wg.Done()

	log.Println("----> Worker ", workerID, " Starting with ", zipFile)
	defer log.Println("<---- Worker ", workerID, " Finished with ", zipFile)

	val := strings.LastIndex(zipFile, "/")
	dir := zipFile[val:]
	dirs := filepath.Join(os.TempDir(), dir)

	files, err := unzip(zipFile, dirs)
	if err != nil {
		log.Println(err)
		os.RemoveAll(dirs)
		return
	}

	tokenizer, err := english.NewSentenceTokenizer(nil)
	if err != nil {
		panic(err)
	}

	// We only want words with no punctuation or numbers
	isAlpha := regexp.MustCompile(`^[A-Za-z]+$`).MatchString

	// For each file in the zip file...
	for _, element := range files {
		b, err := ioutil.ReadFile(element)
		if err != nil {
			log.Print(err)
		}

		// ... convert the text to a string...
		text := string(b)
		// ... and tokenize it into an array of sentences...
		sentences := tokenizer.Tokenize(text)
		// ... and now let's go through each sentence...
		for _, s := range sentences {
			//log.Println("----> ", s.Text)
			scanner, err := lexer.Scanner([]byte(s.Text))
			if err != nil {
				// handle err
			}

			parentWord := ""
			childWord := ""
			inRelationship := false

			// ... and examime each token in the sentence
			for tok, err, eof := scanner.Next(); !eof; tok, err, eof = scanner.Next() {
				if _, is := err.(*machines.UnconsumedInput); is {
					// skip the error via:
					// scanner.TC = ui.FailTC
					//
					//log.Println(err)
				} else if err != nil {
					//log.Println(err)
				}

				// And here we work with the individual tokens themselves, which
				// as of right now we only care about the words and some punctuation
				// to indicate natural breaks (e.g. commas, semicolons, etc.)
				//log.Println("=====> ", tok)

				if tok == nil {
					break
				}

				// This is the word we're working with
				currentToken := string(tok.(*lex.Token).Lexeme)

				// Strip out underscores
				currentToken = strings.Replace(currentToken, "_", "", -1)

				// Make lowercase
				currentToken = strings.ToLower(currentToken)

				// If the current token isn't a true word, we don't want it
				if isAlpha(currentToken) == false {
					break
				}

				if tok.(*lex.Token).Type == WORD {
					if inRelationship == false {
						// If we're not in a relationship, we'll start one
						// now with this token as the parent
						parentWord = currentToken
						// and no child (yet)
						childWord = ""
						// And we're in a relationship now
						inRelationship = true
					} else {
						// We are currently in a relationship, so
						// the child is our current token
						childWord = currentToken

						// And we want to add this relationship to
						// the database but strip off underscores that
						// may be on there
						addMapRelationship(parentWord, childWord)

						// And reset everything
						parentWord = ""
						childWord = ""
						inRelationship = false
					}
				} else {
					// Punctuation or whatever

					// If we're in a relationship, we're going to
					// end it and as of right now, throw the parent
					// and child away
					parentWord = ""
					childWord = ""
					inRelationship = false
				}
			}
		}
	}

	os.RemoveAll(dirs)
}

var (
	NWorkers = flag.Int("n", runtime.NumCPU(), "The number of workers to start")
	wg       sync.WaitGroup
)

func main() {
	var err error

	//
	// Open the database...
	//
	db, err = sql.Open("sqlite3", "../markov.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.Exec("PRAGMA journal_mode=WAL;")
	db.SetMaxOpenConns(1)

	//
	// Start the dispatcher.
	//
	fmt.Println("Starting the dispatcher")
	StartDispatcher(*NWorkers)

	// The iso is available at http://www.gutenberg.org/wiki/Gutenberg:The_CD_and_DVD_Project
	searchDir := "/mnt/pgdvd"

	_ = filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		val := strings.LastIndex(path, ".zip")
		if val != -1 {
			Collector(path)
		}
		return nil
	})

	wg.Wait()

	time.Sleep(1000 * time.Millisecond)

	saveToDb()

	log.Println("hello world!")
}
