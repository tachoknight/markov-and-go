package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

var wordCount = 0

func getWordCount() {
	err := db.QueryRow("select count(*) from parent").Scan(&wordCount)
	if err != nil {
		fmt.Println("Hmm, got an error: ", err)
		return
	}
}

func getRandomNum(max int) int {
	if max == 0 {
		return 0
	}

	startRand := rand.New(rand.NewSource(time.Now().Unix()))
	return startRand.Intn(max)
}

func getChildIDForID(parentID int) int {
	rows, err := db.Query("select child_id, relationship_count from children where parent_id = ?", parentID)
	if err != nil {
		fmt.Println("Hmm, got an error: ", err)
		return 0
	}

	defer rows.Close()

	type child struct {
		childID int
		count   int
	}

	var children []child
	for rows.Next() {
		var c child
		if err := rows.Scan(&c.childID, &c.count); err != nil {
			fmt.Println("Hmm, got an error: ", err)
			return 0
		}

		children = append(children, c)
	}

	// Now pick a random number from the list
	childNum := getRandomNum(len(children))

	return childNum
}

func getWord(id int) string {
	if id == 0 {
		return "."
	}

	word := ""
	err := db.QueryRow("select word from parent where id = ?", id).Scan(&word)
	if err != nil {
		fmt.Println("Hmm, got an error: ", err)
		return ""
	}

	return word
}

func buildSentence() string {
	var sentence = ""

	startNum := getRandomNum(wordCount)

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
			sentence = sentence + word
		}
	}

	return sentence
}

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

	getWordCount()
	fmt.Println("Going to work with", wordCount, "words.")

	// 10 iterations
	for i := 0; i < 10; i++ {
		sentence := buildSentence()
		fmt.Println("-->", sentence)
	}

	fmt.Println("Hello world")
}
