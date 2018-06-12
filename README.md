# Markov Chains in Go
These are a couple of programs I wrote to experiment with [Markov Chains](https://en.wikipedia.org/wiki/Markov_chain). 
## Builder
The `builder/markovbuilder.go` program recursively descends through the [Project Gutenberg DVD](http://www.gutenberg.org/wiki/Gutenberg:The_CD_and_DVD_Project), unzips each text and parses the sentences, building an in-memory map of word pairs along with their frequency. 

Once the book parsing is done it will then write the contents of the map to a sqlite database (`markov.db`). This is what will be used by the bot.

### Note about platforms
The program was written on a Linux machine, so it assumes the DVD is mounted at `/mnt/pgdvd`. Feel free to change this to wherever you have it mounted.


## Bot
The `bot/markovbot.go` program reads the populated sqlite database and builds "sentences" by randomly putting the words together. Right now it's output is pretty much gibberish so I'm planning on experimenting with additional methods of working with the data.

