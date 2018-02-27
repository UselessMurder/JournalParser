package main

import (
	"./journalParser"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {

	args := os.Args[1:]

	rand.Seed(time.Now().UnixNano())

	var wg sync.WaitGroup

	for _, s := range args {
		currentFile, err := os.Stat(s)
		if err != nil {
			fmt.Println(s, " ", err)
			continue
		}
		if mode := currentFile.Mode(); !mode.IsRegular() {
			fmt.Println(s, " is not regular file!")
			continue
		}

		inputFilename := filepath.Base(s)
		outputFilename := inputFilename + "." + randStringRunes(8) + ".result.txt"

		wg.Add(1)

		go func() {
			defer wg.Done()
			jP := journalParser.InitParser(inputFilename, outputFilename)
			if jerr := jP.Try(); jerr != nil {
				_, err = os.Stat(outputFilename)
				if err == nil {
					os.Remove(outputFilename)
				}
				fmt.Println(inputFilename, " ", jerr)
			}
		}()

	}

	wg.Wait()
}
