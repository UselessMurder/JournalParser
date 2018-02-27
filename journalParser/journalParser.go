package journalParser

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"hash/crc64"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const layout = "2006-01-02T15:04:05.000-0700"

type journalLine struct {
	messageTime time.Time
	sourceUser  string
	sourceIP    string
	sourcePort  int
	destUser    string
	destIP      string
	destPort    int
	inputByte   int
	outputByte  int
}

func initLine(args []string) (*journalLine, error) {
	jL := journalLine{}
	if len(args) < 9 {
		return nil, errors.New("Currupted field with len: " + strconv.Itoa(len(args)))
	}
	var err error

	jL.messageTime, err = time.Parse(layout, args[0])
	if err != nil {
		return nil, err
	}

	jL.sourceUser = args[1]
	jL.sourceIP = args[2]

	jL.sourcePort, err = strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	jL.destUser = args[4]
	jL.destIP = args[5]

	jL.destPort, err = strconv.Atoi(args[6])
	if err != nil {
		return nil, err
	}

	jL.inputByte, err = strconv.Atoi(args[7])
	if err != nil {
		return nil, err
	}

	jL.outputByte, err = strconv.Atoi(args[8])
	if err != nil {
		return nil, err
	}

	return &jL, nil
}

func (jL *journalLine) compareLineByDest(currentjL *journalLine) bool {

	if jL.destUser != currentjL.destUser {
		return false
	}

	if jL.destPort != currentjL.destPort {
		return false
	}

	if jL.destIP != currentjL.destIP {
		return false
	}

	return true
}

type journalParser struct {
	inputFile  string
	outputFile string
	lines      []*journalLine
}

func InitParser(inputFile, outputFile string) *journalParser {
	jP := journalParser{inputFile, outputFile, nil}
	return &jP
}

func (jP *journalParser) parseInput() error {

	f, err := os.Open(jP.inputFile)
	if err != nil {
		return err
	}

	defer f.Close()

	var lines [][]string

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = 9

	lines, err = reader.ReadAll()
	if err != nil {
		return err
	}

	if len(lines) < 2 {
		return errors.New("Empty file!")
	}

	jP.lines = make([]*journalLine, len(lines)-1)
	for key, value := range lines {
		if key == 0 {
			continue
		}
		jP.lines[key-1], err = initLine(value)
		if err != nil {
			return err
		}
	}

	return nil
}

type pair struct {
	Key   string
	Value uint64
}

type pairList []pair

func (p pairList) Len() int           { return len(p) }
func (p pairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p pairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (jP *journalParser) firstTask(out *bufio.Writer) error {
	var err error
	_, err = out.WriteString("# Поиск 5ти пользователей, сгенерировавших наибольшее количество запросов\nРешение1\n")
	if err != nil {
		return err
	}

	counts := make(map[string]uint64)

	for _, value := range jP.lines {

		if value.destPort == 80 || value.destPort == 443 {
			if value.sourceUser == "" {
				continue
			}
			if _, ok := counts[value.sourceUser]; !ok {
				counts[value.sourceUser] = 1
			} else {
				counts[value.sourceUser]++
			}
		}
	}

	pl := make(pairList, len(counts))
	i := 0
	for key, value := range counts {
		pl[i] = pair{key, value}
		i++
	}

	sort.Sort(sort.Reverse(pl))

	for i, value := range pl {
		if i == 5 {
			break
		}
		_, err = out.WriteString(strconv.Itoa(i+1) + ": " + value.Key + ": " + strconv.FormatUint(value.Value, 10) + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func (jP *journalParser) secondTask(out *bufio.Writer) error {
	var err error
	_, err = out.WriteString("# Поиск 5ти пользователей, отправивших наибольшее количество данных\nРешение2\n")
	if err != nil {
		return err
	}

	counts := make(map[string]uint64)

	for _, value := range jP.lines {
		if value.sourceUser == "" {
			continue
		}
		if _, ok := counts[value.sourceUser]; !ok {
			counts[value.sourceUser] = uint64(value.inputByte)
		} else {
			counts[value.sourceUser] += uint64(value.inputByte)
		}
		if value.destUser == "" {
			continue
		}
		if _, ok := counts[value.destUser]; !ok {
			counts[value.destUser] = uint64(value.outputByte)
		} else {
			counts[value.destUser] += uint64(value.outputByte)
		}
	}

	pl := make(pairList, len(counts))
	i := 0
	for key, value := range counts {
		pl[i] = pair{key, value}
		i++
	}

	sort.Sort(sort.Reverse(pl))

	for i, value := range pl {
		if i == 5 {
			break
		}
		_, err = out.WriteString(strconv.Itoa(i+1) + ": " + value.Key + ": " + strconv.FormatUint(value.Value, 10) + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

type requestCount struct {
	request *journalLine
	count   uint64
	first   time.Time
	last    time.Time
}

func (jP *journalParser) thirdTask(out *bufio.Writer) error {
	var err error
	_, err = out.WriteString("# Поиск регулярных запросов (запросов выполняющихся периодически) по полю src_user\nРешение3\n")
	if err != nil {
		return err
	}

	counts := make(map[string][]*requestCount)

	for _, value := range jP.lines {
		if value.sourceUser == "" {
			continue
		}
		if value.destPort == 80 || value.destPort == 443 {
			if _, ok := counts[value.sourceUser]; !ok {
				counts[value.sourceUser] = append(counts[value.sourceUser], &requestCount{value, 1, value.messageTime, value.messageTime})
			} else {
				find := false
				for _, v := range counts[value.sourceUser] {
					if v.request.compareLineByDest(value) {
						v.count++
						if v.last.Before(value.messageTime) {
							v.last = value.messageTime
						}
						if v.first.After(value.messageTime) {
							v.first = value.messageTime
						}
						find = true
						break
					}
				}
				if !find {
					counts[value.sourceUser] = append(counts[value.sourceUser], &requestCount{value, 1, value.messageTime, value.messageTime})
				}
			}
		}
	}

	for key, value := range counts {
		for _, val := range value {
			if val.count > 5 {
				delta := val.last.Unix() - val.first.Unix()
				if delta > 3600 {
					_, err = out.WriteString(" Пользователь: " + key + " сделал  " + strconv.FormatUint(val.count, 10) + " запросов  за " + strconv.FormatInt(delta/60, 10) + " минут на  адрес " + val.request.destIP + ":" + strconv.Itoa(val.request.destPort) + "\n")
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (jP *journalParser) fourthTask(out *bufio.Writer) error {
	var err error
	_, err = out.WriteString("# Поиск регулярных запросов (запросов выполняющихся периодически) по полю src_ip\nРешение4\n")
	if err != nil {
		return err
	}

	counts := make(map[string][]*requestCount)

	for _, value := range jP.lines {
		if value.sourceIP == "" {
			continue
		}
		if value.destPort == 80 || value.destPort == 443 {
			if _, ok := counts[value.sourceIP]; !ok {
				counts[value.sourceIP] = append(counts[value.sourceIP], &requestCount{value, 1, value.messageTime, value.messageTime})
			} else {
				find := false
				for _, v := range counts[value.sourceIP] {
					if v.request.compareLineByDest(value) {
						v.count++
						if v.last.Before(value.messageTime) {
							v.last = value.messageTime
						}
						if v.first.After(value.messageTime) {
							v.first = value.messageTime
						}
						find = true
						break
					}
				}
				if !find {
					counts[value.sourceIP] = append(counts[value.sourceIP], &requestCount{value, 1, value.messageTime, value.messageTime})
				}
			}
		}
	}

	for key, value := range counts {
		for _, val := range value {
			if val.count > 5 {
				delta := val.last.Unix() - val.first.Unix()
				if delta > 3600 {
					_, err = out.WriteString(" С IP адреса: " + key + " было сделанно  " + strconv.FormatUint(val.count, 10) + " запросов  за " + strconv.FormatInt(delta/60, 10) + " минут на адрес " + val.request.destIP + ":" + strconv.Itoa(val.request.destPort) + "\n")
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

type nGram struct {
	count    uint64
	checksum uint64
	symbols  [5]*journalLine
}

func getChecksum(val *journalLine, table *crc64.Table) uint64 {
	return crc64.Checksum([]byte(val.sourceUser+strconv.Itoa(val.sourcePort)+val.destIP+strconv.Itoa(val.destPort)), table)
}

type nGrams []*nGram

func (n nGrams) Len() int           { return len(n) }
func (n nGrams) Less(i, j int) bool { return n[i].count < n[j].count }
func (n nGrams) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

type checkSumPair struct {
	index    int
	checksum uint64
}

type nGramStorage struct {
	storage   map[uint64]*nGram
	checksums []uint64
	result    nGrams
	doneChan  chan struct{}
	addChan   chan *nGram
	checkChan chan *checkSumPair
}

func (nGS *nGramStorage) Run(size int) {

	nGS.checksums = make([]uint64, size)
	nGS.doneChan = make(chan struct{})
	nGS.addChan = make(chan *nGram, 500)
	nGS.checkChan = make(chan *checkSumPair, 500)
	nGS.storage = make(map[uint64]*nGram)

	go func() {
		for {
			select {
			case currentNGram := <-nGS.addChan:
				if _, ok := nGS.storage[currentNGram.checksum]; !ok {
					currentNGram.count = 1
					nGS.storage[currentNGram.checksum] = currentNGram
				} else {
					nGS.storage[currentNGram.checksum].count++
				}
			case currentChecksum := <-nGS.checkChan:
				nGS.checksums[currentChecksum.index] = currentChecksum.checksum
			case <-nGS.doneChan:
				return
			}
		}
	}()
}

func (nGS *nGramStorage) Add(currentNGram *nGram) {
	nGS.addChan <- currentNGram
}

func (nGS *nGramStorage) AddCheckSum(currentChecksum *checkSumPair) {
	nGS.checkChan <- currentChecksum
}

func (nGS *nGramStorage) Done() {
	nGS.doneChan <- struct{}{}

	nGS.result = make(nGrams, len(nGS.storage))
	i := 0
	for _, value := range nGS.storage {
		nGS.result[i] = value
		i++
	}
}

func (nGS *nGramStorage) Close() {
	close(nGS.checkChan)
	close(nGS.addChan)
	close(nGS.doneChan)
}

func (jP *journalParser) fifthTask(out *bufio.Writer) error {
	var err error
	_, err = out.WriteString("# Рассматривая события сетевого трафика как символы неизвестного языка,\n# найти 5 наиболее устойчивых N-грамм журнала событий\n# (текста на неизвестном языке)\nРешение5\n")

	var storage nGramStorage

	storage.Run(len(jP.lines))

	crc64Table := crc64.MakeTable(0xC96C5795D7870F42)

	waiter := 0

	var wg sync.WaitGroup

	for i, _ := range jP.lines {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			currentChecksum := checkSumPair{}
			currentChecksum.checksum = getChecksum(jP.lines[iter], crc64Table)
			currentChecksum.index = iter
			storage.AddCheckSum(&currentChecksum)
		}(i)
		waiter++
		if waiter >= 500 {
			wg.Wait()
			waiter = 0
		}
	}

	if waiter != 0 {
		wg.Wait()
	}

	fmt.Println("Calc checksums done!")

	waiter = 0

	for i, _ := range jP.lines {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			var current nGram
			for j := 0; j < 5; j++ {
				if (j + iter) >= len(jP.lines) {
					return
				}
				current.symbols[j] = jP.lines[iter+j]
				current.checksum += storage.checksums[iter+j]
			}
			storage.Add(&current)
		}(i)
		waiter++
		if waiter >= 500 {
			wg.Wait()
			waiter = 0
		}
	}

	if waiter != 0 {
		wg.Wait()
	}

	storage.Done()

	storage.Close()

	sort.Sort(sort.Reverse(storage.result))

	for i, value := range storage.result {
		if i == 5 {
			break
		}
		_, err = out.WriteString("N-грамма №" + strconv.Itoa(i+1) + " встречаеться " + strconv.FormatUint(value.count, 10) + " раз :\n")
		if err != nil {
			return err
		}
		for _, val := range value.symbols {
			_, err = out.WriteString(" " + val.sourceUser + " " + strconv.Itoa(val.sourcePort) + " " + val.destIP + " " + strconv.Itoa(val.destPort) + "\n")
			if err != nil {
				return err
			}
		}
	}

	if err != nil {
		return err
	}
	return nil
}

func (jP *journalParser) Try() error {

	f, err := os.Create(jP.outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	err = jP.parseInput()
	if err != nil {
		return err
	}

	out := bufio.NewWriter(f)

	err = jP.firstTask(out)
	if err != nil {
		return err
	}

	fmt.Println("First task done!")

	err = jP.secondTask(out)
	if err != nil {
		return err
	}

	fmt.Println("Second task done!")

	err = jP.thirdTask(out)
	if err != nil {
		return err
	}

	fmt.Println("Third task done!")

	err = jP.fourthTask(out)
	if err != nil {
		return err
	}

	fmt.Println("Fourth task done!")

	err = jP.fifthTask(out)
	if err != nil {
		return err
	}

	fmt.Println("Fifth task done!")

	out.Flush()

	return nil
}
