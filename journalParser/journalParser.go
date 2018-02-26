package journalParser

import (
	"../pairSort"
	"bufio"
	"encoding/csv"
	"errors"
	_ "fmt"
	"os"
	"sort"
	"strconv"
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

func (jL *journalLine) compareLine(currentjL *journalLine) bool {

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

	pl := make(pairSort.PairList, len(counts))
	i := 0
	for key, value := range counts {
		pl[i] = pairSort.Pair{key, value}
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

	pl := make(pairSort.PairList, len(counts))
	i := 0
	for key, value := range counts {
		pl[i] = pairSort.Pair{key, value}
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

type requestPair struct {
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

	counts := make(map[string][]*requestPair)

	for _, value := range jP.lines {
		if value.sourceUser == "" {
			continue
		}
		if value.destPort == 80 || value.destPort == 443 {
			if _, ok := counts[value.sourceUser]; !ok {
				counts[value.sourceUser] = append(counts[value.sourceUser], &requestPair{value, 1, value.messageTime, value.messageTime})
			} else {
				find := false
				for _, v := range counts[value.sourceUser] {
					if v.request.compareLine(value) {
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
					counts[value.sourceUser] = append(counts[value.sourceUser], &requestPair{value, 1, value.messageTime, value.messageTime})
				}
			}
		}
	}

	for key, value := range counts {
		for _, val := range value {
			if val.count > 5 {
				delta := val.last.Unix() - val.first.Unix()
				if delta > 3600 {
					_, err = out.WriteString("Пользователь: " + key + " сделал  " + strconv.FormatUint(val.count, 10) + " запросов  за " + strconv.FormatInt(delta/60, 10) + " минут на " + " " + val.request.destIP + " " + strconv.Itoa(val.request.destPort) + "\n")
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

	counts := make(map[string][]*requestPair)

	for _, value := range jP.lines {
		if value.sourceIP == "" {
			continue
		}
		if value.destPort == 80 || value.destPort == 443 {
			if _, ok := counts[value.sourceIP]; !ok {
				counts[value.sourceIP] = append(counts[value.sourceIP], &requestPair{value, 1, value.messageTime, value.messageTime})
			} else {
				find := false
				for _, v := range counts[value.sourceIP] {
					if v.request.compareLine(value) {
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
					counts[value.sourceIP] = append(counts[value.sourceIP], &requestPair{value, 1, value.messageTime, value.messageTime})
				}
			}
		}
	}

	for key, value := range counts {
		for _, val := range value {
			if val.count > 5 {
				delta := val.last.Unix() - val.first.Unix()
				if delta > 3600 {
					_, err = out.WriteString("С IP адреса: " + key + " было сделанно  " + strconv.FormatUint(val.count, 10) + " запросов  за " + strconv.FormatInt(delta/60, 10) + " минут на " + " " + val.request.destIP + " " + strconv.Itoa(val.request.destPort) + "\n")
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (jP *journalParser) fifthTask(out *bufio.Writer) error {
	var err error
	_, err = out.WriteString("# Рассматривая события сетевого трафика как символы неизвестного языка,\n# найти 5 наиболее устойчивых N-грамм журнала событий\n# (текста на неизвестном языке)\nРешение5\n")
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

	err = jP.secondTask(out)
	if err != nil {
		return err
	}

	err = jP.thirdTask(out)
	if err != nil {
		return err
	}

	err = jP.fourthTask(out)
	if err != nil {
		return err
	}

	err = jP.fifthTask(out)
	if err != nil {
		return err
	}

	out.Flush()

	return nil
}
