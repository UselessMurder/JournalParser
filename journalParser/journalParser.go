package journalParser

type journalParser struct {
	inputFile  string
	outputFile string
}

func InitParser(inputFile, outputFile string) journalParser {
	jP := journalParser{input_file, output_file}
	return jP
}

func (jp *journalParser) Try() error {

	return nil
}
