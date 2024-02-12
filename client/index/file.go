package index

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"
)

const (
	fieldDelimiter  = '|'
	lastFiledName   = "Last Data Received"
	lastFiledLayout = "January 2, 2006"
	numHeaders      = 5
	numFields       = 5
)

const (
	idxCIK = iota
	idxCompanyName
	idxFormType
	idxDateFiled
	idxFilename
)

func NewFile(r io.Reader) File {
	return File{
		buf: bufio.NewReader(r),
	}
}

type File struct {
	buf        *bufio.Reader
	headers    map[string]string
	fieldNames []string

	lastFiled time.Time
}

func (self *File) ReadHeaders() error {
	if err := self.readIndexHeader(); err != nil {
		return err
	}

	lastFiled, err := self.parseLastFiled()
	if err != nil {
		return err
	}
	self.lastFiled = lastFiled

	rowHeader, err := self.skipEmptyLines()
	if err != nil {
		return err
	}
	self.parseRowHeader(rowHeader)

	if s, err := self.readLine(); err != nil {
		return fmt.Errorf("skipping header divider: %w", err)
	} else if !strings.HasPrefix(s, "---") {
		return fmt.Errorf("got unexpected line %q after row header", s)
	}
	return nil
}

func (self *File) readIndexHeader() error {
	headers := make(map[string]string, numHeaders)
	for {
		s, err := self.readLine()
		if err != nil {
			return fmt.Errorf("reading header: %w", err)
		} else if s == "" {
			break
		}
		h, v, err := self.splitHeaderLine(s)
		if err != nil {
			return fmt.Errorf("reading header: %w", err)
		} else if h == "" || v == "" {
			return fmt.Errorf("invalid header line %q: %w", s, err)
		}
		headers[h] = v
	}
	if len(headers) == 0 {
		return errors.New("headers not found")
	}
	self.headers = headers
	return nil
}

func (self *File) readLine() (string, error) {
	line, err := self.buf.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		err = fmt.Errorf("readLine: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	return strings.TrimSpace(line), err
}

func (self *File) splitHeaderLine(s string) (string, string, error) {
	n := strings.IndexByte(s, ':')
	if n < 0 {
		return "", "", fmt.Errorf("separator not found in header line %q", s)
	}
	name := strings.TrimSpace(s[0:n])
	if n == len(s)-1 {
		return name, "", nil
	}
	return name, strings.TrimSpace(s[n+1:]), nil
}

func (self *File) parseLastFiled() (time.Time, error) {
	t, err := time.Parse(lastFiledLayout, self.headers[lastFiledName])
	if err != nil {
		return t, fmt.Errorf("failed parse header %q: %w", lastFiledName, err)
	}
	return t, nil
}

func (self *File) skipEmptyLines() (string, error) {
	for {
		s, err := self.readLine()
		if err != nil {
			return "", fmt.Errorf("skipping header empty lines: %w", err)
		}
		s = strings.TrimSpace(s)
		if s != "" {
			return s, nil
		}
	}
}

func (self *File) parseRowHeader(s string) {
	self.fieldNames = strings.Split(s, string(fieldDelimiter))
}

func (self *File) Headers() map[string]string {
	return maps.Clone(self.headers)
}

func (self *File) LastFiled() time.Time {
	return self.lastFiled
}

func (self *File) FieldNames() []string {
	return slices.Clone(self.fieldNames)
}

func (self *File) Iterate(fn func(*Item) error) error {
	r := csv.NewReader(self.buf)
	r.Comma = rune(fieldDelimiter)
	r.ReuseRecord = true
	for {
		records, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("iterating edgar index file: %w", err)
		} else if err := callIterFunc(fn, records); err != nil {
			return fmt.Errorf("failed iterate: %w", err)
		}
	}
	return nil
}

func callIterFunc(fn func(*Item) error, r []string) error {
	if len(r) < numFields {
		return fmt.Errorf("unexpected num of fields in record: %#v", r)
	}
	item := Item{
		CompanyName: r[idxCompanyName],
		FormType:    r[idxFormType],
		Filename:    r[idxFilename],
	}
	if err := item.parseCIK(r[idxCIK]); err != nil {
		return err
	} else if err := item.parseFiled(r[idxDateFiled]); err != nil {
		return err
	}
	return fn(&item)
}

func (self *File) CompaniesLastFiled() (map[uint32]time.Time, error) {
	lastFiled := map[uint32]time.Time{}
	err := self.Iterate(func(item *Item) error {
		if item.Filed.After(lastFiled[item.CIK]) {
			lastFiled[item.CIK] = item.Filed
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lastFiled, nil
}
