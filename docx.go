package docx

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

//Contains functions to work with data from a zip file
type ZipData interface {
	files() []*zip.File
	close() error
}

//Type for in memory zip files
type ZipInMemory struct {
	data *zip.Reader
}

func (d ZipInMemory) files() []*zip.File {
	return d.data.File
}

//Since there is nothing to close for in memory, just nil the data and return nil
func (d ZipInMemory) close() error {
	d.data = nil
	return nil
}

//Type for zip files read from disk
type ZipFile struct {
	data *zip.ReadCloser
}

func (d ZipFile) files() []*zip.File {
	return d.data.File
}

func (d ZipFile) close() error {
	return d.data.Close()
}

type ReplaceDocx struct {
	zipReader ZipData
	content   string
	links     string
	headers   map[string]string
	footers   map[string]string
}

func (r *ReplaceDocx) Editable() *Docx {
	return &Docx{
		files:   r.zipReader.files(),
		content: r.content,
		links:   r.links,
		headers: r.headers,
		footers: r.footers,
		images:  make(map[string][]byte),
	}
}

func (r *ReplaceDocx) Close() error {
	return r.zipReader.close()
}

type Docx struct {
	files   []*zip.File
	content string
	links   string
	headers map[string]string
	footers map[string]string
	images  map[string][]byte
}

func (d *Docx) Content() string {
	return d.content
}
func (d *Docx) Files() []*zip.File {
	return d.files
}
func (d *Docx) ReplaceRaw(oldString string, newString string, num int) {
	d.content = strings.Replace(d.content, oldString, newString, num)
}

// func (d *Docx) Replace(oldString string, newString string, num int) (err error) {
// 	oldString, err = encode(oldString)
// 	if err != nil {
// 		return err
// 	}
// 	newString, err = encode(newString)
// 	if err != nil {
// 		return err
// 	}
// 	d.content = strings.Replace(d.content, oldString, newString, num)

// 	return nil
// }

type matchInfo struct {
	Index       int    //step matching index
	MatchSubStr string //which part of the text is matched
}

//Replace replace text blocks
func (d *Docx) Replace(oldString, newString string, num int) (err error) {
	if num == 0 {
		return nil
	}
	var indexListArray [][]matchInfo
	for i := 0; num == -1 || i < num; i++ {
		var tmpList []matchInfo
		ilLen := len(indexListArray)
		if ilLen == 0 {
			tmpList = d.markMatchTextBlocks(oldString, newString, 0)
		} else {
			lLen := len(indexListArray[ilLen-1])
			tmpList = d.markMatchTextBlocks(oldString, newString, indexListArray[ilLen-1][lLen-1].Index)
		}
		if len(tmpList) == 0 {
			break
		}
		indexListArray = append(indexListArray, tmpList)
	}
	// fmt.Printf("待处理的indexListArray :%v \r\n", indexListArray)
	d.replaceMatchTextBlocks(indexListArray, newString)
	return nil
}
func (d *Docx) replaceMatchTextBlocks(mlistArray [][]matchInfo, newString string) (list []matchInfo) {
	r := regexp.MustCompile(`<w\:t>.*?</w\:t>`)
	i := 0
	firstBlock := true
	d.content = r.ReplaceAllStringFunc(d.content, func(matchStr string) string {
		i++
		if len(mlistArray) == 0 {
			return matchStr
		}
		mlLen := len(mlistArray[0])
		if mlLen == 0 { //没有待处理的数组了
			mlistArray = mlistArray[1:] //抛弃当前这个,下一个
			firstBlock = true
			if len(mlistArray) == 0 { //纳尼?没了?那剩下的跳过
				return matchStr
			}
			mlLen = len(mlistArray[0]) //还有就读取一下这个数组成员继续搞
			if mlLen == 0 {            //妈卖批,谁他妈手动构造一个空数组传进来?正常不可能有空数组被传进来的
				return matchStr
			}
		}
		// fmt.Printf("i :%v \r\nmlistArray[0][0].Index:%v\r\n", i, mlistArray[0][0].Index)
		if i != mlistArray[0][0].Index { //序号对不上,跳过
			return matchStr
		}

		// fmt.Printf("当前正在处理的数组 :%v \r\nBlocks数据:%v\r\n", mlistArray[0], matchStr)
		curSubStr := mlistArray[0][0].MatchSubStr
		matchStr = matchStr[5 : len(matchStr)-6]
		if mlLen > 1 { //按顺序一个个block提出来
			if firstBlock { //多个tag组成的blocks第一个匹配项前面可能有多余的玩意儿,要和最后一个一样单独处理
				firstBlock = false
				lastMatchIndex := strings.LastIndex(matchStr, curSubStr)
				if lastMatchIndex == -1 {
					fmt.Println("WTF")
					return "<w:t></w:t>"
				}
				ret := "<w:t>" +
					matchStr[:lastMatchIndex] +
					matchStr[lastMatchIndex+len(curSubStr):] +
					"</w:t>" //第一个块留着未匹配部分,其它删除
				mlistArray[0] = mlistArray[0][1:] //删除这个已经处理的block记录
				return ret
			}
			mlistArray[0] = mlistArray[0][1:]
			return "<w:t></w:t>" //如果是中间的Block就直接替换为空就好了
		}

		//最后一个block有两种情况: 完全匹配剩余字符 以及 部分匹配
		var lastText string

		if matchStr == curSubStr { //如果是完全匹配,直接替换即可
			lastText = "<w:t>" + newString + "</w:t>"
		} else {
			//部分匹配时(比如 <w:t>hello</w:t> 匹配 hel ,要替换为 fuck)
			matchIndex := strings.Index(matchStr, curSubStr)
			if matchIndex == -1 {
				fmt.Println("WTF")
				return "<w:t></w:t>"
			}
			lastText = "<w:t>" +
				matchStr[:matchIndex] +
				newString +
				matchStr[matchIndex+len(curSubStr):] +
				"</w:t>" //替换匹配项为替换内容newString
			// lastText = "<w:t>" + strings.Replace(matchStr, curSubStr, newString, 1) + "</w:t>"
		}

		mlistArray[0] = mlistArray[0][1:]
		return lastText
	})
	return nil
}
func (d *Docx) markMatchTextBlocks(oldString, newString string, startIndex int) (list []matchInfo) {
	r := regexp.MustCompile(`<w\:t>.*?</w\:t>`) //Find all the text blocks
	remainingTexts := oldString

	i := 0
	r.ReplaceAllStringFunc(d.content, func(matchStr string) string {
		i++
		if i <= startIndex { //skip first n step
			return matchStr
		}
		if remainingTexts == "" { //already exact match
			return matchStr
		}
		matchStr = matchStr[5 : len(matchStr)-6] //Remove <w:t> and </w:t> from the beginning and the end
		// println("matchStr=" + matchStr + ",remainingTexts=" + remainingTexts)
		rLen := len(remainingTexts)
		for x := 0; x < rLen; x++ { //Find out if the current block matches from the remaining texts
			if mIndex := strings.LastIndex(matchStr, remainingTexts[:rLen-x]); mIndex != -1 { //This block contains some text to be replaced.
				if len(list) == 0 { // If the first match, check if the matching part is at the end of the block
					if x == 0 { //This block contains full text
						list = append(list, matchInfo{Index: i, MatchSubStr: remainingTexts})
						remainingTexts = ""
						break
					}
					// println("mIndex+len(remainingTexts[:rLen-x])  ", mIndex+len(remainingTexts[:rLen-x]), ",  len(matchStr) ", len(matchStr))
					if mIndex+len(remainingTexts[:rLen-x]) != len(matchStr) { //Not at the end
						break
					}
					list = append(list, matchInfo{Index: i, MatchSubStr: remainingTexts[:rLen-x]}) //At the end, mark it and judge the next block
					remainingTexts = remainingTexts[rLen-x:]
					break
				}
				//If it is not the first match, the rest must be at the beginning of the block
				if mIndex != 0 {
					remainingTexts = oldString
					list = []matchInfo{}
					break
				}
				// println("remainingTexts[:rLen-x] = " + remainingTexts[:rLen-x] + ", remainingTexts[rLen-x:]=" + remainingTexts[rLen-x:])
				list = append(list, matchInfo{Index: i, MatchSubStr: remainingTexts[:rLen-x]})
				remainingTexts = remainingTexts[rLen-x:]
				break
			}
			if x == rLen-1 { //This tag does not match at all, starting from scratch
				remainingTexts = oldString
				list = []matchInfo{}
			}
		}
		return matchStr
	})
	return
}

//ReplaceImage  replace Image in the docx
func (d *Docx) ReplaceImage(imageIndex int, pngBytes []byte) (err error) {
	d.images[fmt.Sprintf("%d", imageIndex)] = pngBytes
	return nil
}

//ReplaceIndexN  replace the match for the specified index n
func (d *Docx) ReplaceIndexN(oldString, newString string, n int) (err error) {
	if n < 0 {
		return nil
	}
	var indexListArray [][]matchInfo
	for i := 0; i < n+1; i++ {
		var tmpList []matchInfo
		ilLen := len(indexListArray)
		if ilLen == 0 {
			tmpList = d.markMatchTextBlocks(oldString, newString, 0)
		} else {
			lLen := len(indexListArray[ilLen-1])
			tmpList = d.markMatchTextBlocks(oldString, newString, indexListArray[ilLen-1][lLen-1].Index)
		}
		if len(tmpList) == 0 {
			break
		}
		indexListArray = append(indexListArray, tmpList)
	}
	if n >= len(indexListArray) {
		return nil
	}
	// fmt.Printf("待处理的indexListArray :%v \r\n", indexListArray)
	d.replaceMatchTextBlocks([][]matchInfo{indexListArray[n]}, newString) //构造一下,只包含要替换index的这个成员数组
	return nil
}

//ReplaceRawIndexN  replace the match for the specified index n on raw docx content
func (d *Docx) ReplaceRawIndexN(oldString string, newString string, n int) (err error) {
	index := 0
	i := 0
	lenOldStr := len(oldString)
	for index < len(d.content) {
		start := strings.Index(d.content[index:], oldString)
		// println(i)
		if start == -1 {
			if index != 0 {
				break
			}
			return nil
		}
		// println(i, "ok")
		if i-1 == n {
			break
		}
		index = index + start + lenOldStr
		i++
	}
	// println(d.content[:index])
	d.content = d.content[:index-lenOldStr] + newString + d.content[index:]
	return nil
}

func (d *Docx) ReplaceLink(oldString string, newString string, num int) (err error) {
	oldString, err = encode(oldString)
	if err != nil {
		return err
	}
	newString, err = encode(newString)
	if err != nil {
		return err
	}
	d.links = strings.Replace(d.links, oldString, newString, num)

	return nil
}

func (d *Docx) ReplaceHeader(oldString string, newString string) (err error) {
	return replaceHeaderFooter(d.headers, oldString, newString)
}

func (d *Docx) ReplaceFooter(oldString string, newString string) (err error) {
	return replaceHeaderFooter(d.footers, oldString, newString)
}

func (d *Docx) WriteToFile(path string) (err error) {
	var target *os.File
	target, err = os.Create(path)
	if err != nil {
		return
	}
	defer target.Close()
	err = d.Write(target)
	return
}

func (d *Docx) Write(ioWriter io.Writer) (err error) {
	w := zip.NewWriter(ioWriter)
	for _, file := range d.files {
		var writer io.Writer
		var readCloser io.ReadCloser

		writer, err = w.Create(file.Name)
		if err != nil {
			return err
		}
		defaultWrite := func() error {
			readCloser, err = file.Open()
			if err != nil {
				return err
			}
			writer.Write(streamToByte(readCloser))
			readCloser.Close()
			return nil
		}
		if file.Name == "word/document.xml" {
			writer.Write([]byte(d.content))
		} else if file.Name == "word/_rels/document.xml.rels" {
			writer.Write([]byte(d.links))
		} else if strings.Contains(file.Name, "header") && d.headers[file.Name] != "" {
			writer.Write([]byte(d.headers[file.Name]))
		} else if strings.Contains(file.Name, "footer") && d.footers[file.Name] != "" {
			writer.Write([]byte(d.footers[file.Name]))
		} else if imgIndex := getMid(file.Name, "word/media/image", "."); imgIndex != "" {
			if imgBytes, ok := d.images[imgIndex]; ok {
				writer.Write(imgBytes)
			} else {
				defaultWrite()
			}
		} else {
			defaultWrite()
		}
	}
	w.Close()
	return
}
func getMid(str string, left string, right string) string {
	lstart := strings.Index(str, left)
	if lstart == -1 {
		return ""
	}
	lstart = lstart + len(left)
	rstart := strings.Index(str[lstart:], right)
	if rstart == -1 {
		return ""
	}
	rstart += lstart
	//fmt.Println("l:", lstart, "r", rstart)
	return str[lstart:rstart]
}
func replaceHeaderFooter(headerFooter map[string]string, oldString string, newString string) (err error) {
	oldString, err = encode(oldString)
	if err != nil {
		return err
	}
	newString, err = encode(newString)
	if err != nil {
		return err
	}

	for k := range headerFooter {
		headerFooter[k] = strings.Replace(headerFooter[k], oldString, newString, -1)
	}

	return nil
}

func ReadDocxFromMemory(data io.ReaderAt, size int64) (*ReplaceDocx, error) {
	reader, err := zip.NewReader(data, size)
	if err != nil {
		return nil, err
	}
	zipData := ZipInMemory{data: reader}
	return ReadDocx(zipData)
}

func ReadDocxFile(path string) (*ReplaceDocx, error) {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	zipData := ZipFile{data: reader}
	return ReadDocx(zipData)
}

func ReadDocx(reader ZipData) (*ReplaceDocx, error) {
	content, err := readText(reader.files())
	if err != nil {
		return nil, err
	}

	links, err := readLinks(reader.files())
	if err != nil {
		return nil, err
	}

	headers, footers, _ := readHeaderFooter(reader.files())
	return &ReplaceDocx{zipReader: reader, content: content, links: links, headers: headers, footers: footers}, nil
}

func readHeaderFooter(files []*zip.File) (headerText map[string]string, footerText map[string]string, err error) {

	h, f, err := retrieveHeaderFooterDoc(files)

	if err != nil {
		return map[string]string{}, map[string]string{}, err
	}

	headerText, err = buildHeaderFooter(h)
	if err != nil {
		return map[string]string{}, map[string]string{}, err
	}

	footerText, err = buildHeaderFooter(f)
	if err != nil {
		return map[string]string{}, map[string]string{}, err
	}

	return headerText, footerText, err
}

func buildHeaderFooter(headerFooter []*zip.File) (map[string]string, error) {

	headerFooterText := make(map[string]string)
	for _, element := range headerFooter {
		documentReader, err := element.Open()
		if err != nil {
			return map[string]string{}, err
		}

		text, err := wordDocToString(documentReader)
		if err != nil {
			return map[string]string{}, err
		}

		headerFooterText[element.Name] = text
	}

	return headerFooterText, nil
}

func readText(files []*zip.File) (text string, err error) {
	var documentFile *zip.File
	documentFile, err = retrieveWordDoc(files)
	if err != nil {
		return text, err
	}
	var documentReader io.ReadCloser
	documentReader, err = documentFile.Open()
	if err != nil {
		return text, err
	}

	text, err = wordDocToString(documentReader)
	return
}

func readLinks(files []*zip.File) (text string, err error) {
	var documentFile *zip.File
	documentFile, err = retrieveLinkDoc(files)
	if err != nil {
		return text, err
	}
	var documentReader io.ReadCloser
	documentReader, err = documentFile.Open()
	if err != nil {
		return text, err
	}

	text, err = wordDocToString(documentReader)
	return
}

func wordDocToString(reader io.Reader) (string, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func retrieveWordDoc(files []*zip.File) (file *zip.File, err error) {
	for _, f := range files {
		if f.Name == "word/document.xml" {
			file = f
		}
	}
	if file == nil {
		err = errors.New("document.xml file not found")
	}
	return
}

func retrieveLinkDoc(files []*zip.File) (file *zip.File, err error) {
	for _, f := range files {
		if f.Name == "word/_rels/document.xml.rels" {
			file = f
		}
	}
	if file == nil {
		err = errors.New("document.xml.rels file not found")
	}
	return
}

func retrieveHeaderFooterDoc(files []*zip.File) (headers []*zip.File, footers []*zip.File, err error) {
	for _, f := range files {

		if strings.Contains(f.Name, "header") {
			headers = append(headers, f)
		}
		if strings.Contains(f.Name, "footer") {
			footers = append(footers, f)
		}
	}
	if len(headers) == 0 && len(footers) == 0 {
		err = errors.New("headers[1-3].xml file not found and footers[1-3].xml file not found.")
	}
	return
}

func streamToByte(stream io.Reader) []byte {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.Bytes()
}

func encode(s string) (string, error) {
	var b bytes.Buffer
	enc := xml.NewEncoder(bufio.NewWriter(&b))
	if err := enc.Encode(s); err != nil {
		return s, err
	}
	output := strings.Replace(b.String(), "<string>", "", 1) // remove string tag
	output = strings.Replace(output, "</string>", "", 1)
	output = strings.Replace(output, "&#xD;&#xA;", "<w:br/>", -1) // \r\n => newline
	return output, nil
}
