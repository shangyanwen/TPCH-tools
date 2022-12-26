package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type SqlObj struct {
	// sql语句
	Sql string
	// sql编号
	SqlNo int
	// 查询结果集
	ResultSet []map[string]string
	// 影响行数
	RowsAffected int64
	// 错误信息
	ErrMsg string
	// 执行时长
	Duration float64
	// 数据文件
	LoadDataFile string
	// 表名
	Table string
}

var USERNAME string
var PASSWORD string
var HOST string
var PORT string
var DB string
var FILE_PATH string
var T_NUM int
var SQL_TYPE string
var LOAD_FIELD_SPLIT string
var LOAD_LINE_SPLIT string

// 系统初始化 -> 解析命令行参数
func init() {
	flag.StringVar(&USERNAME, "uname", "root", "DB Username")
	flag.StringVar(&PASSWORD, "pwd", "123456", "DB Password")
	flag.StringVar(&HOST, "host", "127.0.0.1", "DB IP")
	flag.StringVar(&PORT, "port", "3306", "DB Port")
	flag.StringVar(&DB, "db", "test", "DB Database")
	flag.StringVar(&FILE_PATH, "fd", " ", "SQL Query File Path or Data Dir path")
	flag.IntVar(&T_NUM, "t", 1, "Thread Num")
	flag.StringVar(&SQL_TYPE, "type", "query", "sql type: query/load/ddl")
	flag.StringVar(&LOAD_FIELD_SPLIT, "fsplit", "|", "load data file field split symbol, default: '|'")
	flag.StringVar(&LOAD_LINE_SPLIT, "lsplit", "\\n", "load data file line split symbol, default: '\\n'")

	flag.Parse()
	// 至少1个线程
	if T_NUM < 1 {
		T_NUM = 1
	}
	// 最多26个线程
	if T_NUM > 26 {
		T_NUM = 26
	}
	log.Printf("USERNAME=%s, PASSWORD=%s, HOST=%s, PORT=%s, DB=%s, FILE_PATH=%s, T_NUM=%d, SQL_TYPE=%s, LOAD_FIELD_SPLIT=%s, LOAD_LINE_SPLIT=%s\n", USERNAME, PASSWORD, HOST, PORT, DB, FILE_PATH, T_NUM, SQL_TYPE, LOAD_FIELD_SPLIT, LOAD_LINE_SPLIT)
}

// main
func main() {
	db, openErr := OpenDB()
	if openErr != nil {
		panic(openErr)
	}
	defer db.Close()

	var sqlArr []*SqlObj

	if strings.EqualFold(SQL_TYPE, "load") {
		arr, err := ParseDataDir(FILE_PATH, db)
		if err != nil {
			log.Printf("parse sql file err: %s\n", err)
			return
		}
		sqlArr = arr
	} else {
		arr, err := ParseSqlFile(FILE_PATH)
		if err != nil {
			log.Printf("parse sql file err: %s\n", err)
			return
		}
		sqlArr = arr
	}

	sqlNum := len(sqlArr)
	log.Printf("sql parse finish, sql num: %d\n", sqlNum)
	if sqlNum == 0 {
		panic("sql is empty!")
	}

	// 使用通道隔离多线程
	var sqlChan []chan *SqlObj
	for i := 0; i < T_NUM; i++ {
		ch := make(chan *SqlObj, sqlNum)
		sqlChan = append(sqlChan, ch)
	}

	// DDL(单线程执行)
	if strings.EqualFold(SQL_TYPE, "ddl") || strings.EqualFold(SQL_TYPE, "table") {
		go RunDdlSQl(sqlArr, db, sqlChan[0])
		// 生成创建表报告
		DdlReport(sqlArr, sqlChan[0])
		return
	}

	// load data(单线程执行)
	if strings.EqualFold(SQL_TYPE, "load") {
		// 注册数据文件
		for _, s := range sqlArr {
			mysql.RegisterLocalFile(s.LoadDataFile)
		}
		go RunDdlSQl(sqlArr, db, sqlChan[0])
		// 生成导入数据报告
		LoadDataReport(sqlArr, sqlChan[0])
		return
	}

	// query
	if strings.EqualFold(SQL_TYPE, "query") {
		for _, ch := range sqlChan {
			go RunQuerySql(sqlArr, db, ch)
		}
		// 生成查询报告
		QueryReport(sqlArr, sqlChan)
	}
}

func OpenDB() (*sql.DB, error) {
	jdbcUrl := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?interpolateParams=true", USERNAME, PASSWORD, HOST, PORT, DB)
	db, openErr := sql.Open("mysql", jdbcUrl)
	if openErr != nil {
		log.Printf("db open err: %s\n", openErr)
		return nil, openErr
	}
	db.SetMaxOpenConns(T_NUM)
	db.SetMaxIdleConns(T_NUM)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	pingErr := db.Ping()
	if pingErr != nil {
		log.Printf("db ping err: %s\n", pingErr)
		return nil, pingErr
	}
	return db, nil
}

// db query sql
func Query(db *sql.DB, SQL string) ([]map[string]string, error) {
	rows, err := db.Query(SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	count := len(columns)
	var values = make([]interface{}, count)
	for i := range values {
		var o interface{}
		values[i] = &o
	}
	// 结果集切片
	var ret []map[string]string
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}
		m := make(map[string]string)
		for i, colName := range columns {
			raw_value := *(values[i].(*interface{}))
			b, _ := raw_value.([]byte)
			v := string(b)
			m[colName] = v
		}
		ret = append(ret, m)
	}
	return ret, nil
}

// db ddl sql
func Exec(db *sql.DB, sql string) (int64, error) {
	r, err := db.Exec(sql)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// parse sql file
func ParseSqlFile(filePath string) ([]*SqlObj, error) {
	if strings.HasSuffix(filePath, "/") {
		filePath = filePath[0:strings.LastIndex(filePath, "/")]
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	buffer := new(bytes.Buffer)
	num := 1
	var arr []*SqlObj
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				sql := buffer.String()
				if strings.TrimSpace(sql) != "" {
					sqlEntity := &SqlObj{
						Sql:   sql,
						SqlNo: num,
					}
					arr = append(arr, sqlEntity)
					num++
					buffer.Reset()
				}
				log.Println("sql file read finish...")
				break
			} else {
				log.Printf("sql read file err: %s", err)
				return nil, err
			}
		}
		if strings.TrimSpace(line) == "" {
			continue
		}

		// 去掉注释
		if strings.Contains(line, "--") {
			idx := strings.Index(line, "--")
			noteStr := string(line[idx:])
			line = strings.ReplaceAll(line, noteStr, "")
		}

		if !strings.Contains(line, ";") {
			buffer.WriteString(line + " ")
		} else {
			linArr := strings.Split(line, ";")
			le := len(linArr)
			for i, s := range linArr {
				if i == le-1 {
					buffer.WriteString(s + " ")
					continue
				}

				if strings.TrimSpace(s) != "" {
					buffer.WriteString(s + " ")
				}

				sql := buffer.String()
				if strings.TrimSpace(sql) != "" {
					sqlEntity := &SqlObj{
						Sql:   sql,
						SqlNo: num,
					}
					arr = append(arr, sqlEntity)
					num++
					buffer.Reset()
				}
			}
		}
	}
	return arr, nil
}

// 解析数据目录，构建load data 命令
func ParseDataDir(dataPath string, db *sql.DB) ([]*SqlObj, error) {
	if strings.HasSuffix(dataPath, "/") {
		dataPath = dataPath[0:strings.LastIndex(dataPath, "/")]
	}
	fs, err := os.Stat(dataPath)
	if err != nil {
		return nil, err
	}
	var arr []*SqlObj
	if !fs.IsDir() {
		return arr, nil
	}

	rt, err := Query(db, "show tables")
	if err != nil {
		return nil, err
	}
	tables := make(map[string]string)
	for _, m := range rt {
		for _, v := range m {
			tables[v] = "1"
		}
	}

	dir, err := os.ReadDir(dataPath)
	if err != nil {
		return nil, err
	}

	num := 1
	for table := range tables {
		for _, d := range dir {
			if d.IsDir() {
				continue
			}

			fullFileName := d.Name()

			var fileName = ""
			if strings.Contains(fullFileName, ".") {
				fileName = fullFileName[0:strings.Index(fullFileName, ".")]
			} else {
				fileName = fullFileName
			}

			if fileName == "" {
				continue
			}

			// 文件名 == 表名
			if strings.EqualFold(fileName, table) {
				dataFile := dataPath + "/" + fullFileName
				// load data sql
				loadSql := fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s' LINES TERMINATED BY '%s'", dataFile, table, LOAD_FIELD_SPLIT, LOAD_LINE_SPLIT)
				sqlEntity := &SqlObj{
					Sql:          loadSql,
					LoadDataFile: dataFile,
					Table:        table,
					SqlNo:        num,
				}
				arr = append(arr, sqlEntity)
				num++
			}
		}
	}

	return arr, nil
}

// run query sql
func RunQuerySql(sqlArr []*SqlObj, db *sql.DB, ch chan *SqlObj) {
	defer close(ch)
	for _, v := range sqlArr {
		// copy new obj
		tmp := *v
		se := &tmp

		start := time.Now()
		result, err := Query(db, se.Sql)
		if err != nil {
			se.ErrMsg = err.Error()
			se.Duration = -1
		} else {
			sec := time.Since(start).Seconds()
			value, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", sec), 64)
			se.Duration = value
			se.ResultSet = result
		}

		ch <- se
	}
}

// run ddl sql
func RunDdlSQl(sqlArr []*SqlObj, db *sql.DB, ch chan *SqlObj) {
	defer close(ch)
	for _, v := range sqlArr {
		// copy new obj
		tmp := *v
		se := &tmp

		start := time.Now()
		r, err := Exec(db, se.Sql)
		if err != nil {
			se.ErrMsg = err.Error()
			se.Duration = -1
		} else {
			sec := time.Since(start).Seconds()
			value, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", sec), 64)
			se.Duration = value
			se.RowsAffected = r
		}

		ch <- se
	}
}

var COLUMN = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}

// query report
func QueryReport(sqlArr []*SqlObj, sqlChan []chan *SqlObj) {
	startTime := time.Now().Format("2006-01-02 15:04:05.00")

	xlsx := excelize.NewFile()

	xlsx.NewSheet("Stream")
	xlsx.SetCellStr("Stream", COLUMN[0]+"1", "Stream")
	xlsx.SetCellStr("Stream", COLUMN[1]+"1", "Start")
	xlsx.SetCellStr("Stream", COLUMN[2]+"1", "End")

	idx1 := xlsx.NewSheet("Query")
	xlsx.SetActiveSheet(idx1)
	xlsx.SetCellStr("Query", COLUMN[0]+"1", "Query")
	queryIdx := 2
	for _, v := range sqlArr {
		xlsx.SetCellInt("Query", COLUMN[0]+strconv.Itoa(queryIdx), v.SqlNo)
		queryIdx++
	}

	var wg = sync.WaitGroup{}
	streamIdx := 2
	for i, ch := range sqlChan {
		stream := "Stream" + strconv.Itoa(i)
		xlsx.SetCellStr("Stream", COLUMN[0]+strconv.Itoa(streamIdx), stream)

		// All Stream StartTime
		xlsx.SetCellStr("Stream", COLUMN[1]+strconv.Itoa(streamIdx), startTime)

		cell := COLUMN[i+1]
		xlsx.SetCellStr("Query", cell+"1", stream)

		wg.Add(1)
		go func(ch chan *SqlObj, streamIdx int, cell string, stream string) {
			defer wg.Done()
			i := 2
			for r := range ch {
				axis := cell + strconv.Itoa(i)
				if r.ErrMsg == "" {
					log.Printf("[query] (%s) Query%d Duration: %f\n", stream, r.SqlNo, r.Duration)
					xlsx.SetCellValue("Query", axis, r.Duration)
				} else {
					log.Printf("[query] (%s) Query%d err: %s\n", stream, r.SqlNo, r.ErrMsg)
					xlsx.SetCellValue("Query", axis, r.ErrMsg)
				}
				i++
			}
			// Stream EndTime
			xlsx.SetCellStr("Stream", COLUMN[2]+strconv.Itoa(streamIdx), time.Now().Format("2006-01-02 15:04:05.00"))
		}(ch, streamIdx, cell, stream)

		streamIdx++
	}
	wg.Wait()

	xlsx.DeleteSheet("Sheet1")
	finishTime := time.Now().Format("20060102150405")
	reportFilePath := fmt.Sprintf("./query_%d_report_%s.xlsx", T_NUM, finishTime)
	saveErr := xlsx.SaveAs(reportFilePath)
	if saveErr != nil {
		log.Printf("query report err: %s", saveErr)
	}
}

// load data report
func LoadDataReport(sqlArr []*SqlObj, sqlChan chan *SqlObj) {
	startTime := time.Now().Format("2006-01-02 15:04:05.00")

	xlsx := excelize.NewFile()
	idx := xlsx.NewSheet("Sheet1")
	xlsx.SetActiveSheet(idx)

	xlsx.SetCellStr("Sheet1", COLUMN[0]+"2", "table")
	xlsx.SetCellStr("Sheet1", COLUMN[1]+"2", "count")
	xlsx.SetCellStr("Sheet1", COLUMN[2]+"2", "seconds")

	i := 3
	for r := range sqlChan {
		xlsx.SetCellStr("Sheet1", COLUMN[0]+strconv.Itoa(i), r.Table)
		if r.ErrMsg == "" {
			log.Printf("[load] %d table: %s, load data success! duration: %f\n", r.SqlNo, r.Table, r.Duration)
			xlsx.SetCellValue("Sheet1", COLUMN[1]+strconv.Itoa(i), r.RowsAffected)
			xlsx.SetCellValue("Sheet1", COLUMN[2]+strconv.Itoa(i), r.Duration)
		} else {
			log.Printf("[load] %d table: %s, load data error: %s\n", r.SqlNo, r.Table, r.ErrMsg)
			xlsx.SetCellValue("Sheet1", COLUMN[1]+strconv.Itoa(i), r.ErrMsg)
			xlsx.SetCellValue("Sheet1", COLUMN[2]+strconv.Itoa(i), -1)
		}
		i++
	}

	endTime := time.Now().Format("2006-01-02 15:04:05.00")
	xlsx.MergeCell("Sheet1", COLUMN[0]+"1", COLUMN[2]+"1")
	xlsx.SetCellStr("Sheet1", COLUMN[0]+"1", fmt.Sprintf("开始时间 ~ 结束时间: %s ~ %s", startTime, endTime))

	finishTime := time.Now().Format("20060102150405")
	reportFilePath := "./load_data_report_" + finishTime + ".xlsx"
	saveErr := xlsx.SaveAs(reportFilePath)
	if saveErr != nil {
		log.Printf("load report err: %s", saveErr)
	}
}

// Ddl report
func DdlReport(sqlArr []*SqlObj, sqlChan chan *SqlObj) {
	startTime := time.Now().Format("2006-01-02 15:04:05.00")
	xlsx := excelize.NewFile()
	idx := xlsx.NewSheet("Sheet1")
	xlsx.SetActiveSheet(idx)

	xlsx.SetCellStr("Sheet1", COLUMN[0]+"2", "sqlNo")
	xlsx.SetCellStr("Sheet1", COLUMN[1]+"2", "seconds")

	i := 3
	for s := range sqlChan {
		xlsx.SetCellInt("Sheet1", COLUMN[0]+strconv.Itoa(i), s.SqlNo)
		if s.ErrMsg == "" {
			log.Printf("[ddl] %d exec success! duration: %f\n", s.SqlNo, s.Duration)
			xlsx.SetCellValue("Sheet1", COLUMN[1]+strconv.Itoa(i), s.Duration)
		} else {
			log.Printf("[ddl] %d exec err: %s\n", s.SqlNo, s.ErrMsg)
			xlsx.SetCellValue("Sheet1", COLUMN[1]+strconv.Itoa(i), s.ErrMsg)
		}
		i++
	}

	endTime := time.Now().Format("2006-01-02 15:04:05.00")
	xlsx.MergeCell("Sheet1", COLUMN[0]+"1", COLUMN[1]+"1")
	xlsx.SetCellStr("Sheet1", COLUMN[0]+"1", fmt.Sprintf("开始时间 ~ 结束时间: %s ~ %s", startTime, endTime))

	finishTime := time.Now().Format("20060102150405")
	reportFilePath := "./ddl_report_" + finishTime + ".xlsx"
	saveErr := xlsx.SaveAs(reportFilePath)
	if saveErr != nil {
		log.Printf("ddl report err: %s", saveErr)
	}
}
