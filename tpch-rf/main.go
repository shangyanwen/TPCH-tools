package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/jmoiron/sqlx"
	"github.com/scylladb/termtables"
)

type Orders struct {
	O_ORDERKEY      int64
	O_CUSTKEY       int64
	O_ORDERSTATUS   string
	O_TOTALPRICE    float64
	O_ORDERDATE     string
	O_ORDERPRIORITY string
	O_CLERK         string
	O_SHIPPRIORITY  int64
	O_COMMENT       string
}

type Lineitem struct {
	L_ORDERKEY      int64
	L_PARTKEY       int64
	L_SUPPKEY       int64
	L_LINENUMBER    int64
	L_QUANTITY      float64
	L_EXTENDEDPRICE float64
	L_DISCOUNT      float64
	L_TAX           float64
	L_RETURNFLAG    string
	L_LINESTATUS    string
	L_SHIPDATE      string
	L_COMMITDATE    string
	L_RECEIPTDATE   string
	L_SHIPINSTRUCT  string
	L_SHIPMODE      string
	L_COMMENT       string
}

var USERNAME string
var PASSWORD string
var HOST string
var PORT string
var DB string
var FILE_PATH string
var TYPE string
var SF int

// 系统初始化 -> 解析命令行参数
func init() {
	flag.StringVar(&USERNAME, "uname", "root", "DB Username")
	flag.StringVar(&PASSWORD, "pwd", "123456", "DB Password")
	flag.StringVar(&HOST, "host", "127.0.0.1", "DB IP")
	flag.StringVar(&PORT, "port", "3306", "DB Port")
	flag.StringVar(&DB, "db", "test", "DB Database")
	flag.StringVar(&FILE_PATH, "fd", " ", "Data Dir path")
	flag.StringVar(&TYPE, "type", "rf1", "tpch RF: rf1/rf2")
	flag.IntVar(&SF, "sf", 1, "tpch SF")

	flag.Parse()

	fmt.Printf("USERNAME=%s, PASSWORD=%s, HOST=%s, PORT=%s, DB=%s, FILE_PATH=%s, TYPE=%s, SF=%d\n", USERNAME, PASSWORD, HOST, PORT, DB, FILE_PATH, TYPE, SF)
}

func main() {
	if strings.HasSuffix(FILE_PATH, "/") {
		FILE_PATH = FILE_PATH[0:strings.LastIndex(FILE_PATH, "/")]
	}
	fs, err := os.Stat(FILE_PATH)
	if err != nil {
		panic(err)
	}
	if !fs.IsDir() {
		fmt.Printf("fd is not dir: %s\n", FILE_PATH)
		return
	}

	db, openErr := OpenDB()
	if openErr != nil {
		panic(openErr)
	}
	defer db.Close()

	l, _ := time.LoadLocation("Asia/Shanghai")

	// insert
	if strings.EqualFold(TYPE, "rf1") {

		var ordersFiles []string
		var lineitemFiles []string
		files, _ := ioutil.ReadDir(FILE_PATH)
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			fileName := f.Name()
			// orders
			if strings.HasPrefix(strings.ToLower(fileName), "orders") {
				ordersFiles = append(ordersFiles, fileName)
			}
			// lineitem
			if strings.HasPrefix(strings.ToLower(fileName), "lineitem") {
				lineitemFiles = append(lineitemFiles, fileName)
			}
		}
		fmt.Printf("orders files num: %d\n", len(ordersFiles))
		fmt.Printf("lineitem files num: %d\n", len(lineitemFiles))

		times := SF * 1500

		ordersCh := make(chan Orders, times)
		lineitemCh := make(chan Lineitem, times*7)

		go ParseInsertOrdersValues(ordersFiles, ordersCh)
		go ParseInsertLineitemValues(lineitemFiles, lineitemCh)

		var insertOrdersDuration float64
		var insertOrdersRows int64
		var insertOrdersExpectRows = int64(times)
		var insertLineitemDuration float64
		var insertLineitemRows int64
		var insertLineitemExpectRows int64

		startTime := time.Now().In(l).Format("2006-01-02 15:04:05.00")
		insertOrderSql := "INSERT INTO ORDERS(O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT) VALUES(?,?,?,?,?,?,?,?,?)"
		insertLineitemSql := "INSERT INTO LINEITEM(L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
		r := rand.New(rand.NewSource(time.Now().Unix()))
		for i := 0; i < times; i++ {
			ordersValues := <-ordersCh
			if reflect.DeepEqual(ordersValues, Orders{}) {
				continue
			}

			start1 := time.Now()
			r1, err := ExecSql(db, insertOrderSql, ordersValues.O_ORDERKEY, ordersValues.O_CUSTKEY, ordersValues.O_ORDERSTATUS, ordersValues.O_TOTALPRICE, ordersValues.O_ORDERDATE, ordersValues.O_ORDERPRIORITY, ordersValues.O_CLERK, ordersValues.O_SHIPPRIORITY, ordersValues.O_COMMENT)
			insertOrdersRows += r1
			if err != nil {
				fmt.Printf("insert orders err: %v\n", err)
			}
			end1 := time.Since(start1).Seconds()
			t1, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", end1), 64)
			insertOrdersDuration += t1

			lineitemTimes := r.Intn(7) + 1
			insertLineitemExpectRows += int64(lineitemTimes)
			for j := 0; j < lineitemTimes; j++ {
				lineitemValues := <-lineitemCh
				if reflect.DeepEqual(lineitemValues, Lineitem{}) {
					continue
				}

				start2 := time.Now()
				r2, err := ExecSql(db, insertLineitemSql, lineitemValues.L_ORDERKEY, lineitemValues.L_PARTKEY, lineitemValues.L_SUPPKEY, lineitemValues.L_LINENUMBER, lineitemValues.L_QUANTITY, lineitemValues.L_EXTENDEDPRICE, lineitemValues.L_DISCOUNT, lineitemValues.L_TAX, lineitemValues.L_RETURNFLAG, lineitemValues.L_LINESTATUS, lineitemValues.L_SHIPDATE, lineitemValues.L_COMMITDATE, lineitemValues.L_RECEIPTDATE, lineitemValues.L_SHIPINSTRUCT, lineitemValues.L_SHIPMODE, lineitemValues.L_COMMENT)
				insertLineitemRows += r2
				if err != nil {
					fmt.Printf("insert lineitem err: %v\n", err)
				}
				end2 := time.Since(start2).Seconds()
				t2, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", end2), 64)
				insertLineitemDuration += t2
			}
		}
		endTime := time.Now().In(l).Format("2006-01-02 15:04:05.00")

		fmt.Println("---------------------rf1---------------------")
		fmt.Printf("------startTime: %s\n", startTime)
		fmt.Printf("------endTime: %s\n", endTime)
		t := termtables.CreateTable()
		t.AddHeaders("Table", "ExpectRows", "Rows", "Duration(s)")
		t.AddRow("ORDERS", insertOrdersExpectRows, insertOrdersRows, insertOrdersDuration)
		t.AddRow("LINEITEM", insertLineitemExpectRows, insertLineitemRows, insertLineitemDuration)
		fmt.Println(t.Render())
		return
	}

	// delete
	if strings.EqualFold(TYPE, "rf2") {
		var deleteFiles []string
		files, _ := ioutil.ReadDir(FILE_PATH)
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			fileName := f.Name()
			if strings.HasPrefix(strings.ToLower(fileName), "delete") {
				deleteFiles = append(deleteFiles, fileName)
			}
		}
		fmt.Printf("delete files num: %d\n", len(deleteFiles))

		times := SF * 1500

		deleteCh := make(chan int64, times)

		go ParseDeleteOrderkey(deleteFiles, deleteCh)

		var deleteOrdersDuration float64
		var deleteOrdersRows int64
		var deleteOrdersExpectRows = int64(times)
		var deleteLineitemDuration float64
		var deleteLineitemRows int64
		var deleteLineitemExpectRows = int64(times)

		startTime := time.Now().In(l).Format("2006-01-02 15:04:05.00")
		for i := 0; i < times; i++ {
			orderkey := <-deleteCh
			fmt.Printf("delete orderkey: %d\n", orderkey)
			if orderkey == 0 {
				continue
			}

			start1 := time.Now()
			r1, err := ExecSql(db, fmt.Sprintf("DELETE FROM ORDERS WHERE O_ORDERKEY = %d", orderkey))
			deleteOrdersRows += r1
			if err != nil {
				fmt.Printf("delete orders err: %v\n", err)
			}
			end1 := time.Since(start1).Seconds()
			t1, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", end1), 64)
			deleteOrdersDuration += t1

			start2 := time.Now()
			r2, err := ExecSql(db, fmt.Sprintf("DELETE FROM LINEITEM WHERE L_ORDERKEY = %d", orderkey))
			deleteLineitemRows += r2
			if err != nil {
				fmt.Printf("delete lineitem err: %v\n", err)
			}
			end2 := time.Since(start2).Seconds()
			t2, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", end2), 64)
			deleteLineitemDuration += t2
		}
		endTime := time.Now().In(l).Format("2006-01-02 15:04:05.00")

		fmt.Println("---------------------rf2---------------------")
		fmt.Printf("------startTime: %s\n", startTime)
		fmt.Printf("------endTime: %s\n", endTime)
		t := termtables.CreateTable()
		t.AddHeaders("Table", "ExpectRows", "Rows", "Duration(s)")
		t.AddRow("ORDERS", deleteOrdersExpectRows, deleteOrdersRows, deleteOrdersDuration)
		t.AddRow("LINEITEM", deleteLineitemExpectRows, deleteLineitemRows, deleteLineitemDuration)
		fmt.Println(t.Render())
		return
	}
}

func ParseInsertOrdersValues(fileNames []string, ch chan Orders) {
	// 关闭通道
	defer close(ch)
	for _, fileName := range fileNames {
		file, err := os.Open(FILE_PATH + "/" + fileName)
		if err != nil {
			fmt.Printf("open file: %s, err: %v\n", fileName, err)
			continue
		}
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("read file: %s err: %v\n", fileName, err)
				}
				break
			}
			values := strings.Split(line, "|")
			values = values[0:9]
			var orders Orders

			orderkey, err := strconv.ParseInt(values[0], 10, 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				orders.O_ORDERKEY = orderkey
			}

			custkey, err := strconv.ParseInt(values[1], 10, 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				orders.O_CUSTKEY = custkey
			}

			orders.O_ORDERSTATUS = values[2]

			price, err := strconv.ParseFloat(values[3], 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				orders.O_TOTALPRICE = price
			}
			orders.O_ORDERDATE = values[4]
			orders.O_ORDERPRIORITY = values[5]
			orders.O_CLERK = values[6]
			shippriority, err := strconv.ParseInt(values[7], 10, 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				orders.O_SHIPPRIORITY = shippriority
			}
			orders.O_COMMENT = values[8]

			ch <- orders
		}
		file.Close()
	}
}

func ParseInsertLineitemValues(fileNames []string, ch chan Lineitem) {
	// 关闭通道
	defer close(ch)
	for _, fileName := range fileNames {
		file, err := os.Open(FILE_PATH + "/" + fileName)
		if err != nil {
			fmt.Printf("open file: %s, err: %v\n", fileName, err)
			continue
		}
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("read file: %s err: %v\n", fileName, err)
				}
				break
			}
			values := strings.Split(line, "|")
			values = values[0:16]
			var lineitem Lineitem

			orderkey, err := strconv.ParseInt(values[0], 10, 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				lineitem.L_ORDERKEY = orderkey
			}

			partkey, err := strconv.ParseInt(values[1], 10, 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				lineitem.L_PARTKEY = partkey
			}

			suppkey, err := strconv.ParseInt(values[2], 10, 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				lineitem.L_SUPPKEY = suppkey
			}

			lineitemumber, err := strconv.ParseInt(values[3], 10, 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				lineitem.L_LINENUMBER = lineitemumber
			}

			quantity, err := strconv.ParseFloat(values[4], 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				lineitem.L_QUANTITY = quantity
			}

			price, err := strconv.ParseFloat(values[5], 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				lineitem.L_EXTENDEDPRICE = price
			}

			discount, err := strconv.ParseFloat(values[6], 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				lineitem.L_DISCOUNT = discount
			}

			tax, err := strconv.ParseFloat(values[7], 64)
			if err != nil {
				fmt.Printf("parse err: %v\n", err)
			} else {
				lineitem.L_TAX = tax
			}

			lineitem.L_RETURNFLAG = values[8]
			lineitem.L_LINESTATUS = values[9]
			lineitem.L_SHIPDATE = values[10]
			lineitem.L_COMMITDATE = values[11]
			lineitem.L_RECEIPTDATE = values[12]
			lineitem.L_SHIPINSTRUCT = values[13]
			lineitem.L_SHIPMODE = values[14]
			lineitem.L_COMMENT = values[15]

			ch <- lineitem
		}
		file.Close()
	}
}

func ParseDeleteOrderkey(fileNames []string, ch chan int64) {
	// 关闭通道
	defer close(ch)
	for _, fileName := range fileNames {
		file, err := os.Open(FILE_PATH + "/" + fileName)
		if err != nil {
			fmt.Printf("open file: %s, err: %v\n", fileName, err)
			continue
		}
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("read file: %s err: %v\n", fileName, err)
				}
				break
			}
			ids := strings.Split(line, "|")
			if ids == nil || len(ids) <= 0 {
				continue
			}
			orderkey, er := strconv.ParseInt(ids[0], 10, 64)
			if er != nil {
				fmt.Printf("parse order key err: %v\n", er)
				continue
			}
			ch <- orderkey
		}
		file.Close()
	}
}

func OpenDB() (*sqlx.DB, error) {
	jdbcUrl := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?interpolateParams=true", USERNAME, PASSWORD, HOST, PORT, DB)
	db, err := sqlx.Connect("mysql", jdbcUrl)
	if err != nil {
		fmt.Printf("openDB err: %v\n", err)
		return nil, err
	}
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(64)
	db.SetConnMaxLifetime(time.Second * 10)
	pingErr := db.Ping()
	if pingErr != nil {
		fmt.Printf("db ping err: %s\n", pingErr)
		return nil, pingErr
	}
	return db, nil
}

func ExecSql(db *sqlx.DB, sql string, args ...interface{}) (int64, error) {
	r, err := db.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}
