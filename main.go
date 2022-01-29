package main

import (
	"bufio"
	"compress/bzip2"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mikioh/ipaddr"
	"github.com/schollz/progressbar/v3"
)

const (
	dataURI = "http://archive.routeviews.org/oix-route-views/oix-full-snapshot-latest.dat.bz2"
	fileOIX = "oix-full-snapshot-latest.dat.bz2"
	host    = "sql11.freemysqlhosting.net"
	dba     = "sql11468981"
	user    = "sql11468981"
	pass    = "wXdF5vMxGj"
)

func main() {
	if !fileExists(fileOIX) {
		err := downloadOIX()
		if err != nil {
			log.Fatal(err)
		}
	}

	// open the file
	f, err := os.OpenFile(fileOIX, 0, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	br := bufio.NewReader(f)
	cr := bzip2.NewReader(br)
	fmt.Println("Now we parsing a BIG file... Searching AS: 25537, 39494, 48287, 5537")
	fmt.Println("Please be patient!")
	pasefileRes, err := parsefile(cr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Now we generalize networks... ")
	r := generalize(pasefileRes)
	fmt.Println(r)

	fmt.Println("OK now, let's save our results to Mysql DB!")
	fmt.Println("We will use free online Database.")
	fmt.Println("HostName: sql11.freemysqlhosting.net")
	fmt.Println("Database Name: sql11468981")
	fmt.Println("Database Username: sql11468981")
	fmt.Println("Password: wXdF5vMxGj")
	db, err := dbConnection()
	if err != nil {
		log.Printf("Error %s when getting db connection", err)
		return
	}
	defer db.Close()
	log.Printf("Successfully connected to database")
	err = createNetworkTable(db)
	if err != nil {
		log.Printf("Create networks table failed with error %s", err)
		return
	}

	for _, i := range r {
		err = insert(db, i.String())
		if err != nil {
			log.Printf("Insert network failed with error %s", err)
			return
		}

	}
}

/***** Функции *****/
// Загрузка файла для парсинга.
func downloadOIX() error {
	// Загружаем свежую таблицу BGP.
	req, err := http.NewRequest("GET", dataURI, nil)
	if err != nil {
		log.Println(err)
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}
	defer resp.Body.Close()
	f, err := os.OpenFile("oix-full-snapshot-latest.dat.bz2", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Println(err)
		return err
	}
	defer f.Close()
	// Используем сторонний прогресс бар, чтобы не реализовывать свой и был понятен прогресс скачивания.
	bar := progressbar.DefaultBytes(
		resp.ContentLength,
		"downloading",
	)
	_, err = io.Copy(io.MultiWriter(f, bar), resp.Body)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

// Проверка существует ли файл
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// Парсинг файла
func parsefile(reader io.Reader) ([]string, error) {
	result := make([]string, 0, 30)
	unic := make(map[string]struct{})
	sc := bufio.NewScanner(reader)
	for sc.Scan() {
		row := sc.Text()
		if strings.Contains(row, " 25537 ") || strings.Contains(row, " 39494 ") ||
			strings.Contains(row, " 48287 ") || strings.Contains(row, " 5537 ") {

			sl := strings.Split(row, " ")
			if _, ok := unic[sl[2]]; !ok {
				result = append(result, sl[2])
				unic[sl[2]] = struct{}{}
			}

		}

	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result, nil
}

// Генерализация подсетей
func generalize(netList []string) (genNetList []*ipaddr.Prefix) {
	flag := false
	blackList := make([]int, 0, 20)
	prefSlice := make([]*ipaddr.Prefix, 0, 60)
	resultSlice := make([]*ipaddr.Prefix, 0, 30)

	for _, ip := range netList {
		_, ipNet, err := net.ParseCIDR(ip)
		if err != nil {
			log.Fatal(err)
		}
		pref := ipaddr.NewPrefix(ipNet)
		prefSlice = append(prefSlice, pref)
	}
	sort.SliceStable(prefSlice, func(i, j int) bool {
		return prefSlice[i].Mask.String() < prefSlice[j].Mask.String()
	})

	for k := range prefSlice {
		flag = false

	LN:
		for z := k; z < len(prefSlice); z++ {
			for _, vl := range blackList {
				if z == vl {
					break LN
				}
			}
			if prefSlice[k].Contains(prefSlice[z]) {
				blackList = append(blackList, z)
				if !flag {
					flag = true
				}
			}

		}

		if flag {
			resultSlice = append(resultSlice, prefSlice[k])
		}

	}
	return resultSlice
}

// MYSQL
// Соединение с БД
func dbConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn(""))
	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		return nil, err
	}
	// defer db.Close()

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dba)
	if err != nil {
		log.Printf("Error %s when creating DB\n", err)
		return nil, err
	}
	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return nil, err
	}
	log.Printf("rows affected %d\n", no)

	db.Close()
	db, err = sql.Open("mysql", dsn(dba))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return nil, err
	}
	// defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)

	ctx, cancelfunc = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = db.PingContext(ctx)
	if err != nil {
		log.Printf("Errors %s pinging DB", err)
		return nil, err
	}
	log.Printf("Connected to DB %s successfully\n", db)
	return db, nil
}

// Создание таблицы
func createNetworkTable(db *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS networks(id int primary key auto_increment, networks text, 
        created_at TIMESTAMP default CURRENT_TIMESTAMP)`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when creating networks table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when getting rows affected", err)
		return err
	}
	log.Printf("Rows affected when creating table: %d", rows)
	return nil
}

func dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, host, dbName)
}

// Вставка строк в БД
func insert(db *sql.DB, n string) error {
	query := "INSERT INTO networks(networks) VALUES (?)"
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, n)
	if err != nil {
		log.Printf("Error %s when inserting row into networks table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d networks created ", rows)
	return nil
}
