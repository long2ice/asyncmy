package main

import (
	"context"
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type mixedRow struct {
	id, a, b  int64
	c         float64
	d, s1, s2 string
	dt, da    time.Time
	tm        time.Duration
}

func duration(raw string) time.Duration {
	negative := strings.HasPrefix(raw, "-")
	parts := strings.Split(strings.TrimPrefix(raw, "-"), ":")
	h, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		panic(err)
	}
	m, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		panic(err)
	}
	s, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		panic(err)
	}
	result := time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s*1e9)
	if negative {
		result = -result
	}
	return result
}
func measure(name string, fn func() any) {
	fn()
	samples := make([]float64, 0, 7)
	for i := 0; i < 7; i++ {
		start := time.Now()
		result := fn()
		elapsed := float64(time.Since(start)) / 1e6
		var sum int64
		switch rows := result.(type) {
		case [][8]int64:
			if len(rows) != 50000 {
				panic(len(rows))
			}
			for _, row := range rows {
				sum += row[0]
			}
		case []mixedRow:
			if len(rows) != 50000 {
				panic(len(rows))
			}
			for _, row := range rows {
				sum += row.a
			}
		case int64:
			sum = rows
		default:
			panic("unexpected result")
		}
		expected := int64(0)
		if strings.HasPrefix(name, "scan_") {
			expected = 1249975000
		}
		if sum != expected {
			panic(sum)
		}
		samples = append(samples, elapsed)
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	json.NewEncoder(os.Stdout).Encode(map[string]interface{}{"runtime": "go", "case": name, "median_ms": sorted[3], "samples_ms": samples})
}
func main() {
	runtime.GOMAXPROCS(1)
	db, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/test?parseTime=true&autocommit=true&interpolateParams=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	held := make([]*sql.Conn, 10)
	for i := range held {
		held[i], err = db.Conn(context.Background())
		if err != nil {
			panic(err)
		}
		if err = held[i].PingContext(context.Background()); err != nil {
			panic(err)
		}
	}
	for _, c := range held {
		c.Close()
	}
	for _, kind := range []string{"integer", "mixed"} {
		query := "SELECT a,b,a,b,a,b,a,b FROM test.bench"
		if kind == "mixed" {
			query = "SELECT * FROM test.bench"
		}
		for _, mode := range []string{"text", "binary"} {
			var stmt *sql.Stmt
			if mode == "binary" {
				stmt, err = db.Prepare(query)
				if err != nil {
					panic(err)
				}
			}
			measure("scan_"+kind+"_"+mode, func() any {
				var rows *sql.Rows
				var err error
				if stmt != nil {
					rows, err = stmt.Query()
				} else {
					rows, err = db.Query(query)
				}
				if err != nil {
					panic(err)
				}
				var data any
				if kind == "integer" {
					values := make([][8]int64, 0, 50000)
					for rows.Next() {
						var row [8]int64
						if err = rows.Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7]); err != nil {
							panic(err)
						}
						values = append(values, row)
					}
					data = values
				} else {
					values := make([]mixedRow, 0, 50000)
					for rows.Next() {
						var row mixedRow
						var tm string
						if err = rows.Scan(&row.id, &row.a, &row.b, &row.c, &row.d, &row.s1, &row.s2, &row.dt, &row.da, &tm); err != nil {
							panic(err)
						}
						row.tm = duration(tm)
						values = append(values, row)
					}
					data = values
				}
				if err = rows.Err(); err != nil {
					panic(err)
				}
				rows.Close()
				return data
			})
			if stmt != nil {
				stmt.Close()
			}
		}
	}
	for _, mode := range []string{"text", "binary"} {
		var stmt *sql.Stmt
		if mode == "binary" {
			stmt, err = db.Prepare("SELECT a FROM test.bench WHERE id=?")
			if err != nil {
				panic(err)
			}
		}
		measure("point_"+mode, func() any {
			sums := make([]int64, 20)
			var wg sync.WaitGroup
			for w := 0; w < 20; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for q := 0; q < 200; q++ {
						var a int64
						var err error
						if stmt != nil {
							err = stmt.QueryRow(1).Scan(&a)
						} else {
							err = db.QueryRow("SELECT a FROM test.bench WHERE id=?", 1).Scan(&a)
						}
						if err != nil {
							panic(err)
						}
						sums[w] += a
					}
				}(w)
			}
			wg.Wait()
			var sum int64
			for _, v := range sums {
				sum += v
			}
			return sum
		})
		if stmt != nil {
			stmt.Close()
		}
	}
}
