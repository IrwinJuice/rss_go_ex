package main

import (
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"time"

	"github.com/sijms/go-ora/v2"
	"log"
	"net/http"
	"os"
)

func main() {
	godotenv.Load(".env")

	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("PORT is not found in the env")
	}

	urlOptions := map[string]string{
		"SID": "FREE",
	}
	connStr := go_ora.BuildUrl("localhost", 1521, "", "sys", "passwd", urlOptions)
	conn, err := sql.Open("oracle", connStr)
	log.Println(connStr)

	if err != nil {

		log.Fatal("Can't connect to the database: ")
	}

	//oerr := createTable(conn)
	//if oerr != nil {
	//	log.Fatal("Can't create table " + err.Error())
	//}

	oerr := insertData(conn)
	if oerr != nil {
		log.Fatal("Can't insert data")
	}

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	e.GET("/", func(c echo.Context) error {

		err := queryData(conn)
		if err != nil {
			log.Fatal("Can't get data")
		}

		return c.String(http.StatusOK, "Hello, World!")
	})
	e.Logger.Fatal(e.Start(":" + port))

}
func createTable(conn *sql.DB) error {
	t := time.Now()
	sqlText := `CREATE TABLE GOORA_TEMP_VISIT(
	VISIT_ID	number(10)	NOT NULL,
	NAME		VARCHAR(200),
	VAL			number(10,2),
	VISIT_DATE	date,
	PRIMARY KEY(VISIT_ID)
	)`
	_, err := conn.Exec(sqlText)
	if err != nil {
		return err
	}
	fmt.Println("Finish create table GOORA_TEMP_VISIT :", time.Now().Sub(t))
	return nil
}
func insertData(conn *sql.DB) error {
	t := time.Now()
	index := 1
	stmt, err := conn.Prepare(`INSERT INTO GOORA_TEMP_VISIT(VISIT_ID, NAME, VAL, VISIT_DATE) 
VALUES(:1, :2, :3, :4)`)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close()
	}()
	val := 1.1
	for index = 1; index <= 100; index++ {
		log.Println("Inserting row: ", index)
		nameText := "Name " + fmt.Sprintf("%d", index)
		if index%5 == 0 {
			_, err = stmt.Exec(index, nameText, val, nil)
		} else {
			_, err = stmt.Exec(index, nameText, val, time.Now())
		}
		if err != nil {
			return err
		}
		val += 1.1
	}
	fmt.Println("100 rows inserted: ", time.Now().Sub(t))
	return nil
}

func queryData(conn *sql.DB) error {
	t := time.Now()
	rows, err := conn.Query("SELECT VISIT_ID, NAME, VAL, VISIT_DATE FROM GOORA_TEMP_VISIT")
	if err != nil {
		return err
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			fmt.Println("Can't close dataset: ", err)
		}
	}()
	var (
		id   int64
		name string
		val  float32
		date sql.NullTime
	)
	for rows.Next() {
		err = rows.Scan(&id, &name, &val, &date)
		if err != nil {
			return err
		}
		fmt.Println("ID: ", id, "\tName: ", name, "\tval: ", val, "\tDate: ", date)
	}
	fmt.Println("Finish query rows: ", time.Now().Sub(t))
	return nil
}
