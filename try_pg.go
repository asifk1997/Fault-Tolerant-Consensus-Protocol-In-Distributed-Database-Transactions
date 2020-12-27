package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "go1"
)

var globalDb *sql.DB
func main(){
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}
	globalDb=db
	fmt.Println("Successfully connected!")
	sqlStatement := `UPDATE "Transactions" set "Value" = "Value" + ($1) 
    			where "Transactions"."Index" = 1`
	_, err = globalDb.Exec(sqlStatement, -10)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("\nRow Updated successfully!")
	}
}