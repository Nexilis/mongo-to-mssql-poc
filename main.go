package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoConnectionString = "mongodb://localhost:27017"
	mongoDbName           = "CitiesService"
	mongoCollectionName   = "cities"
	serviceAddress        = ":12345"
	mssqlServer           = "localhost"
	mssqlPort             = 1433
	mssqlUser             = "sa"
	mssqlPassword         = ""
	mssqlDatabase         = "CitiesService"
)

var client *mongo.Client
var db *sql.DB

type mongoCity struct {
	ID      primitive.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	City    string             `json:"city,omitempty" bson:"city,omitempty"`
	Country string             `json:"country,omitempty" bson:"country,omitempty"`
}

type mssqlCity struct {
	ID      int
	Name    string
	Country string
}

// Connect to MongoDB => https://github.com/mongodb/mongo-go-driver
// Connect to MSSQL   => https://docs.microsoft.com/en-us/azure/sql-database/sql-database-connect-query-go
func main() {
	fmt.Println("Service started... waiting for calls...")

	configureMongo()
	configureSQL()

	router := mux.NewRouter()
	router.HandleFunc("/mongo/cities", getCitiesMongo).Methods("GET")
	router.HandleFunc("/mssql/cities", getCitiesMssql).Methods("GET")
	router.HandleFunc("/mssql/cities", createCitiesMssql).Methods("POST")
	log.Fatal(http.ListenAndServe(serviceAddress, router))
}

func configureMongo() {
	var ctx, cancelFunc = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	client, _ = mongo.Connect(ctx, options.Client().ApplyURI(mongoConnectionString))
}

func configureSQL() {
	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		mssqlServer, mssqlUser, mssqlPassword, mssqlPort, mssqlDatabase)

	var err error

	// Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Printf("Connected!\n")
}

func getCitiesMongo(response http.ResponseWriter, request *http.Request) {
	log.Println("Cities mongo request")
	response.Header().Set("content-type", "application/json")
	var cities []mongoCity
	collection := client.Database(mongoDbName).Collection(mongoCollectionName)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
		return
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var city mongoCity
		cursor.Decode(&city)
		cities = append(cities, city)
	}
	if err := cursor.Err(); err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
		return
	}
	json.NewEncoder(response).Encode(cities)
}

func createCitiesMssql(response http.ResponseWriter, request *http.Request) {
	name := "Warsaw"
	country := "Poland"

	ctx := context.Background()
	var err error
	if db == nil {
		err = errors.New("CreateEmployee: db is null")
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
		return
	}

	// Check if database is alive.
	err = db.PingContext(ctx)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
		return
	}

	tsql := "INSERT INTO Cities (Name, Country) VALUES (@Name, @Country); select convert(bigint, SCOPE_IDENTITY());"

	stmt, err := db.Prepare(tsql)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
		return
	}
	defer stmt.Close()

	row := stmt.QueryRowContext(
		ctx,
		sql.Named("Name", name),
		sql.Named("Country", country))
	var newID int64
	err = row.Scan(&newID)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
		return
	}
	json.NewEncoder(response).Encode(newID)
}

func getCitiesMssql(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("content-type", "application/json")
	log.Println("Cities mssql request")
	ctx := context.Background()

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
		return
	}

	tsql := fmt.Sprintf("SELECT Id, Name, Country FROM Cities;")

	// Execute query
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
		return
	}

	defer rows.Close()

	var cities []mssqlCity
	for rows.Next() {
		var id int
		var name, country string
		err := rows.Scan(&id, &name, &country)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			response.Write([]byte(`{ "message": "` + err.Error() + `" }`))
			return
		}
		var city = mssqlCity{id, name, country}
		cities = append(cities, city)
	}
	json.NewEncoder(response).Encode(cities)
}
