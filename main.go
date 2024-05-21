package main

import (
	"database/sql"
	"fmt"
	"log"
	_ "os"
	"strconv"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	var db *sql.DB = iniciar_conexion_base_de_datos()
	iniciar_conexion_mqtt(db)
	for {

	}
}

func iniciar_conexion_base_de_datos() *sql.DB {
	db, err := sql.Open("mysql", "esp:1234@tcp(localhost:3306)/db_lampara_lava")

	if err != nil {
		panic(err.Error())
	}

	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(20)

	if err := db.Ping(); err != nil {
		log.Fatalln(err)
	} else {
		fmt.Println("Conexion con base de datos establecida")
	}

	return db
}

func iniciar_conexion_mqtt(db *sql.DB) {

	var client mqtt.Client

	var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())

		string_payload := string(msg.Payload()[:])

		if msg.Topic() == "esp32/temperature" {
			temp, err := strconv.ParseFloat(string_payload, 32)
			if err != nil {
				log.Fatal(err)
				return
			}
			insertTemperatura(db, float32(temp))
			minMaxTemp := getTemperatures(db)
			fmt.Printf("min = %v, max = %v\n", minMaxTemp[0], minMaxTemp[1])
			factor := (float32(temp) - minMaxTemp[0]) / (minMaxTemp[1] - minMaxTemp[0])
			publishCooldown(client, factor)
		} else if msg.Topic() == "esp32/humidity" {
			humidity, err := strconv.ParseFloat(string_payload, 32)
			if err != nil {
				log.Fatal(err)
				return
			}
			insertHumedad(db, float32(humidity))
			minMaxHum := getHumidities(db)
			fmt.Printf("min = %v, max = %v\n", minMaxHum[0], minMaxHum[1])

			factor := (float32(humidity) - minMaxHum[0]) / (minMaxHum[1] - minMaxHum[0])
			publishColor(client, factor)
		}
	}
	var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
		fmt.Println("Connected")
	}
	var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
		fmt.Printf("Connect lost: %v", err)
	}

	var broker = "localhost"
	var port = 1883

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client = mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	sub(client)
	//publish(client)

}

func publishCooldown(client mqtt.Client, cooldownFactor float32) {

	text := fmt.Sprintf("%v", cooldownFactor)
	token := client.Publish("server/cooldown", 0, false, text)
	token.Wait()

}

func publishColor(client mqtt.Client, color float32) {

	text := fmt.Sprintf("%v", color)
	token := client.Publish("server/color", 0, false, text)
	token.Wait()

}

func sub(client mqtt.Client) {
	topic := "esp32/temperature"
	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	fmt.Println("Servidor se ha suscrito al topico:", topic)

	topic = "esp32/humidity"
	token = client.Subscribe(topic, 1, nil)
	token.Wait()
	fmt.Println("Servidor se ha suscrito al topico:", topic)
}

func getTemperatures(db *sql.DB) []float32 {

	rows, err := db.Query("select MIN(temperatura), MAX(temperatura) from temperatura where tiempo_registro>=subdate(current_date, 1);")

	if err != nil {
		panic(err.Error())
	}

	defer rows.Close()

	var minTemp, maxTemp float32
	if rows.Next() {
		err = rows.Scan(&minTemp, &maxTemp)
		if err != nil {
			log.Fatal(err)
		}
	}
	return []float32{minTemp, maxTemp}
}

func getHumidities(db *sql.DB) []float32 {

	rows, err := db.Query("select MIN(humedad), MAX(humedad) from humedad where tiempo_registro>=subdate(current_date, 1);")

	if err != nil {
		panic(err.Error())
	}

	defer rows.Close()

	var minHum, maxHum float32
	if rows.Next() {
		err = rows.Scan(&minHum, &maxHum)
		if err != nil {
			log.Fatal(err)
		}
	}
	return []float32{minHum, maxHum}
}

func insertTemperatura(db *sql.DB, temperatura float32) {
	statement, err := db.Prepare("insert into temperatura(temperatura, tiempo_registro) values(?, NOW());")
	if err != nil {
		log.Fatal(err.Error())
	}
	_, err = statement.Exec(temperatura)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func insertHumedad(db *sql.DB, humedad float32) {
	statement, err := db.Prepare("insert into humedad(humedad, tiempo_registro) values(?, NOW());")
	if err != nil {
		log.Fatal(err.Error())
	}
	_, err = statement.Exec(humedad)
	if err != nil {
		log.Fatal(err.Error())
	}
}
