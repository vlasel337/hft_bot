package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Определение структуры для таблицы crypto_prices (должна соответствовать вашей таблице)
type CryptoPrice struct {
	ID          uint      `gorm:"primaryKey"`
	Ticker      string    `gorm:"unique;not null"`
	Price       float64   `gorm:"type:decimal(18,8);not null"`
	LastUpdated time.Time `gorm:"type:timestamp with time zone;default:CURRENT_TIMESTAMP"`
}

func main() {
	// Загрузка переменных окружения из файла .env
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Ошибка при загрузке файла .env: %v", err)
	}

	// Получение параметров подключения из переменных окружения
	host := os.Getenv("DB_HOST")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbname := os.Getenv("DB_NAME")
	port := os.Getenv("DB_PORT")

	// Строка подключения к PostgreSQL
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=require", host, user, password, dbname, port)

	// Подключение к базе данных PostgreSQL с помощью GORM
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Не удалось подключиться к базе данных: %v", err)
	}

	fmt.Println("Успешно подключено к базе данных PostgreSQL!")

	// Далее вы можете выполнять операции с базой данных, используя объект 'db'

	// Например, автоматическая миграция схемы (создание таблицы, если ее нет)
	err = db.AutoMigrate(&CryptoPrice{})
	if err != nil {
		log.Fatalf("Не удалось выполнить миграцию схемы: %v", err)
	}
	fmt.Println("Миграция схемы выполнена.")

	// Пример чтения данных (получение всех записей)
	var prices []CryptoPrice
	result := db.Find(&prices)
	if result.Error != nil {
		log.Printf("Ошибка при чтении данных: %v", result.Error)
	} else {
		fmt.Printf("Найдено %d записей:\n", result.RowsAffected)
		for _, price := range prices {
			fmt.Printf("ID: %d, Ticker: %s, Price: %f, Last Updated: %v\n", price.ID, price.Ticker, price.Price, price.LastUpdated)
		}
	}
}
