package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal" // Пакет для обработки сигналов ОС
	"strconv"
	"syscall" // Пакет для системных вызовов (сигналов)
	"time"

	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// OKX API endpoint constants
const (
	OKX_API_BASE_URL = "https://www.okx.com"
	BOOKS_ENDPOINT   = "/api/v5/market/books"
	ENV_FILENAME     = ".env" // Имя файла с переменными окружения
)

// OrderBookResponse represents the overall structure of the API response
type OrderBookResponse struct {
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data []OrderBookData `json:"data"`
}

// OrderBookData represents the order book data for a single instrument
type OrderBookData struct {
	Asks [][]string `json:"asks"` // [[price, size, liquidated_orders, order_count]]
	Bids [][]string `json:"bids"` // [[price, size, liquidated_orders, order_count]]
	Ts   string     `json:"ts"`   // Timestamp in milliseconds
}

// PriceData represents the extracted price information
type PriceData struct {
	Timestamp time.Time
	BestBid   float64
	BestAsk   float64
}

// OkxPriceSnapshot is the GORM model struct mapping to the database table
type OkxPriceSnapshot struct {
	ID        int       `gorm:"primaryKey"`
	Timestamp time.Time `gorm:"column:timestamp;type:timestamp with time zone;not null"`
	BestBid   float64   `gorm:"column:best_bid;type:numeric(20,10);not null"`
	BestAsk   float64   `gorm:"column:best_ask;type:numeric(20,10);not null"`
	CreatedAt time.Time `gorm:"column:created_at;type:timestamp with time zone;default:CURRENT_TIMESTAMP"`
}

func (OkxPriceSnapshot) TableName() string {
	return "okx_price_snapshots"
}

// fetchOrderBook fetches order book data from OKX API
func fetchOrderBook(instrumentID string, depth int) (*OrderBookData, error) {
	url := fmt.Sprintf("%s%s?instId=%s&sz=%d", OKX_API_BASE_URL, BOOKS_ENDPOINT, instrumentID, depth)

	log.Printf("Выполнение HTTP GET запроса к %s", url) // Логируем URL запроса

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания HTTP запроса: %w", err)
	}
	req.Header.Set("User-Agent", "Go OKX API Client/1.0")

	client := &http.Client{Timeout: 10 * time.Second} // Устанавливаем таймаут
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения HTTP запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ошибка API: статус код %d (%s), тело ответа: %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения тела ответа: %w", err)
	}

	var apiResponse OrderBookResponse
	err = json.Unmarshal(bodyBytes, &apiResponse)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга JSON ответа: %w", err)
	}

	if apiResponse.Code != "0" {
		return nil, fmt.Errorf("ответ API содержит ошибку: код %s, сообщение: %s", apiResponse.Code, apiResponse.Msg)
	}

	if len(apiResponse.Data) == 0 {
		return nil, fmt.Errorf("ответ API не содержит данных книги ордеров")
	}

	return &apiResponse.Data[0], nil
}

// processOrderBookData processes order book data and extracts best bid/ask
func processOrderBookData(data *OrderBookData) (*PriceData, error) {
	if len(data.Bids) == 0 || len(data.Asks) == 0 {
		return nil, fmt.Errorf("книга ордеров пуста (нет предложений покупки или продажи)")
	}

	bestBidStr := data.Bids[0][0]
	bestBid, err := strconv.ParseFloat(bestBidStr, 64)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга лучшей цены покупки '%s': %w", bestBidStr, err)
	}

	bestAskStr := data.Asks[0][0]
	bestAsk, err := strconv.ParseFloat(bestAskStr, 64)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга лучшей цены продажи '%s': %w", bestAskStr, err)
	}

	tsInt, err := strconv.ParseInt(data.Ts, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга временной метки '%s': %w", data.Ts, err)
	}
	timestamp := time.Unix(0, tsInt*int64(time.Millisecond)).UTC()

	return &PriceData{
		Timestamp: timestamp,
		BestBid:   bestBid,
		BestAsk:   bestAsk,
	}, nil
}

// savePriceDataToPostgresGORM saves the price data to the PostgreSQL database using GORM
func savePriceDataToPostgresGORM(db *gorm.DB, data *PriceData) error {
	snapshot := OkxPriceSnapshot{
		Timestamp: data.Timestamp,
		BestBid:   data.BestBid,
		BestAsk:   data.BestAsk,
	}

	result := db.Create(&snapshot)

	if result.Error != nil {
		return fmt.Errorf("ошибка сохранения данных в БД через GORM: %w", result.Error)
	}

	log.Printf("Данные успешно сохранены в БД через GORM (ID: %d): Timestamp=%s, BestBid=%.4f, BestAsk=%.4f",
		snapshot.ID, data.Timestamp.Format(time.RFC3339), data.BestBid, data.BestAsk)

	return nil
}

func main() {
	instrument := "BTC-USDT"    // Укажите нужный инструмент
	depth := 5                  // Укажите нужную глубину стакана (количество уровней)
	interval := 1 * time.Minute // Интервал сбора данных

	// Загрузка переменных окружения из .env файла
	err := godotenv.Load(ENV_FILENAME)
	if err != nil {
		log.Printf("Внимание: Не удалось загрузить %s файл. Проверьте, что переменные окружения БД установлены вручную. Ошибка: %v", ENV_FILENAME, err)
		// В продакшене, возможно, лучше выйти с ошибкой, если .env обязателен:
		// log.Fatalf("Ошибка загрузки .env файла: %v", err)
	}

	// Получение параметров подключения из переменных окружения
	dbHost := os.Getenv("DB_HOST")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbPort := os.Getenv("DB_PORT")

	// Формирование строки подключения
	if dbHost == "" || dbUser == "" || dbPassword == "" || dbName == "" || dbPort == "" {
		log.Fatalf("Отсутствуют необходимые переменные окружения для подключения к БД. Проверьте .env файл или настройки окружения.")
	}

	// Используем формат DSN для GORM, он же подходит и для database/sql
	dbConnectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	// Подключение к базе данных с помощью GORM
	db, err := gorm.Open(postgres.Open(dbConnectionString), &gorm.Config{})
	if err != nil {
		log.Fatalf("Ошибка подключения к базе данных через GORM: %v", err)
	}
	log.Println("Успешное подключение к базе данных PostgreSQL через GORM.")

	// Опционально: Автоматическая миграция схемы таблицы (для разработки удобно)
	// log.Println("Выполнение автоматической миграции схемы БД...")
	// err = db.AutoMigrate(&OkxPriceSnapshot{})
	// if err != nil {
	// 	log.Fatalf("Ошибка выполнения автоматической миграции: %v", err)
	// }
	// log.Println("Автоматическая миграция завершена.")

	// Настройка тикера для запуска задачи каждую минуту
	ticker := time.NewTicker(interval)
	defer ticker.Stop() // Гарантируем остановку тикера при выходе

	// Канал для сигналов завершения
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM) // Ловим SIGINT (Ctrl+C) и SIGTERM (сигнал от systemd)

	log.Printf("Скрипт запущен и собирает данные для %s каждые %s...", instrument, interval)

	// Запуск первой задачи сразу при старте
	go func() {
		log.Println("Выполнение первой задачи немедленно...")
		collectAndSave(db, instrument, depth)
	}()

	// Основной цикл, который ждет тиков или сигнала завершения
	for {
		select {
		case <-ticker.C:
			// Срабатывает каждую минуту
			log.Println("Сработал тикер. Выполнение задачи...")
			collectAndSave(db, instrument, depth)
		case <-stop:
			// Получен сигнал завершения
			log.Println("Получен сигнал завершения. Остановка скрипта...")
			// Здесь можно добавить логику корректного завершения, например, закрытие соединений
			// Соединение с БД уже закроется через defer в main
			return // Завершаем функцию main и скрипт
		}
	}
}

// collectAndSave инкапсулирует логику получения и сохранения данных
func collectAndSave(db *gorm.DB, instrument string, depth int) {
	orderBookData, err := fetchOrderBook(instrument, depth)
	if err != nil {
		log.Printf("Ошибка в collectAndSave при получении книги ордеров: %v", err)
		return // Продолжаем работу, просто пропуская этот интервал
	}

	priceData, err := processOrderBookData(orderBookData)
	if err != nil {
		log.Printf("Ошибка в collectAndSave при обработке данных книги ордеров: %v", err)
		return // Продолжаем работу, просто пропуская этот интервал
	}

	// log.Printf("Получены данные: Timestamp=%s, BestBid=%.4f, BestAsk=%.4f", priceData.Timestamp.Format(time.RFC3339), priceData.BestBid, priceData.BestAsk)

	err = savePriceDataToPostgresGORM(db, priceData)
	if err != nil {
		log.Printf("Ошибка в collectAndSave при сохранении данных в PostgreSQL: %v", err)
		// Здесь можно добавить логику повторных попыток или уведомлений
	}
}
