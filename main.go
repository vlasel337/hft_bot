package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

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

// OrderBookLevel represents a single price level to be stored in the database
type OrderBookLevel struct {
	ID                int       `gorm:"primaryKey"`
	SnapshotTimestamp time.Time `gorm:"column:snapshot_timestamp;type:timestamp with time zone;not null"` // Временная метка всего снимка
	Price             float64   `gorm:"column:price;type:numeric(20,10);not null"`
	Size              float64   `gorm:"column:size;type:numeric(20,10);not null"`
	Type              string    `gorm:"column:type;type:varchar(4);not null"` // "bid" or "ask"
	Level             int       `gorm:"column:level;type:integer;not null"`   // Порядковый уровень
	RecordedAt        time.Time `gorm:"column:recorded_at;type:timestamp with time zone;default:CURRENT_TIMESTAMP"`
}

// Указываем GORM имя таблицы
func (OrderBookLevel) TableName() string {
	return "okx_price_levels" // Новое имя таблицы
}

// fetchOrderBook fetches order book data from OKX API
func fetchOrderBook(instrumentID string, depth int) (*OrderBookData, error) {
	url := fmt.Sprintf("%s%s?instId=%s&sz=%d", OKX_API_BASE_URL, BOOKS_ENDPOINT, instrumentID, depth)

	log.Printf("Выполнение HTTP GET запроса к %s", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания HTTP запроса: %w", err)
	}
	req.Header.Set("User-Agent", "Go OKX API Client/1.0")

	client := &http.Client{Timeout: 10 * time.Second}
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

// extractOrderBookLevels processes order book data and extracts specified depth levels
func extractOrderBookLevels(data *OrderBookData, depth int) ([]OrderBookLevel, error) {
	var levels []OrderBookLevel

	// Parse snapshot timestamp once for all levels from this snapshot
	tsInt, err := strconv.ParseInt(data.Ts, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга временной метки '%s': %w", data.Ts, err)
	}
	snapshotTimestamp := time.Unix(0, tsInt*int64(time.Millisecond)).UTC()

	// Process Bids (up to 'depth' levels)
	for i := 0; i < len(data.Bids) && i < depth; i++ {
		bid := data.Bids[i]
		// OKX API returns [price, size, liquidated_orders, order_count]
		if len(bid) < 2 {
			log.Printf("Предупреждение: пропущен bid ордер на уровне %d из-за неполных данных: %v", i+1, bid)
			continue // Пропускаем неполные данные, но продолжаем с другими уровнями
		}
		priceStr := bid[0]
		sizeStr := bid[1]

		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Printf("Предупреждение: ошибка парсинга цены bid на уровне %d ('%s'): %v", i+1, priceStr, err)
			continue // Пропускаем этот уровень из-за ошибки парсинга цены
		}
		size, err := strconv.ParseFloat(sizeStr, 64)
		if err != nil {
			log.Printf("Предупреждение: ошибка парсинга размера bid на уровне %d ('%s'): %v", i+1, sizeStr, err)
			continue // Пропускаем этот уровень из-за ошибки парсинга размера
		}

		levels = append(levels, OrderBookLevel{
			SnapshotTimestamp: snapshotTimestamp,
			Price:             price,
			Size:              size,
			Type:              "bid",
			Level:             i + 1, // Уровень начинается с 1
		})
	}

	// Process Asks (up to 'depth' levels)
	for i := 0; i < len(data.Asks) && i < depth; i++ {
		ask := data.Asks[i]
		// OKX API returns [price, size, liquidated_orders, order_count]
		if len(ask) < 2 {
			log.Printf("Предупреждение: пропущен ask ордер на уровне %d из-за неполных данных: %v", i+1, ask)
			continue // Пропускаем неполные данные
		}
		priceStr := ask[0]
		sizeStr := ask[1]

		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Printf("Предупреждение: ошибка парсинга цены ask на уровне %d ('%s'): %v", i+1, priceStr, err)
			continue // Пропускаем этот уровень
		}
		size, err := strconv.ParseFloat(sizeStr, 64)
		if err != nil {
			log.Printf("Предупреждение: ошибка парсинга размера ask на уровне %d ('%s'): %v", i+1, sizeStr, err)
			continue // Пропускаем этот уровень
		}

		levels = append(levels, OrderBookLevel{
			SnapshotTimestamp: snapshotTimestamp,
			Price:             price,
			Size:              size,
			Type:              "ask",
			Level:             i + 1, // Уровень начинается с 1
		})
	}

	if len(levels) == 0 {
		// Возможно, книга ордеров была получена, но не содержала валидных уровней нужной глубины
		return nil, fmt.Errorf("не удалось извлечь %d уровней книги ордеров (bid/ask) из ответа", depth)
	}

	log.Printf("Извлечено %d уровней книги ордеров (depth=%d)", len(levels), depth)

	return levels, nil
}

// saveOrderBookLevelsGORM saves multiple order book levels to the database using GORM
func saveOrderBookLevelsGORM(db *gorm.DB, levels []OrderBookLevel) error {
	if len(levels) == 0 {
		log.Println("Нет уровней книги ордеров для сохранения.")
		return nil // Ничего не сохраняем, это не ошибка
	}

	// GORM поддерживает массовую вставку (bulk insert) при передаче среза структур
	result := db.Create(&levels)

	if result.Error != nil {
		return fmt.Errorf("ошибка массового сохранения данных в БД через GORM: %w", result.Error)
	}

	log.Printf("Успешно сохранено %d уровней книги ордеров в БД", len(levels))

	return nil
}

// collectAndSave инкапсулирует логику получения, обработки и сохранения данных
func collectAndSave(db *gorm.DB, instrument string, depth int) {
	orderBookData, err := fetchOrderBook(instrument, depth)
	if err != nil {
		log.Printf("Ошибка в collectAndSave при получении книги ордеров: %v", err)
		return // Продолжаем работу
	}

	// Извлекаем все необходимые уровни
	levels, err := extractOrderBookLevels(orderBookData, depth)
	if err != nil {
		log.Printf("Ошибка в collectAndSave при обработке данных книги ордеров: %v", err)
		return // Продолжаем работу
	}

	// Сохраняем все полученные уровни в таблицу PostgreSQL с помощью GORM
	err = saveOrderBookLevelsGORM(db, levels)
	if err != nil {
		log.Printf("Ошибка в collectAndSave при сохранении данных в PostgreSQL: %v", err)
		// Здесь можно добавить логику повторных попыток или уведомлений
	}
}

// healthCheckHandler отвечает на запросы проверки состояния
// func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
// 	// Устанавливаем статус 200 OK
// 	w.WriteHeader(http.StatusOK)
// 	// Отправляем простое тело ответа (необязательно, но полезно для отладки)
// 	fmt.Fprintln(w, "OK")
// 	log.Println("Health check request processed successfully.") // Логируем успешный health check
// }

func main() {
	instrument := "BTC-USDT"    // Укажите нужный инструмент
	depth := 5                  // *** Укажите, сколько уровней с КАЖДОЙ стороны вы хотите получить (например, 5 даст 5 bid + 5 ask, 10 даст 10 bid + 10 ask) ***
	interval := 1 * time.Minute // Интервал сбора данных

	// Загрузка переменных окружения из .env файла
	// err := godotenv.Load(ENV_FILENAME)
	// if err != nil {
	// 	log.Printf("Внимание: Не удалось загрузить %s файл. Проверьте, что переменные окружения БД установлены вручную. Ошибка: %v", ENV_FILENAME, err)
	// 	// В продакшене, возможно, лучше выйти с ошибкой, если .env обязателен:
	// 	// log.Fatalf("Ошибка загрузки .env файла: %v", err)
	// }

	// Получение параметров подключения из переменных окружения
	// dbHost := os.Getenv("DB_HOST")
	// dbUser := os.Getenv("DB_USER")
	// dbPassword := os.Getenv("DB_PASSWORD")
	// dbName := os.Getenv("DB_NAME")
	// dbPort := os.Getenv("DB_PORT")

	// Определяем порт для health check сервера
	// Fly.io обычно устанавливает переменную окружения PORT
	// port := os.Getenv("PORT")
	// if port == "" {
	// 	port = "8080" // Порт по умолчанию, если PORT не установлен
	// }
	// listenAddr := ":" + port // Формат для ListenAndServe, например ":8080"

	// // Регистрируем обработчик для пути /health
	// http.HandleFunc("/health", healthCheckHandler)

	// // Запускаем HTTP-сервер в отдельной горутине
	// go func() {
	// 	log.Printf("Health check server starting to listen on %s", listenAddr)
	// 	// ListenAndServe блокирует выполнение, пока сервер работает или не возникнет ошибка
	// 	if err := http.ListenAndServe(listenAddr, nil); err != nil {
	// 		// Логируем ошибку, если сервер не смог запуститься
	// 		// Не используем log.Fatalf, чтобы не остановить основное приложение
	// 		log.Printf("ERROR: Health check server failed: %v", err)
	// 	}
	// }()

	//Передаем креды БД в явном виде (нужно будет убрать спрятать их в переменные окружения)
	dbHost := "dpg-d05l1pq4d50c73f4qqfg-a.frankfurt-postgres.render.com"
	dbUser := "admin"
	dbPassword := "958G9FNfWvQGUvfmy3oiiftd5MjAu0OD"
	dbName := "hft_0yd2"
	dbPort := "5432"

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

	// !!! ВНИМАНИЕ: Если вы используете новую таблицу okx_price_levels,
	//  закомментируйте старую миграцию и раскомментируйте эту, если нужна авто-миграция
	// Опционально: Автоматическая миграция схемы новой таблицы
	// log.Println("Выполнение автоматической миграции схемы БД для okx_price_levels...")
	// err = db.AutoMigrate(&OrderBookLevel{})
	// if err != nil {
	// 	log.Fatalf("Ошибка выполнения автоматической миграции: %v", err)
	// }
	// log.Println("Автоматическая миграция завершена.")

	// Настройка тикера для запуска задачи каждую минуту
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Канал для сигналов завершения
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	log.Printf("Скрипт запущен и собирает данные для %s каждые %s...", instrument, interval)

	// Запуск первой задачи сразу при старте
	go func() {
		log.Println("Выполнение первой задачи немедленно...")
		collectAndSave(db, instrument, depth) // Используем обновленную функцию collectAndSave
	}()

	// Основной цикл, который ждет тиков или сигнала завершения
	for {
		select {
		case <-ticker.C:
			log.Println("Сработал тикер. Выполнение задачи...")
			collectAndSave(db, instrument, depth) // Используем обновленную функцию collectAndSave
		case <-stop:
			log.Println("Получен сигнал завершения. Остановка скрипта...")
			return // Выход из функции main
		}
	}
}
