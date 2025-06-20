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

// Структура для конфигурации отслеживаемых инструментов
type InstrumentConfig struct {
	ApiID     string // Идентификатор для OKX API (например, "BTC-USDT")
	TableName string // Имя таблицы в БД (например, "okx_prices_BTC")
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

// saveOrderBookLevelsGORM saves multiple order book levels to the specified table using GORM
func saveOrderBookLevelsGORM(db *gorm.DB, levels []OrderBookLevel, tableName string) error {
	if len(levels) == 0 {
		log.Printf("[%s] Нет уровней книги ордеров для сохранения.", tableName)
		return nil // Ничего не сохраняем, это не ошибка
	}

	// Указываем таблицу динамически
	result := db.Table(tableName).Create(&levels) // Используем db.Table(tableName)

	if result.Error != nil {
		return fmt.Errorf("[%s] ошибка массового сохранения данных в БД через GORM: %w", tableName, result.Error)
	}

	log.Printf("[%s] Успешно сохранено %d уровней книги ордеров в БД", tableName, len(levels))
	return nil
}

// collectAndSave инкапсулирует логику получения, обработки и сохранения данных для указанного инструмента и таблицы
func collectAndSave(db *gorm.DB, instrumentApiID string, depth int, tableName string) {
	log.Printf("[%s] Начинаем сбор данных...", instrumentApiID)
	orderBookData, err := fetchOrderBook(instrumentApiID, depth)
	if err != nil {
		log.Printf("[%s] Ошибка в collectAndSave при получении книги ордеров: %v", instrumentApiID, err)
		return
	}

	levels, err := extractOrderBookLevels(orderBookData, depth)
	if err != nil {
		log.Printf("[%s] Ошибка в collectAndSave при обработке данных книги ордеров: %v", instrumentApiID, err)
		return
	}

	// Сохраняем все полученные уровни в указанную таблицу
	err = saveOrderBookLevelsGORM(db, levels, tableName)
	if err != nil {
		log.Printf("[%s -> %s] Ошибка в collectAndSave при сохранении данных: %v", instrumentApiID, tableName, err)
	}
}

func main() {
	// Список инструментов для отслеживания
	instrumentsToMonitor := []InstrumentConfig{
		{ApiID: "BTC-USDT", TableName: "okx_prices_btc"},
		{ApiID: "ETH-USDT", TableName: "okx_prices_eth"},
		{ApiID: "SOL-USDT", TableName: "okx_prices_sol"},
		{ApiID: "TON-USDT", TableName: "okx_prices_ton"},
		// Добавьте сюда другие инструменты при необходимости
	}

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

	//Передаем креды БД в явном виде (нужно будет убрать спрятать их в переменные окружения)
	dbHost := "dpg-d1aibube5dus73ekloj0-a.frankfurt-postgres.render.com"
	dbUser := "admin"
	dbPassword := "p4mTZrzf9XZOX6KcQaRetlhPjgwK0juM"
	dbName := "hft_db_4cxe"
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

	// --- Автоматическая миграция (если нужна) ---
	// Теперь миграцию нужно делать для каждой таблицы
	runAutoMigration := true // Установите true, если хотите выполнить миграцию при запуске
	if runAutoMigration {
		log.Println("Выполнение автоматической миграции схем БД...")
		for _, config := range instrumentsToMonitor {
			log.Printf("Миграция для таблицы %s...", config.TableName)
			// Указываем GORM, для какой таблицы выполнить миграцию и какую структуру использовать
			err := db.Table(config.TableName).AutoMigrate(&OrderBookLevel{})
			if err != nil {
				log.Fatalf("Ошибка выполнения автоматической миграции для таблицы %s: %v", config.TableName, err)
			}
			log.Printf("Автоматическая миграция для таблицы %s завершена.", config.TableName)
		}
		log.Println("Все автоматические миграции завершены.")
	}

	// Основная логика (с циклом по инструментам)
	// Настройка тикера для запуска задачи каждую минуту
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	log.Printf("Скрипт запущен. Интервал сбора данных: %s", interval)

	// Первоначальный сбор данных для всех инструментов
	log.Println("Выполнение первоначального сбора данных для всех инструментов...")
	for _, config := range instrumentsToMonitor {
		go collectAndSave(db, config.ApiID, depth, config.TableName) // Запускаем в горутинах, чтобы не блокировать
	}

	// Основной цикл
	for {
		select {
		case <-ticker.C:
			log.Println("Сработал тикер. Выполнение задач для всех инструментов...")
			for _, config := range instrumentsToMonitor {
				// Можно запускать в горутинах, если сбор данных для одного инструмента не должен блокировать другие
				// и если API не будет вас блокировать за частые параллельные запросы.
				// Для последовательного сбора:
				// collectAndSave(db, config.ApiID, depth, config.TableName)
				// Для параллельного сбора:
				go collectAndSave(db, config.ApiID, depth, config.TableName)
			}
		case <-stop:
			log.Println("Получен сигнал завершения. Остановка скрипта...")
			return
		}
	}
}
