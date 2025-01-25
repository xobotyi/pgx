package pgx_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Product struct {
	ID          int32     `db:"id"`
	Name        string    `db:"name"`
	Description string    `db:"description"`
	Price       float64   `db:"price"`
	Stock       int32     `db:"stock"`
	Category    string    `db:"category"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
	IsActive    bool      `db:"is_active"`
	SKU         string    `db:"sku"`
	Weight      float64   `db:"weight"`
	Dimensions  string    `db:"dimensions"`
	Color       string    `db:"color"`
	Brand       string    `db:"brand"`
	Rating      float64   `db:"rating"`
	Reviews     int32     `db:"reviews"`
	Discount    float64   `db:"discount"`
	Tags        []string  `db:"tags"`
	SupplierID  int32     `db:"supplier_id"`
	WarehouseID int32     `db:"warehouse_id"`
}

// MockRows implements pgx.Rows interface
type MockRows struct {
	currentRow int
	rows       []Product
}

// NewMockRows creates a new MockRows instance with predefined data
func NewMockRows(products []Product) *MockRows {
	return &MockRows{
		currentRow: -1,
		rows:       products,
	}
}

// Close implements the pgx.Rows Close method
func (m *MockRows) Close() {
	// No-op for mock
}

// Err implements the pgx.Rows Err method
func (m *MockRows) Err() error {
	return nil
}

// CommandTag implements the pgx.Rows CommandTag method
func (m *MockRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

// FieldDescriptions implements the pgx.Rows FieldDescriptions method
func (m *MockRows) FieldDescriptions() []pgconn.FieldDescription {
	// Return mock field descriptions matching your struct
	return []pgconn.FieldDescription{
		{Name: "id"},
		{Name: "name"},
		{Name: "description"},
		{Name: "price"},
		{Name: "stock"},
		{Name: "category"},
		{Name: "created_at"},
		{Name: "updated_at"},
		{Name: "is_active"},
		{Name: "sku"},
		{Name: "weight"},
		{Name: "dimensions"},
		{Name: "color"},
		{Name: "brand"},
		{Name: "rating"},
		{Name: "reviews"},
		{Name: "discount"},
		{Name: "tags"},
		{Name: "supplier_id"},
		{Name: "warehouse_id"},
	}
}

// Next implements the pgx.Rows Next method
func (m *MockRows) Next() bool {
	m.currentRow++
	return m.currentRow < len(m.rows)
}

// Scan implements the pgx.Rows Scan method
func (m *MockRows) Scan(dest ...interface{}) error {
	if m.currentRow >= len(m.rows) {
		return pgx.ErrNoRows
	}

	row := m.rows[m.currentRow]

	// Scan values into destination pointers
	*dest[0].(*int32) = row.ID
	*dest[1].(*string) = row.Name
	*dest[2].(*string) = row.Description
	*dest[3].(*float64) = row.Price
	*dest[4].(*int32) = row.Stock
	*dest[5].(*string) = row.Category
	*dest[6].(*time.Time) = row.CreatedAt
	*dest[7].(*time.Time) = row.UpdatedAt
	*dest[8].(*bool) = row.IsActive
	*dest[9].(*string) = row.SKU
	*dest[10].(*float64) = row.Weight
	*dest[11].(*string) = row.Dimensions
	*dest[12].(*string) = row.Color
	*dest[13].(*string) = row.Brand
	*dest[14].(*float64) = row.Rating
	*dest[15].(*int32) = row.Reviews
	*dest[16].(*float64) = row.Discount
	*dest[17].(*[]string) = row.Tags
	*dest[18].(*int32) = row.SupplierID
	*dest[19].(*int32) = row.WarehouseID

	return nil
}

// Values implements the pgx.Rows Values method
func (m *MockRows) Values() ([]interface{}, error) {
	if m.currentRow >= len(m.rows) {
		return nil, pgx.ErrNoRows
	}

	row := m.rows[m.currentRow]
	return []interface{}{
		row.ID,
		row.Name,
		row.Description,
		row.Price,
		row.Stock,
		row.Category,
		row.CreatedAt,
		row.UpdatedAt,
		row.IsActive,
		row.SKU,
		row.Weight,
		row.Dimensions,
		row.Color,
		row.Brand,
		row.Rating,
		row.Reviews,
		row.Discount,
		row.Tags,
		row.SupplierID,
		row.WarehouseID,
	}, nil
}

// RawValues implements the pgx.Rows RawValues method
func (m *MockRows) RawValues() [][]byte {
	return nil
}

// Conn implements the pgx.Rows Conn method
func (m *MockRows) Conn() *pgx.Conn {
	return nil
}

// Scanner function that complies with RowToFunc[T any] interface
func mockScanner[T any](rows pgx.CollectableRow) (T, error) {
	var product Product
	err := rows.Scan(
		&product.ID,
		&product.Name,
		&product.Description,
		&product.Price,
		&product.Stock,
		&product.Category,
		&product.CreatedAt,
		&product.UpdatedAt,
		&product.IsActive,
		&product.SKU,
		&product.Weight,
		&product.Dimensions,
		&product.Color,
		&product.Brand,
		&product.Rating,
		&product.Reviews,
		&product.Discount,
		&product.Tags,
		&product.SupplierID,
		&product.WarehouseID,
	)

	if err != nil {
		var zero T
		return zero, err
	}

	// Type assert the result
	result, ok := any(product).(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("cannot convert Product to desired type")
	}

	return result, nil
}

// pkg: github.com/jackc/pgx/v5
// cpu: AMD Ryzen 9 7950X 16-Core Processor
// BenchmarkAllRowsScanned
// BenchmarkAllRowsScanned/AllRowsScanned
// BenchmarkAllRowsScanned/AllRowsScanned-32		3901	    308009 ns/op	  899405 B/op	    3029 allocs/op
// BenchmarkAllRowsScanned/CollectRows
// BenchmarkAllRowsScanned/CollectRows-32   		2388	    479571 ns/op	 1787199 B/op	    3015 allocs/op
func BenchmarkAllRowsScanned(b *testing.B) {
	// this benchmark is to compare the performance of iterator scanner against the
	// collect rows scanner, itself scanning is omitted (and stubbed) for the sake of
	// simplicity

	// Create test data for 1k items
	testProducts := make([]Product, 0, 1000)
	for i := 0; i < 1000; i++ {
		testProducts = append(testProducts, Product{
			ID:          int32(i),
			Name:        "Product Name",
			Description: "Product Description",
			Price:       100.0,
			Stock:       10,
			Category:    "Category",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
			IsActive:    true,
			SKU:         "SKU",
			Weight:      1.0,
			Dimensions:  "10x10x10",
			Color:       "Red",
			Brand:       "Brand",
			Rating:      4.5,
			Reviews:     100,
			Discount:    0.0,
			Tags:        []string{"tag1", "tag2"},
			SupplierID:  1,
			WarehouseID: 1,
		})
	}

	b.Run("AllRowsScanned", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			mockRows := NewMockRows(testProducts)

			dict := make(map[int32]Product)
			for row := range pgx.AllRowsScanned[Product](mockRows, mockScanner) {
				dict[row.ID] = row
			}
		}
	})

	b.Run("CollectRows", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			mockRows := NewMockRows(testProducts)

			rows, _ := pgx.CollectRows[Product](mockRows, mockScanner)

			dict := make(map[int32]Product, len(rows))
			for _, row := range rows {
				dict[row.ID] = row
			}
		}
	})
}
