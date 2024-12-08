package main

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
	"honnef.co/go/tools/lintcmd/cache"
)

const (
	initialFare     = 500
	farePerDistance = 100
)

type ownerPostOwnersRequest struct {
	Name string `json:"name"`
}

type ownerPostOwnersResponse struct {
	ID                 string `json:"id"`
	ChairRegisterToken string `json:"chair_register_token"`
}

func ownerPostOwners(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &ownerPostOwnersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name) are empty"))
		return
	}

	ownerID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	chairRegisterToken := secureRandomStr(32)

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO owners (id, name, access_token, chair_register_token) VALUES (?, ?, ?, ?)",
		ownerID, req.Name, accessToken, chairRegisterToken,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "owner_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &ownerPostOwnersResponse{
		ID:                 ownerID,
		ChairRegisterToken: chairRegisterToken,
	})
}

type chairSales struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Sales int    `json:"sales"`
}

type modelSales struct {
	Model string `json:"model"`
	Sales int    `json:"sales"`
}

type ownerGetSalesResponse struct {
	TotalSales int          `json:"total_sales"`
	Chairs     []chairSales `json:"chairs"`
	Models     []modelSales `json:"models"`
}

func ownerGetSales(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	since := time.Unix(0, 0)
	until := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	if r.URL.Query().Get("since") != "" {
		parsed, err := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		since = time.UnixMilli(parsed)
	}
	if r.URL.Query().Get("until") != "" {
		parsed, err := strconv.ParseInt(r.URL.Query().Get("until"), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		until = time.UnixMilli(parsed)
	}

	owner := r.Context().Value("owner").(*Owner)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	txl, err := dbl.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer txl.Rollback()

	chairs := []Chair{}
	if err := txl.SelectContext(ctx, &chairs, "SELECT * FROM chairs WHERE owner_id = ?", owner.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	res := ownerGetSalesResponse{
		TotalSales: 0,
	}

	modelSalesByModel := map[string]int{}
	for _, chair := range chairs {
		rides := []Ride{}
		if err := tx.SelectContext(ctx, &rides, "SELECT rides.* FROM rides JOIN ride_statuses ON rides.id = ride_statuses.ride_id WHERE chair_id = ? AND status = 'COMPLETED' AND updated_at BETWEEN ? AND ? + INTERVAL 999 MICROSECOND", chair.ID, since, until); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		sales := sumSales(rides)
		res.TotalSales += sales

		res.Chairs = append(res.Chairs, chairSales{
			ID:    chair.ID,
			Name:  chair.Name,
			Sales: sales,
		})

		modelSalesByModel[chair.Model] += sales
	}

	models := []modelSales{}
	for model, sales := range modelSalesByModel {
		models = append(models, modelSales{
			Model: model,
			Sales: sales,
		})
	}
	res.Models = models

	writeJSON(w, http.StatusOK, res)
}

func sumSales(rides []Ride) int {
	sale := 0
	for _, ride := range rides {
		sale += calculateSale(ride)
	}
	return sale
}

func calculateSale(ride Ride) int {
	return calculateFare(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
}

type chairWithDetail struct {
	ID                     string       `db:"id"`
	OwnerID                string       `db:"owner_id"`
	Name                   string       `db:"name"`
	AccessToken            string       `db:"access_token"`
	Model                  string       `db:"model"`
	IsActive               bool         `db:"is_active"`
	CreatedAt              time.Time    `db:"created_at"`
	UpdatedAt              time.Time    `db:"updated_at"`
	TotalDistance          int          `db:"total_distance"`
	TotalDistanceUpdatedAt sql.NullTime `db:"total_distance_updated_at"`
}

type ownerGetChairResponse struct {
	Chairs []ownerGetChairResponseChair `json:"chairs"`
}

type ownerGetChairResponseChair struct {
	ID                     string `json:"id"`
	Name                   string `json:"name"`
	Model                  string `json:"model"`
	Active                 bool   `json:"active"`
	RegisteredAt           int64  `json:"registered_at"`
	TotalDistance          int    `json:"total_distance"`
	TotalDistanceUpdatedAt *int64 `json:"total_distance_updated_at,omitempty"`
}

// グローバルキャッシュ
var chairDistanceCache *cache.Cache

// キャッシュ構造体
type ChairDistance struct {
	TotalDistance          float64   // 合計距離
	TotalDistanceUpdatedAt time.Time // 最終更新日時
}

func init() {
	// 0.05秒間の有効期限、0.5秒間アクセスがない場合に自動削除
	chairDistanceCache = cache.New(50*time.Millisecond, 500*time.Millisecond)
}

func updateChairDistanceCache(chairID string, totalDistance float64, updatedAt time.Time) {
	// キャッシュにデータを保存
	chairDistanceCache.Set(chairID, ChairDistance{
		TotalDistance:          totalDistance,
		TotalDistanceUpdatedAt: updatedAt,
	}, cache.DefaultExpiration)
}

func getChairDistance(chairID string) (ChairDistance, bool) {
	// キャッシュから取得
	if data, found := chairDistanceCache.Get(chairID); found {
		return data.(ChairDistance), true
	}
	return ChairDistance{}, false
}

func ownerGetChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	owner := ctx.Value("owner").(*Owner)

	// 改良されたクエリ（キャッシュ未ヒット時に利用）
	chairs := []chairWithDetail{}
	query := `
		SELECT c.id,
		       c.owner_id,
		       c.name,
		       c.access_token,
		       c.model,
		       c.is_active,
		       c.created_at,
		       c.updated_at
		FROM chairs c
		WHERE c.owner_id = ?
	`
	if err := dbl.SelectContext(ctx, &chairs, query, owner.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// キャッシュ利用のレスポンス構築
	res := ownerGetChairResponse{}
	for _, chair := range chairs {
		// キャッシュから移動距離を取得
		distance, found := getChairDistance(chair.ID)
		if !found {
			// キャッシュが見つからない場合はデータベースから計算して更新
			totalDistance, updatedAt, err := calculateDistanceFromDatabase(ctx, chair.ID)

			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}

			updateChairDistanceCache(chair.ID, totalDistance, updatedAt)
			distance = ChairDistance{
				TotalDistance:          totalDistance,
				TotalDistanceUpdatedAt: updatedAt,
			}
		}

		// レスポンスに追加
		c := ownerGetChairResponseChair{
			ID:            chair.ID,
			Name:          chair.Name,
			Model:         chair.Model,
			Active:        chair.IsActive,
			RegisteredAt:  chair.CreatedAt.UnixMilli(),
			TotalDistance: distance.TotalDistance,
		}
		if !distance.TotalDistanceUpdatedAt.IsZero() {
			t := distance.TotalDistanceUpdatedAt.UnixMilli()
			c.TotalDistanceUpdatedAt = &t
		}
		res.Chairs = append(res.Chairs, c)
	}
	writeJSON(w, http.StatusOK, res)
}

// 地球の半径（km単位）
const EarthRadiusKm = 6371.0

// 地球の半径（メートル単位、整数で管理）
const EarthRadiusMeters = 6371000

// 精度調整のためのスケール（小数点以下を整数に変換）
const ScaleFactor = 1000

// 2点間の距離をハバースイン公式で計算（整数）
func haversineDistanceInt(lat1, lon1, lat2, lon2 int) int {
	// 度をラジアンに変換（整数で計算）
	rad := func(deg int) int {
		return deg * int(math.Pi*ScaleFactor/180) / ScaleFactor
	}

	lat1Rad := rad(lat1)
	lon1Rad := rad(lon1)
	lat2Rad := rad(lat2)
	lon2Rad := rad(lon2)

	// 緯度と経度の差（整数で計算）
	deltaLat := lat2Rad - lat1Rad
	deltaLon := lon2Rad - lon1Rad

	// ハバースイン公式（整数演算）
	a := (int(math.Sin(float64(deltaLat)/2*ScaleFactor/ScaleFactor)) *
		int(math.Sin(float64(deltaLat)/2*ScaleFactor/ScaleFactor))) +
		(int(math.Cos(float64(lat1Rad*ScaleFactor/ScaleFactor))) *
			int(math.Cos(float64(lat2Rad*ScaleFactor/ScaleFactor))) *
			int(math.Sin(float64(deltaLon)/2*ScaleFactor/ScaleFactor)) *
			int(math.Sin(float64(deltaLon)/2*ScaleFactor/ScaleFactor)))

	c := 2 * int(math.Atan2(math.Sqrt(float64(a)), math.Sqrt(float64(1-a))))

	return EarthRadiusMeters * c / ScaleFactor
}

// データベースから移動距離を計算する関数
func calculateDistanceFromDatabase(ctx context.Context, chairID string) (int, time.Time, error) {
	query := `
		SELECT latitude, longitude, created_at
		FROM chair_locations
		WHERE chair_id = ?
		ORDER BY created_at;
	`

	// クエリ実行
	rows, err := dbl.QueryContext(ctx, query, chairID)
	if err != nil {
		return 0, time.Time{}, err
	}
	defer rows.Close()

	// 距離計算用変数
	var totalDistance int
	var prevLat, prevLon int
	var prevTime time.Time
	var updatedAt time.Time

	// データを1行ずつ処理
	isFirstRow := true
	for rows.Next() {
		var lat, lon float64
		var createdAt time.Time

		if err := rows.Scan(&lat, &lon, &createdAt); err != nil {
			return 0, time.Time{}, err
		}

		// 緯度・経度をスケールに基づき整数に変換
		intLat := int(lat * ScaleFactor)
		intLon := int(lon * ScaleFactor)

		// 初回の処理をスキップ
		if isFirstRow {
			prevLat, prevLon, prevTime = intLat, intLon, createdAt
			isFirstRow = false
			continue
		}

		// 距離を計算（整数で計算）
		totalDistance += haversineDistanceInt(prevLat, prevLon, intLat, intLon)
		prevLat, prevLon, prevTime = intLat, intLon, createdAt
		updatedAt = createdAt
	}

	if err := rows.Err(); err != nil {
		return 0, time.Time{}, err
	}

	return totalDistance, updatedAt, nil
}
