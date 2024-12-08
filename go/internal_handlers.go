package main

import (
	"database/sql"
	"errors"
	"net/http"
)

type ChairWithDistance struct {
	ID        string
	Latitude  float64
	Longitude float64
	Distance  float64
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 未マッチのライドを取得
	ride := &Ride{}
	err := db.GetContext(ctx, ride, `SELECT id, pickup_latitude, pickup_longitude FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent) // 未マッチのライドなし
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ライドのピックアップ地点を基に最も近い空き椅子を検索
	chair := &ChairWithDistance{}
	query := `
		SELECT chairs.id, chair_locations.latitude, chair_locations.longitude,
		       (6371 * ACOS(COS(RADIANS(?)) * COS(RADIANS(chair_locations.latitude)) *
		       COS(RADIANS(chair_locations.longitude) - RADIANS(?)) +
		       SIN(RADIANS(?)) * SIN(RADIANS(chair_locations.latitude)))) AS distance
		FROM chairs
		INNER JOIN chair_locations ON chairs.id = chair_locations.chair_id
		WHERE chairs.is_active = TRUE
		  AND chairs.is_in_use = FALSE
		ORDER BY distance ASC
		LIMIT 1;
	`
	err = db.GetContext(ctx, chair, query, ride.PickupLatitude, ride.PickupLongitude, ride.PickupLatitude)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent) // 空き椅子なし
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ライドと椅子をマッチング
	_, err = db.ExecContext(ctx, `
		UPDATE rides SET chair_id = ? WHERE id = ?
	`, chair.ID, ride.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 椅子の状態を更新
	_, err = db.ExecContext(ctx, `
		UPDATE chairs SET is_in_use = TRUE WHERE id = ?
	`, chair.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
