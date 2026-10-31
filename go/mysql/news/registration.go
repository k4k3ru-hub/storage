package news

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// RegisterPublisher inserts immutable publisher registration or confirms an exact retry.
// Any error requires the caller to roll back the transaction.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) RegisterPublisher(ctx context.Context, tx *sql.Tx, p Publisher) error {
	const op = "register news publisher"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if err := token(p.ID, 64, false); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if err := text(p.DisplayName, 256, false); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if _, err := CanonicalURL(p.HomepageURL); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if !validTime(p.CreatedAt) {
		return invalid(op, "created_at=out_of_range")
	}
	var existing Publisher
	err := tx.QueryRowContext(ctx, "SELECT publisher_id,display_name,homepage_url,created_at FROM "+s.table("publishers")+" WHERE publisher_id=? FOR UPDATE", p.ID).Scan(&existing.ID, &existing.DisplayName, &existing.HomepageURL, &existing.CreatedAt)
	if err == nil {
		if existing.ID == p.ID && existing.DisplayName == p.DisplayName && existing.HomepageURL == p.HomepageURL && existing.CreatedAt.Equal(p.CreatedAt) {
			return nil
		}
		return conflict(op)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return rowError(err, op)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table("publishers")+" (publisher_id,display_name,homepage_url,created_at) VALUES (?,?,?,?)", p.ID, p.DisplayName, p.HomepageURL, p.CreatedAt.UTC())
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return nil
}

// RegisterSource creates version 1 when expectedVersion is zero, or updates a source
// using version compare-and-swap. The new version must be expectedVersion+1.
// Exact retries are accepted; publisher membership and creation time are immutable.
// Any error requires the caller to roll back the transaction.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) RegisterSource(ctx context.Context, tx *sql.Tx, v Source, expectedVersion uint64) error {
	const op = "register news source"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if err := validateSource(v); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if expectedVersion == ^uint64(0) || v.RegistrationVersion != expectedVersion+1 {
		return invalid(op, "registration_version=invalid")
	}
	old, err := s.getSource(ctx, tx, v.ID, true)
	if err == nil {
		if sameSource(old, v) {
			return nil
		}
		if old.RegistrationVersion != expectedVersion || old.PublisherID != v.PublisherID || !old.CreatedAt.Equal(v.CreatedAt) || v.UpdatedAt.Before(old.UpdatedAt) {
			return conflict(op)
		}
		result, err := tx.ExecContext(ctx, "UPDATE "+s.table("sources")+" SET display_name=?,endpoint_url=?,format_key=?,language_hint=?,registration_version=?,enabled=?,updated_at=? WHERE source_id=? AND registration_version=?", v.DisplayName, v.EndpointURL, v.FormatKey, nullableString(v.LanguageHint), v.RegistrationVersion, v.Enabled, v.UpdatedAt.UTC(), v.ID, expectedVersion)
		if err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		return expectOne(result, op)
	}
	if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if expectedVersion != 0 {
		return conflict(op)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table("sources")+" (source_id,publisher_id,display_name,endpoint_url,format_key,language_hint,registration_version,enabled,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)", v.ID, v.PublisherID, v.DisplayName, v.EndpointURL, v.FormatKey, nullableString(v.LanguageHint), v.RegistrationVersion, v.Enabled, v.CreatedAt.UTC(), v.UpdatedAt.UTC())
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return nil
}

// RegisterCategory creates a category or updates its label and enabled flag.
// Its key, direct parent and creation time remain immutable. Any error requires rollback.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) RegisterCategory(ctx context.Context, tx *sql.Tx, v Category) error {
	const op = "register news category"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if err := categoryKey(v.Key); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	parent := ""
	if index := strings.LastIndexByte(v.Key, '/'); index >= 0 {
		parent = v.Key[:index]
	}
	if v.ParentKey != parent {
		return invalid(op, "parent_key=invalid")
	}
	if err := text(v.DisplayName, 256, false); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if !validTime(v.CreatedAt) {
		return invalid(op, "created_at=out_of_range")
	}
	var old Category
	var oldParent sql.NullString
	err := tx.QueryRowContext(ctx, "SELECT category_key,parent_key,display_name,enabled,created_at FROM "+s.table("categories")+" WHERE category_key=? FOR UPDATE", v.Key).Scan(&old.Key, &oldParent, &old.DisplayName, &old.Enabled, &old.CreatedAt)
	if err == nil {
		if oldParent.String != v.ParentKey || !old.CreatedAt.Equal(v.CreatedAt) {
			return conflict(op)
		}
		if old.DisplayName == v.DisplayName && old.Enabled == v.Enabled {
			return nil
		}
		_, err = tx.ExecContext(ctx, "UPDATE "+s.table("categories")+" SET display_name=?,enabled=? WHERE category_key=?", v.DisplayName, v.Enabled, v.Key)
	} else if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table("categories")+" (category_key,parent_key,display_name,enabled,created_at) VALUES (?,?,?,?,?)", v.Key, nullableString(v.ParentKey), v.DisplayName, v.Enabled, v.CreatedAt.UTC())
	}
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return nil
}

func validateSource(v Source) error {
	for _, f := range []struct {
		v string
		n int
	}{{v.ID, 64}, {v.PublisherID, 64}, {v.FormatKey, 32}} {
		if err := token(f.v, f.n, false); err != nil {
			return err
		}
	}
	if err := text(v.DisplayName, 256, false); err != nil {
		return err
	}
	if _, err := CanonicalURL(v.EndpointURL); err != nil {
		return err
	}
	if err := language(v.LanguageHint); err != nil {
		return err
	}
	if v.RegistrationVersion == 0 {
		return invalid("validate news source", "registration_version=empty")
	}
	if !validTime(v.CreatedAt) || !validTime(v.UpdatedAt) || v.UpdatedAt.Before(v.CreatedAt) {
		return invalid("validate news source", "time=out_of_range")
	}
	return nil
}
func sameSource(a, b Source) bool {
	return a.ID == b.ID && a.PublisherID == b.PublisherID && a.DisplayName == b.DisplayName && a.EndpointURL == b.EndpointURL && a.FormatKey == b.FormatKey && a.LanguageHint == b.LanguageHint && a.RegistrationVersion == b.RegistrationVersion && a.Enabled == b.Enabled && a.CreatedAt.Equal(b.CreatedAt) && a.UpdatedAt.Equal(b.UpdatedAt)
}
func (s *Store) getSource(ctx context.Context, tx *sql.Tx, id string, lock bool) (Source, error) {
	var v Source
	var lang sql.NullString
	q := "SELECT source_id,publisher_id,display_name,endpoint_url,format_key,language_hint,registration_version,enabled,created_at,updated_at FROM " + s.table("sources") + " WHERE source_id=?"
	if lock {
		q += " FOR UPDATE"
	}
	err := tx.QueryRowContext(ctx, q, id).Scan(&v.ID, &v.PublisherID, &v.DisplayName, &v.EndpointURL, &v.FormatKey, &lang, &v.RegistrationVersion, &v.Enabled, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		return Source{}, rowError(err, "read news source")
	}
	v.LanguageHint = lang.String
	if err := validateSource(v); err != nil {
		return Source{}, fmt.Errorf("failed to read news source: %w", ErrIntegrity)
	}
	return v, nil
}
func expectOne(result sql.Result, op string) error {
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if n != 1 {
		return conflict(op)
	}
	return nil
}
