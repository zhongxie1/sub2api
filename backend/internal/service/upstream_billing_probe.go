package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	// These values live in accounts.extra so PR2 does not require a schema migration.
	UpstreamBillingProbeExtraKey           = "upstream_billing_probe"
	UpstreamBillingProbeEnabledExtraKey    = "upstream_billing_probe_enabled"
	UpstreamBillingRateSyncEnabledExtraKey = "upstream_billing_rate_sync_enabled"

	upstreamBillingProbeDefaultIntervalMinutes = 1
	upstreamBillingProbeMinIntervalMinutes     = 1
	upstreamBillingProbeMaxIntervalMinutes     = 24 * 60
	upstreamBillingProbeCycleInterval          = time.Minute
	upstreamBillingProbeRequestTimeout         = 10 * time.Second
	upstreamBillingProbeMaxBodyBytes           = 64 * 1024
	upstreamBillingProbeMaxPerCycle            = 1000
	upstreamBillingProbeManualBatchSize        = 20
	upstreamBillingProbeConcurrency            = 8
	upstreamBillingProbeMaxDelay               = 24 * time.Hour
	// unsupported 账号的重探间隔倍数：上游不是 sub2api 中转就不会突然长出
	// /v1/sub2api/billing，按常规 interval 重排只会持续占满每周期
	// upstreamBillingProbeMaxPerCycle 个名额。
	upstreamBillingProbeUnsupportedDelayFactor = 8
	upstreamBillingProbeAccountRateScale       = 10000.0
	upstreamBillingProbeLeaderLockKey          = "upstream:billing:probe:leader"
	upstreamBillingProbeMaxBatchRuntime        = time.Duration((upstreamBillingProbeMaxPerCycle+upstreamBillingProbeConcurrency-1)/upstreamBillingProbeConcurrency) * upstreamBillingProbeRequestTimeout
	upstreamBillingProbeLeaderLockTTL          = upstreamBillingProbeMaxBatchRuntime + 5*time.Minute
)

// UpstreamBillingProbeMaxBatchSize limits one manual refresh request. The
// periodic runner has a larger independent bound so all normal-size pools can
// still be refreshed each minute.
const UpstreamBillingProbeMaxBatchSize = upstreamBillingProbeManualBatchSize

// upstreamBillingRateSyncMaxMultiplier bounds the value the automatic
// write-back may push into accounts.rate_multiplier.
//
// No other code path bounds that column from above — admins may type any
// non-negative number and the only ceiling is the DECIMAL(10,4) column itself
// (999999.9999). That ceiling is meaningless as a guard: rate_multiplier
// scales the per-request account cost that feeds quota_used, so a single
// declared 999999 would exhaust any account quota on the first request and
// poison cost reporting. 100 is picked as a deliberately generous bound: it is
// two orders of magnitude above the 1.0 default and far above any plausible
// upstream resale markup, so no legitimate declaration is rejected while an
// absurd or hostile one cannot reach the quota control plane unattended.
// It only constrains the automatic path; manual edits keep their old range.
const upstreamBillingRateSyncMaxMultiplier = 100.0

var (
	ErrUpstreamBillingProbeUnavailable = infraerrors.ServiceUnavailable(
		"UPSTREAM_BILLING_PROBE_UNAVAILABLE", "upstream billing probe is unavailable",
	)
	ErrUpstreamBillingProbeAccountInvalid = infraerrors.BadRequest(
		"UPSTREAM_BILLING_PROBE_ACCOUNT_INVALID", "account is not a non-OAuth upstream account",
	)
	ErrUpstreamBillingProbeIdentityChanged = infraerrors.Conflict(
		"UPSTREAM_BILLING_PROBE_IDENTITY_CHANGED", "account identity changed during upstream billing probe; retry the probe",
	)
	ErrUpstreamBillingRateSyncBulkConflict = infraerrors.Conflict(
		"UPSTREAM_BILLING_RATE_SYNC_BULK_CONFLICT",
		"account rate multiplier cannot be changed in bulk while upstream billing rate sync is enabled",
	)
	ErrUpstreamBillingRateSyncConflict = infraerrors.Conflict(
		"UPSTREAM_BILLING_RATE_SYNC_CONFLICT",
		"account rate multiplier cannot be changed while upstream billing rate sync is enabled",
	)
)

const (
	UpstreamBillingProbeStatusOK          = "ok"
	UpstreamBillingProbeStatusUnsupported = "unsupported"
	UpstreamBillingProbeStatusFailed      = "failed"
)

// UpstreamBillingProbeSettings controls the periodic probe runner.
type UpstreamBillingProbeSettings struct {
	Enabled         bool `json:"enabled"`
	IntervalMinutes int  `json:"interval_minutes"`
}

// UpstreamBillingProbeSnapshot is persisted in accounts.extra. Data is kept as
// a sanitized map so future response fields do not require a database change.
type UpstreamBillingProbeSnapshot struct {
	Status        string         `json:"status"`
	Data          map[string]any `json:"data,omitempty"`
	ReceivedAt    *time.Time     `json:"received_at,omitempty"`
	FreshUntil    *time.Time     `json:"fresh_until,omitempty"`
	LastAttemptAt time.Time      `json:"last_attempt_at"`
	NextProbeAt   time.Time      `json:"next_probe_at"`
	FailureCount  int            `json:"failure_count,omitempty"`
	HTTPStatus    int            `json:"http_status,omitempty"`
	LastError     string         `json:"last_error,omitempty"`
	// SyncedRateMultiplier records the value this probe wrote into
	// accounts.rate_multiplier. It is only set when the account opted into rate
	// sync and the declared value passed the write-back range check, so the
	// stored snapshot always answers "did this probe move the account rate, and
	// to what" without a separate history table.
	SyncedRateMultiplier *float64 `json:"synced_rate_multiplier,omitempty"`
}

// UpstreamBillingProbeResult is returned by manual probe endpoints.
type UpstreamBillingProbeResult struct {
	AccountID int64                         `json:"account_id"`
	Snapshot  *UpstreamBillingProbeSnapshot `json:"snapshot,omitempty"`
	Error     string                        `json:"error,omitempty"`
}

// UpstreamBillingRateSnapshotItem is the compact representation used by the
// account table's background refresh. It intentionally excludes credentials,
// runtime counters, and usage data from the response.
type UpstreamBillingRateSnapshotItem struct {
	AccountID int64                         `json:"account_id"`
	Snapshot  *UpstreamBillingProbeSnapshot `json:"snapshot"`
}

// BuildUpstreamBillingRateSnapshotItems projects account rows into the
// read-only payload used by the rate refresh endpoint. Decode snapshots here
// so malformed or legacy extra data is handled consistently with probe logic.
func BuildUpstreamBillingRateSnapshotItems(accounts []Account) []UpstreamBillingRateSnapshotItem {
	items := make([]UpstreamBillingRateSnapshotItem, 0, len(accounts))
	for _, account := range accounts {
		var snapshot *UpstreamBillingProbeSnapshot
		// The billing endpoint is supported by every API-key platform; limiting
		// this projection to OpenAI would make the background refresh erase the
		// other platforms' persisted snapshots from the table.
		if account.Type == AccountTypeAPIKey {
			snapshot = decodeUpstreamBillingProbeSnapshot(account.Extra)
		}
		items = append(items, UpstreamBillingRateSnapshotItem{
			AccountID: account.ID,
			Snapshot:  snapshot,
		})
	}
	return items
}

type upstreamBillingProbeResponse struct {
	Object                  string   `json:"object"`
	SchemaVersion           int      `json:"schema_version"`
	BillingScope            string   `json:"billing_scope"`
	GroupRateMultiplier     *float64 `json:"group_rate_multiplier"`
	UserRateMultiplier      *float64 `json:"user_rate_multiplier"`
	ResolvedRateMultiplier  *float64 `json:"resolved_rate_multiplier"`
	PeakRateEnabled         *bool    `json:"peak_rate_enabled"`
	PeakStart               *string  `json:"peak_start"`
	PeakEnd                 *string  `json:"peak_end"`
	PeakRateMultiplier      *float64 `json:"peak_rate_multiplier"`
	AppliedPeakMultiplier   *float64 `json:"applied_peak_multiplier"`
	EffectiveRateMultiplier *float64 `json:"effective_rate_multiplier"`
	Timezone                *string  `json:"timezone"`
	ObservedAt              string   `json:"observed_at"`
}

type upstreamInfoProbeResponse struct {
	Object                  string   `json:"object"`
	SchemaVersion           int      `json:"schema_version"`
	Provider                string   `json:"provider"`
	Balance                 *float64 `json:"balance"`
	Currency                string   `json:"currency"`
	KeyQuota                *float64 `json:"key_quota"`
	KeyQuotaUsed            *float64 `json:"key_quota_used"`
	KeyQuotaRemaining       *float64 `json:"key_quota_remaining"`
	GroupCheckSupported     *bool    `json:"group_check_supported"`
	RemoteGroupID           *int64   `json:"remote_group_id"`
	RemoteGroupName         string   `json:"remote_group_name"`
	RemoteGroupExists       *bool    `json:"remote_group_exists"`
	RemoteGroupStatus       string   `json:"remote_group_status"`
	GroupRateMultiplier     *float64 `json:"group_rate_multiplier"`
	UserRateMultiplier      *float64 `json:"user_rate_multiplier"`
	ResolvedRateMultiplier  *float64 `json:"resolved_rate_multiplier"`
	PeakRateEnabled         *bool    `json:"peak_rate_enabled"`
	PeakStart               *string  `json:"peak_start"`
	PeakEnd                 *string  `json:"peak_end"`
	PeakRateMultiplier      *float64 `json:"peak_rate_multiplier"`
	AppliedPeakMultiplier   *float64 `json:"applied_peak_multiplier"`
	EffectiveRateMultiplier *float64 `json:"effective_rate_multiplier"`
	Timezone                *string  `json:"timezone"`
	ObservedAt              string   `json:"observed_at"`
}

type sub2APIUsageProbeResponse struct {
	Mode    string   `json:"mode"`
	Balance *float64 `json:"balance"`
	Unit    string   `json:"unit"`
}

type upstreamProbeEndpoint struct {
	provider string
	path     string
	newAPI   bool
	usage    bool
}

type upstreamProbeHTTPResponse struct {
	statusCode int
	header     http.Header
	body       []byte
}

// GetUpstreamBillingProbeSettings returns defaults when the setting is absent.
func (s *SettingService) GetUpstreamBillingProbeSettings(ctx context.Context) (*UpstreamBillingProbeSettings, error) {
	defaults := defaultUpstreamBillingProbeSettings()
	if s == nil || s.settingRepo == nil {
		return defaults, nil
	}
	value, err := s.settingRepo.GetValue(ctx, SettingKeyUpstreamBillingProbeSettings)
	if err != nil {
		if errors.Is(err, ErrSettingNotFound) {
			return defaults, nil
		}
		return nil, fmt.Errorf("get upstream billing probe settings: %w", err)
	}
	if strings.TrimSpace(value) == "" {
		return defaults, nil
	}
	settings := *defaults
	if err := json.Unmarshal([]byte(value), &settings); err != nil {
		return nil, fmt.Errorf("parse upstream billing probe settings: %w", err)
	}
	if settings.IntervalMinutes == 0 {
		settings.IntervalMinutes = defaults.IntervalMinutes
	}
	normalizeUpstreamBillingProbeSettings(&settings)
	return &settings, nil
}

// SetUpstreamBillingProbeSettings validates and persists the runner settings.
func (s *SettingService) SetUpstreamBillingProbeSettings(ctx context.Context, settings *UpstreamBillingProbeSettings) error {
	if s == nil || s.settingRepo == nil {
		return fmt.Errorf("setting repository is unavailable")
	}
	if settings == nil {
		return infraerrors.BadRequest("INVALID_UPSTREAM_BILLING_PROBE_SETTINGS", "settings cannot be nil")
	}
	if settings.IntervalMinutes < upstreamBillingProbeMinIntervalMinutes || settings.IntervalMinutes > upstreamBillingProbeMaxIntervalMinutes {
		return infraerrors.BadRequest(
			"INVALID_UPSTREAM_BILLING_PROBE_INTERVAL",
			fmt.Sprintf("interval_minutes must be between %d and %d", upstreamBillingProbeMinIntervalMinutes, upstreamBillingProbeMaxIntervalMinutes),
		)
	}
	normalizeUpstreamBillingProbeSettings(settings)
	data, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("marshal upstream billing probe settings: %w", err)
	}
	return s.settingRepo.Set(ctx, SettingKeyUpstreamBillingProbeSettings, string(data))
}

func defaultUpstreamBillingProbeSettings() *UpstreamBillingProbeSettings {
	return &UpstreamBillingProbeSettings{Enabled: true, IntervalMinutes: upstreamBillingProbeDefaultIntervalMinutes}
}

func normalizeUpstreamBillingProbeSettings(settings *UpstreamBillingProbeSettings) {
	if settings.IntervalMinutes < upstreamBillingProbeMinIntervalMinutes {
		settings.IntervalMinutes = upstreamBillingProbeMinIntervalMinutes
	}
	if settings.IntervalMinutes > upstreamBillingProbeMaxIntervalMinutes {
		settings.IntervalMinutes = upstreamBillingProbeMaxIntervalMinutes
	}
}

// UpstreamBillingProbeService discovers a remote Sub2API billing snapshot.
type UpstreamBillingProbeService struct {
	accountRepo        AccountRepository
	accountTestService *AccountTestService
	settingService     *SettingService

	parentCtx    context.Context
	parentCancel context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.Mutex
	started      bool
	stopped      bool
	cycleMu      sync.Mutex
	probeGroup   singleflight.Group
	probeSlots   chan struct{}
	now          func() time.Time
	lockCache    LeaderLockCache
	db           *sql.DB
	instanceID   string
}

type upstreamBillingProbeSnapshotWriter interface {
	UpdateUpstreamBillingProbeSnapshot(context.Context, *Account, *UpstreamBillingProbeSnapshot, *float64) error
}

type upstreamBillingProbeDueAccountLister interface {
	ListDueUpstreamBillingProbeAccounts(context.Context, time.Time, int) ([]Account, error)
}

func NewUpstreamBillingProbeService(
	accountRepo AccountRepository,
	accountTestService *AccountTestService,
	settingService *SettingService,
) *UpstreamBillingProbeService {
	ctx, cancel := context.WithCancel(context.Background())
	return &UpstreamBillingProbeService{
		accountRepo:        accountRepo,
		accountTestService: accountTestService,
		settingService:     settingService,
		parentCtx:          ctx,
		parentCancel:       cancel,
		probeSlots:         make(chan struct{}, upstreamBillingProbeConcurrency),
		now:                time.Now,
		instanceID:         uuid.NewString(),
	}
}

func (s *UpstreamBillingProbeService) SetLeaderLock(lockCache LeaderLockCache, db *sql.DB) {
	if s == nil {
		return
	}
	s.lockCache = lockCache
	s.db = db
}

// ProvideUpstreamBillingProbeService starts the process-wide periodic runner.
func ProvideUpstreamBillingProbeService(
	accountRepo AccountRepository,
	accountTestService *AccountTestService,
	settingService *SettingService,
	lockCache LeaderLockCache,
	db *sql.DB,
) *UpstreamBillingProbeService {
	svc := NewUpstreamBillingProbeService(accountRepo, accountTestService, settingService)
	svc.SetLeaderLock(lockCache, db)
	svc.Start()
	return svc
}

func (s *UpstreamBillingProbeService) Start() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.started || s.stopped {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.wg.Add(1)
	s.mu.Unlock()
	go s.runLoop()
}

func (s *UpstreamBillingProbeService) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.parentCancel()
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *UpstreamBillingProbeService) runLoop() {
	defer s.wg.Done()
	_ = s.RunDue(s.parentCtx)
	ticker := time.NewTicker(upstreamBillingProbeCycleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.parentCtx.Done():
			return
		case <-ticker.C:
			if err := s.RunDue(s.parentCtx); err != nil {
				logger.LegacyPrintf("service.upstream_billing_probe", "run_due_failed: err=%v", err)
			}
		}
	}
}

// RunDue executes at most one bounded batch of due accounts.
func (s *UpstreamBillingProbeService) RunDue(ctx context.Context) error {
	if s == nil || s.accountRepo == nil {
		return nil
	}
	s.cycleMu.Lock()
	defer s.cycleMu.Unlock()

	settings, err := s.getSettings(ctx)
	if err != nil {
		return err
	}
	if !settings.Enabled {
		return nil
	}
	runRelease, acquired, lockErr := s.tryAcquireLeaderLock(ctx, upstreamBillingProbeLeaderLockKey)
	if lockErr != nil {
		return fmt.Errorf("acquire upstream billing probe leader lock: %w", lockErr)
	}
	if !acquired {
		return nil
	}
	defer runRelease()

	lockNow := time.Now()
	cadenceRelease, acquired, lockErr := s.tryAcquireLeaderLock(ctx, upstreamBillingProbeLeaderLockKeyAt(lockNow))
	if lockErr != nil {
		return fmt.Errorf("acquire upstream billing probe cadence lock: %w", lockErr)
	}
	if !acquired {
		return nil
	}
	defer releaseUpstreamBillingProbeLeaderLock(cadenceRelease, lockNow.Truncate(upstreamBillingProbeCycleInterval).Add(upstreamBillingProbeCycleInterval))

	now := s.currentTime()
	accounts, err := s.listDueAccounts(ctx, now)
	if err != nil {
		return fmt.Errorf("list enabled upstream billing probes: %w", err)
	}
	due := make([]Account, 0, len(accounts))
	for i := range accounts {
		account := accounts[i]
		if !isUpstreamBillingProbeAccount(&account) || !account.IsActive() ||
			!upstreamBillingProbeEnabled(&account) || !upstreamBillingProbeConfigured(&account) {
			continue
		}
		snapshot := decodeUpstreamBillingProbeSnapshot(account.Extra)
		if snapshot != nil && !snapshot.NextProbeAt.IsZero() && now.Before(snapshot.NextProbeAt) {
			continue
		}
		due = append(due, account)
	}
	sort.SliceStable(due, func(i, j int) bool {
		left := decodeUpstreamBillingProbeSnapshot(due[i].Extra)
		right := decodeUpstreamBillingProbeSnapshot(due[j].Extra)
		leftUnset := left == nil || left.NextProbeAt.IsZero()
		rightUnset := right == nil || right.NextProbeAt.IsZero()
		if leftUnset && rightUnset {
			return due[i].ID < due[j].ID
		}
		if leftUnset {
			return true
		}
		if rightUnset {
			return false
		}
		return left.NextProbeAt.Before(right.NextProbeAt)
	})
	if len(due) > upstreamBillingProbeMaxPerCycle {
		due = due[:upstreamBillingProbeMaxPerCycle]
	}

	var group errgroup.Group
	for i := range due {
		accountID := due[i].ID
		group.Go(func() error {
			if _, probeErr := s.probeScheduledAccount(ctx, accountID, settings.IntervalMinutes); probeErr != nil {
				logger.LegacyPrintf("service.upstream_billing_probe", "probe_due_failed: account_id=%d err=%v", accountID, probeErr)
			}
			return nil
		})
	}
	return group.Wait()
}

func (s *UpstreamBillingProbeService) listDueAccounts(ctx context.Context, now time.Time) ([]Account, error) {
	if lister, ok := s.accountRepo.(upstreamBillingProbeDueAccountLister); ok {
		return lister.ListDueUpstreamBillingProbeAccounts(ctx, now, upstreamBillingProbeMaxPerCycle)
	}
	// Non-production repositories and older adapters keep the generic path. The
	// runner still truncates before issuing network requests.
	return s.accountRepo.FindByExtraField(ctx, UpstreamBillingProbeEnabledExtraKey, true)
}

func (s *UpstreamBillingProbeService) getSettings(ctx context.Context) (*UpstreamBillingProbeSettings, error) {
	if s.settingService == nil {
		return defaultUpstreamBillingProbeSettings(), nil
	}
	return s.settingService.GetUpstreamBillingProbeSettings(ctx)
}

func (s *UpstreamBillingProbeService) GetSettings(ctx context.Context) (*UpstreamBillingProbeSettings, error) {
	return s.getSettings(ctx)
}

func (s *UpstreamBillingProbeService) UpdateSettings(ctx context.Context, settings *UpstreamBillingProbeSettings) error {
	if s == nil || s.settingService == nil {
		return ErrUpstreamBillingProbeUnavailable
	}
	return s.settingService.SetUpstreamBillingProbeSettings(ctx, settings)
}

// ProbeAccount performs one manual or scheduled probe. Manual calls ignore both switches.
func (s *UpstreamBillingProbeService) ProbeAccount(ctx context.Context, accountID int64) (*UpstreamBillingProbeSnapshot, error) {
	if s == nil || s.accountRepo == nil {
		return nil, ErrUpstreamBillingProbeUnavailable
	}
	settings, err := s.getSettings(ctx)
	if err != nil {
		return nil, err
	}
	return s.probeAccount(ctx, accountID, settings.IntervalMinutes)
}

func (s *UpstreamBillingProbeService) probeAccount(ctx context.Context, accountID int64, intervalMinutes int) (*UpstreamBillingProbeSnapshot, error) {
	return s.probeAccountWithMode(ctx, accountID, intervalMinutes, false)
}

func (s *UpstreamBillingProbeService) probeScheduledAccount(ctx context.Context, accountID int64, intervalMinutes int) (*UpstreamBillingProbeSnapshot, error) {
	return s.probeAccountWithMode(ctx, accountID, intervalMinutes, true)
}

func (s *UpstreamBillingProbeService) probeAccountWithMode(ctx context.Context, accountID int64, intervalMinutes int, requireEnabled bool) (*UpstreamBillingProbeSnapshot, error) {
	key := strconv.FormatInt(accountID, 10)
	value, err, _ := s.probeGroup.Do(key, func() (any, error) {
		select {
		case s.probeSlots <- struct{}{}:
			defer func() { <-s.probeSlots }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		account, loadErr := s.accountRepo.GetByID(ctx, accountID)
		if loadErr != nil {
			return nil, loadErr
		}
		if !isUpstreamBillingProbeAccount(account) {
			return nil, ErrUpstreamBillingProbeAccountInvalid
		}
		if requireEnabled {
			if !account.IsActive() || !upstreamBillingProbeEnabled(account) || !upstreamBillingProbeConfigured(account) {
				return nil, nil
			}
			if snapshot := decodeUpstreamBillingProbeSnapshot(account.Extra); snapshot != nil &&
				!snapshot.NextProbeAt.IsZero() && s.currentTime().Before(snapshot.NextProbeAt) {
				return nil, nil
			}
		}
		return s.probeLoadedAccount(ctx, account, intervalMinutes)
	})
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	snapshot, ok := value.(*UpstreamBillingProbeSnapshot)
	if !ok {
		return nil, fmt.Errorf("invalid upstream billing probe result")
	}
	return snapshot, nil
}

// ProbeAccounts performs a bounded manual batch with the same concurrency limit as the runner.
func (s *UpstreamBillingProbeService) ProbeAccounts(ctx context.Context, accountIDs []int64) []UpstreamBillingProbeResult {
	if len(accountIDs) > upstreamBillingProbeManualBatchSize {
		accountIDs = accountIDs[:upstreamBillingProbeManualBatchSize]
	}
	results := make([]UpstreamBillingProbeResult, len(accountIDs))
	if s == nil || s.accountRepo == nil {
		for i, accountID := range accountIDs {
			results[i] = UpstreamBillingProbeResult{AccountID: accountID, Error: ErrUpstreamBillingProbeUnavailable.Error()}
		}
		return results
	}
	settings, settingsErr := s.getSettings(ctx)
	if settingsErr != nil {
		for i, accountID := range accountIDs {
			results[i] = UpstreamBillingProbeResult{AccountID: accountID, Error: safeProbeError(settingsErr)}
		}
		return results
	}
	var group errgroup.Group
	for i, accountID := range accountIDs {
		i, accountID := i, accountID
		results[i].AccountID = accountID
		group.Go(func() error {
			snapshot, err := s.probeAccount(ctx, accountID, settings.IntervalMinutes)
			if err != nil {
				results[i].Error = safeProbeError(err)
				return nil
			}
			results[i].Snapshot = snapshot
			return nil
		})
	}
	_ = group.Wait()
	return results
}

func upstreamBillingProbeLeaderLockKeyAt(now time.Time) string {
	return fmt.Sprintf("%s:%d", upstreamBillingProbeLeaderLockKey, now.Unix()/int64(upstreamBillingProbeCycleInterval/time.Second))
}

func (s *UpstreamBillingProbeService) tryAcquireLeaderLock(ctx context.Context, key string) (func(), bool, error) {
	lockCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if s.lockCache != nil {
		acquired, err := s.lockCache.TryAcquireLeaderLock(lockCtx, key, s.instanceID, upstreamBillingProbeLeaderLockTTL)
		if err != nil {
			return nil, false, err
		}
		if !acquired {
			return nil, false, nil
		}
		return func() {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer releaseCancel()
			_ = s.lockCache.ReleaseLeaderLock(releaseCtx, key, s.instanceID)
		}, true, nil
	}
	if s.db != nil {
		return tryAcquireDBAdvisoryLockWithError(lockCtx, s.db, hashAdvisoryLockID(key))
	}
	return func() {}, true, nil
}

func releaseUpstreamBillingProbeLeaderLock(release func(), releaseAt time.Time) {
	delay := time.Until(releaseAt)
	if delay <= 0 {
		release()
		return
	}
	time.AfterFunc(delay, release)
}

func (s *UpstreamBillingProbeService) SetAccountEnabled(ctx context.Context, accountID int64, enabled bool) error {
	if s == nil || s.accountRepo == nil {
		return ErrUpstreamBillingProbeUnavailable
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return err
	}
	if !isUpstreamBillingProbeAccount(account) {
		return ErrUpstreamBillingProbeAccountInvalid
	}
	updates := map[string]any{UpstreamBillingProbeEnabledExtraKey: enabled}
	if !enabled {
		updates[UpstreamBillingRateSyncEnabledExtraKey] = false
	}
	return s.accountRepo.UpdateExtra(ctx, accountID, updates)
}

func (s *UpstreamBillingProbeService) probeLoadedAccount(ctx context.Context, account *Account, intervalMinutes int) (*UpstreamBillingProbeSnapshot, error) {
	now := s.currentTime().UTC()
	if s.accountTestService == nil || s.accountTestService.httpUpstream == nil {
		return s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "transport_unavailable", 0)
	}
	apiKey := strings.TrimSpace(account.GetCredential("api_key"))
	if apiKey == "" {
		return s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "missing_api_key", 0)
	}
	baseURL := strings.TrimSpace(account.GetCredential("base_url"))
	if account.RoutesProtocolByInbound() && account.IsAdaptiveAPIProtocol() {
		baseURL = account.GetCNProtocolBaseURL(APIProtocolChatCompletions)
	}
	if account.Platform == PlatformOpenAI {
		if baseURL == "" {
			baseURL = "https://api.openai.com"
		}
	} else if upstreamBillingProbeTargetIsOfficialAPI(baseURL) {
		return s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "unsupported", 0)
	}
	normalizedBaseURL, err := s.accountTestService.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "invalid_base_url", 0)
	}
	proxyURL := ""
	if account.ProxyID != nil {
		if account.Proxy == nil {
			return s.persistProbeFailure(ctx, account, intervalMinutes, now, 0, "proxy_unavailable", 0)
		}
		if account.Proxy.ID != *account.ProxyID {
			return nil, ErrUpstreamBillingProbeIdentityChanged
		}
		proxyURL = account.Proxy.URL()
	}
	probeCtx, cancel := context.WithTimeout(ctx, upstreamBillingProbeRequestTimeout)
	defer cancel()
	var tlsProfile *tlsfingerprint.Profile
	if s.accountTestService.tlsFPProfileService != nil {
		tlsProfile = s.accountTestService.tlsFPProfileService.ResolveTLSProfile(account)
	}

	lastStatus := 0
	lastReason := "unsupported"
	lastRetryAfter := time.Duration(0)
	for _, endpoint := range upstreamProbeEndpoints(account) {
		probeURL := buildUpstreamProbeEndpointURL(normalizedBaseURL, endpoint)
		result, reason := s.fetchUpstreamProbeEndpoint(probeCtx, account, proxyURL, tlsProfile, apiKey, probeURL)
		if reason != "" {
			statusCode := 0
			retryDelay := time.Duration(0)
			if result != nil {
				statusCode = result.statusCode
				retryDelay = retryAfter(result.header, now)
			}
			return s.persistProbeFailure(ctx, account, intervalMinutes, now, statusCode, reason, retryDelay)
		}
		lastStatus = result.statusCode
		lastRetryAfter = retryAfter(result.header, now)
		if result.statusCode == http.StatusNotFound || result.statusCode == http.StatusMethodNotAllowed {
			continue
		}
		if result.statusCode < 200 || result.statusCode >= 300 {
			return s.persistProbeFailure(ctx, account, intervalMinutes, now, result.statusCode, "http_error", lastRetryAfter)
		}

		data, parseErr := parseUpstreamProbeEndpointResponse(endpoint, result.body, now)
		if parseErr != nil {
			lastReason = "invalid_response"
			continue
		}
		if endpoint.newAPI {
			s.enrichNewAPIBalance(probeCtx, account, proxyURL, tlsProfile, normalizedBaseURL, data)
			s.enrichNewAPIGroupCheck(probeCtx, account, proxyURL, tlsProfile, apiKey, normalizedBaseURL, data)
		}
		snapshot := &UpstreamBillingProbeSnapshot{
			Status:        UpstreamBillingProbeStatusOK,
			Data:          data,
			ReceivedAt:    probeTimePtr(now),
			FreshUntil:    probeTimePtr(now.Add(2 * time.Duration(intervalMinutes) * time.Minute)),
			LastAttemptAt: now,
			NextProbeAt:   now.Add(nextProbeDelay(intervalMinutes, 0)),
			HTTPStatus:    result.statusCode,
		}
		var syncRate *float64
		previousRate := account.BillingRateMultiplier()
		if upstreamBillingRateSyncEnabled(account) {
			if value, valid := upstreamBillingProbeSyncRate(data); valid {
				syncRate = &value
				snapshot.SyncedRateMultiplier = &value
			} else {
				declared, _ := resolveAccountExtraNumber(data, "resolved_rate_multiplier")
				slog.Warn("upstream_billing_rate_sync_rejected",
					"source", "upstream_billing_probe",
					"account_id", account.ID,
					"declared_resolved_rate_multiplier", declared,
					"max_rate_multiplier", upstreamBillingRateSyncMaxMultiplier,
					"current_rate_multiplier", previousRate,
				)
			}
		}
		if err := s.updateSnapshot(ctx, account, snapshot, syncRate); err != nil {
			return nil, err
		}
		if syncRate != nil {
			slog.Info("upstream_billing_rate_sync_applied",
				"source", "upstream_billing_probe",
				"account_id", account.ID,
				"old_rate_multiplier", previousRate,
				"new_rate_multiplier", *syncRate,
			)
		}
		return snapshot, nil
	}
	return s.persistProbeFailure(ctx, account, intervalMinutes, now, lastStatus, lastReason, lastRetryAfter)
}

func upstreamProbeEndpoints(account *Account) []upstreamProbeEndpoint {
	sub2apiInfo := upstreamProbeEndpoint{provider: "sub2api", path: "/v1/sub2api/upstream-info"}
	sub2apiUsage := upstreamProbeEndpoint{provider: "sub2api", path: "/v1/usage", usage: true}
	sub2apiBilling := upstreamProbeEndpoint{provider: "sub2api", path: "/v1/sub2api/billing"}
	newAPIUsage := upstreamProbeEndpoint{provider: "newapi", path: "/api/usage/token/", newAPI: true}
	provider := ""
	if snapshot := decodeUpstreamBillingProbeSnapshot(account.Extra); snapshot != nil && snapshot.Data != nil {
		provider, _ = snapshot.Data["provider"].(string)
		if provider == "" {
			if object, _ := snapshot.Data["object"].(string); strings.HasPrefix(object, "sub2api.") {
				provider = "sub2api"
			}
		}
	}
	if provider == "newapi" {
		return []upstreamProbeEndpoint{newAPIUsage, sub2apiInfo, sub2apiUsage, sub2apiBilling}
	}
	return []upstreamProbeEndpoint{sub2apiInfo, sub2apiUsage, sub2apiBilling, newAPIUsage}
}

func buildUpstreamProbeEndpointURL(baseURL string, endpoint upstreamProbeEndpoint) string {
	if !endpoint.newAPI {
		return buildOpenAIEndpointURL(baseURL, endpoint.path)
	}
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return strings.TrimRight(baseURL, "/") + endpoint.path
	}
	path := strings.TrimRight(parsed.Path, "/")
	if slash := strings.LastIndex(path, "/"); slash >= 0 && openAIBaseURLHasVersionSuffix(path) {
		path = path[:slash]
	} else if openAIBaseURLHasVersionSuffix(path) {
		path = ""
	}
	parsed.Path = strings.TrimRight(path, "/") + endpoint.path
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func (s *UpstreamBillingProbeService) fetchUpstreamProbeEndpoint(
	ctx context.Context,
	account *Account,
	proxyURL string,
	tlsProfile *tlsfingerprint.Profile,
	apiKey string,
	probeURL string,
) (*upstreamProbeHTTPResponse, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, bytes.NewReader(nil))
	if err != nil {
		return nil, "request_build_failed"
	}
	profile := HTTPUpstreamProfileDefault
	if account.Platform == PlatformOpenAI {
		profile = HTTPUpstreamProfileOpenAI
	}
	reqCtx := WithHTTPUpstreamProfile(req.Context(), profile)
	req = req.WithContext(WithHTTPUpstreamRedirectsDisabled(reqCtx))
	req.Header.Set("Accept", "application/json")
	if strings.TrimSpace(apiKey) != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	account.ApplyHeaderOverrides(req.Header)
	resp, err := s.accountTestService.httpUpstream.DoWithTLS(req, proxyURL, account.ID, account.Concurrency, tlsProfile)
	if err != nil {
		return nil, "request_failed"
	}
	if resp == nil || resp.Body == nil {
		return nil, "empty_response"
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamBillingProbeMaxBodyBytes+1))
	result := &upstreamProbeHTTPResponse{statusCode: resp.StatusCode, header: resp.Header, body: body}
	if err != nil {
		return result, "response_read_failed"
	}
	if len(body) > upstreamBillingProbeMaxBodyBytes {
		return result, "response_too_large"
	}
	return result, ""
}

func (s *UpstreamBillingProbeService) persistProbeFailure(
	ctx context.Context,
	account *Account,
	intervalMinutes int,
	now time.Time,
	statusCode int,
	reason string,
	retryAfterDuration time.Duration,
) (*UpstreamBillingProbeSnapshot, error) {
	previous := decodeUpstreamBillingProbeSnapshot(account.Extra)
	failureCount := 1
	if previous != nil {
		failureCount = previous.FailureCount + 1
	}
	status := UpstreamBillingProbeStatusFailed
	delay := nextProbeDelay(intervalMinutes, retryAfterDuration)
	if reason == "unsupported" {
		status = UpstreamBillingProbeStatusUnsupported
		delay = unsupportedProbeDelay(intervalMinutes, retryAfterDuration)
	}
	snapshot := &UpstreamBillingProbeSnapshot{
		Status:        status,
		LastAttemptAt: now,
		NextProbeAt:   now.Add(delay),
		FailureCount:  failureCount,
		HTTPStatus:    statusCode,
		LastError:     reason,
	}
	if previous != nil {
		snapshot.Data = previous.Data
		snapshot.ReceivedAt = previous.ReceivedAt
		snapshot.FreshUntil = previous.FreshUntil
		if snapshot.FreshUntil == nil && previous.Status == UpstreamBillingProbeStatusOK && previous.ReceivedAt != nil {
			snapshot.FreshUntil = probeTimePtr(previous.ReceivedAt.Add(2 * time.Duration(intervalMinutes) * time.Minute))
		}
	}
	if err := s.updateSnapshot(ctx, account, snapshot, nil); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *UpstreamBillingProbeService) updateSnapshot(
	ctx context.Context,
	account *Account,
	snapshot *UpstreamBillingProbeSnapshot,
	rateMultiplier *float64,
) error {
	writer, ok := s.accountRepo.(upstreamBillingProbeSnapshotWriter)
	if !ok {
		return ErrUpstreamBillingProbeUnavailable
	}
	return writer.UpdateUpstreamBillingProbeSnapshot(ctx, account, snapshot, rateMultiplier)
}

func parseUpstreamProbeEndpointResponse(endpoint upstreamProbeEndpoint, body []byte, observedAt time.Time) (map[string]any, error) {
	if endpoint.newAPI {
		return parseNewAPIUsageResponse(body, observedAt)
	}
	if endpoint.usage {
		return parseSub2APIUsageResponse(body, observedAt)
	}
	var identity struct {
		Object string `json:"object"`
	}
	if err := json.Unmarshal(body, &identity); err != nil {
		return nil, err
	}
	switch identity.Object {
	case "sub2api.upstream_info":
		return parseSub2APIUpstreamInfoResponse(body)
	case "sub2api.key_billing":
		return parseUpstreamBillingProbeResponse(body)
	default:
		return nil, fmt.Errorf("unexpected upstream response schema")
	}
}

func parseSub2APIUsageResponse(body []byte, observedAt time.Time) (map[string]any, error) {
	var response sub2APIUsageProbeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if response.Balance == nil {
		return nil, fmt.Errorf("sub2api usage response does not include account balance")
	}
	if math.IsNaN(*response.Balance) || math.IsInf(*response.Balance, 0) {
		return nil, fmt.Errorf("invalid upstream balance")
	}

	data := map[string]any{
		"object":      "sub2api.usage",
		"provider":    "sub2api",
		"balance":     *response.Balance,
		"observed_at": observedAt.UTC().Format(time.RFC3339Nano),
	}
	if unit := strings.TrimSpace(response.Unit); unit != "" {
		data["currency"] = unit
	}
	return data, nil
}

func parseSub2APIUpstreamInfoResponse(body []byte) (map[string]any, error) {
	var response upstreamInfoProbeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if response.Object != "sub2api.upstream_info" || response.SchemaVersion != 1 || response.Provider != "sub2api" {
		return nil, fmt.Errorf("unexpected upstream info response schema")
	}
	observedAt, err := time.Parse(time.RFC3339Nano, response.ObservedAt)
	if err != nil || observedAt.IsZero() {
		return nil, fmt.Errorf("invalid observed_at")
	}
	for _, value := range []*float64{response.Balance, response.KeyQuota, response.KeyQuotaUsed, response.KeyQuotaRemaining} {
		if value != nil && (math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return nil, fmt.Errorf("invalid upstream balance")
		}
	}
	for _, value := range []*float64{response.KeyQuota, response.KeyQuotaUsed, response.KeyQuotaRemaining} {
		if value != nil && *value < 0 {
			return nil, fmt.Errorf("invalid upstream quota")
		}
	}

	data := map[string]any{
		"object":         response.Object,
		"schema_version": response.SchemaVersion,
		"provider":       response.Provider,
		"observed_at":    observedAt.UTC().Format(time.RFC3339Nano),
	}
	copyOptionalProbeNumber(data, "balance", response.Balance)
	copyOptionalProbeNumber(data, "key_quota", response.KeyQuota)
	copyOptionalProbeNumber(data, "key_quota_used", response.KeyQuotaUsed)
	copyOptionalProbeNumber(data, "key_quota_remaining", response.KeyQuotaRemaining)
	if response.Currency != "" {
		data["currency"] = response.Currency
	}
	if response.GroupCheckSupported != nil {
		data["group_check_supported"] = *response.GroupCheckSupported
	}
	if response.RemoteGroupID != nil {
		data["remote_group_id"] = *response.RemoteGroupID
	}
	if response.RemoteGroupName != "" {
		data["remote_group_name"] = response.RemoteGroupName
	}
	if response.RemoteGroupExists != nil {
		data["remote_group_exists"] = *response.RemoteGroupExists
	}
	if response.RemoteGroupStatus != "" {
		data["remote_group_status"] = response.RemoteGroupStatus
	}

	if response.GroupRateMultiplier != nil || response.ResolvedRateMultiplier != nil || response.EffectiveRateMultiplier != nil {
		billingPayload, marshalErr := json.Marshal(upstreamBillingProbeResponse{
			Object:                  "sub2api.key_billing",
			SchemaVersion:           1,
			BillingScope:            "token",
			GroupRateMultiplier:     response.GroupRateMultiplier,
			UserRateMultiplier:      response.UserRateMultiplier,
			ResolvedRateMultiplier:  response.ResolvedRateMultiplier,
			PeakRateEnabled:         response.PeakRateEnabled,
			PeakStart:               response.PeakStart,
			PeakEnd:                 response.PeakEnd,
			PeakRateMultiplier:      response.PeakRateMultiplier,
			AppliedPeakMultiplier:   response.AppliedPeakMultiplier,
			EffectiveRateMultiplier: response.EffectiveRateMultiplier,
			Timezone:                response.Timezone,
			ObservedAt:              response.ObservedAt,
		})
		if marshalErr != nil {
			return nil, marshalErr
		}
		billingData, parseErr := parseUpstreamBillingProbeResponse(billingPayload)
		if parseErr != nil {
			return nil, parseErr
		}
		for key, value := range billingData {
			if key != "object" && key != "schema_version" && key != "provider" && key != "observed_at" {
				data[key] = value
			}
		}
	}
	return data, nil
}

func parseNewAPIUsageResponse(body []byte, observedAt time.Time) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil {
		return nil, err
	}
	if success, ok := root["success"].(bool); ok && !success {
		return nil, fmt.Errorf("newapi usage request failed")
	}
	if code, ok := root["code"].(bool); ok && !code {
		return nil, fmt.Errorf("newapi usage request failed")
	}
	payload := root
	if nested, ok := root["data"].(map[string]any); ok {
		payload = nested
	}

	data := map[string]any{
		"object":                "newapi.token_usage",
		"schema_version":        1,
		"provider":              "newapi",
		"group_check_supported": false,
		"observed_at":           observedAt.UTC().Format(time.RFC3339Nano),
	}
	recognized := false
	for _, mapping := range []struct {
		output string
		keys   []string
	}{
		{output: "balance", keys: []string{"balance", "remaining_balance"}},
		{output: "total_granted", keys: []string{"total_granted"}},
		{output: "total_used", keys: []string{"total_used"}},
		{output: "key_quota", keys: []string{"key_quota", "quota", "total_granted"}},
		{output: "key_quota_used", keys: []string{"key_quota_used", "used_quota", "total_used"}},
		{output: "key_quota_remaining", keys: []string{"key_quota_remaining", "total_available"}},
		{output: "group_rate_multiplier", keys: []string{"group_rate_multiplier", "group_ratio"}},
		{output: "resolved_rate_multiplier", keys: []string{"resolved_rate_multiplier", "rate_multiplier", "ratio"}},
	} {
		if value, ok := firstProbeNumber(payload, mapping.keys...); ok {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("invalid newapi numeric value")
			}
			data[mapping.output] = value
			recognized = true
		}
	}
	if value, ok := firstProbeString(payload, "currency"); ok {
		data["currency"] = value
	}
	if value, ok := firstProbeBool(payload, "unlimited_quota"); ok {
		data["unlimited_quota"] = value
		recognized = true
	}
	if value, ok := firstProbeString(payload, "group", "group_name", "token_group"); ok {
		data["remote_group_name"] = value
		recognized = true
	}
	if value, ok := firstProbeScalar(payload, "group_id"); ok {
		data["remote_group_id"] = value
		recognized = true
	}
	if value, ok := firstProbeBool(payload, "group_exists", "remote_group_exists"); ok {
		data["remote_group_exists"] = value
		data["group_check_supported"] = true
		recognized = true
	}
	if value, ok := firstProbeString(payload, "group_status", "remote_group_status"); ok {
		data["remote_group_status"] = value
		recognized = true
	}
	if _, ok := payload["name"]; ok {
		recognized = true
	}
	if !recognized {
		return nil, fmt.Errorf("unrecognized newapi usage response")
	}

	if resolved, ok := data["resolved_rate_multiplier"].(float64); ok {
		if resolved < 0 {
			return nil, fmt.Errorf("invalid newapi multiplier")
		}
		data["billing_scope"] = "token"
		data["peak_rate_enabled"] = false
		data["effective_rate_multiplier"] = resolved
	} else if groupRate, ok := data["group_rate_multiplier"].(float64); ok {
		if groupRate < 0 {
			return nil, fmt.Errorf("invalid newapi multiplier")
		}
		data["billing_scope"] = "token"
		data["resolved_rate_multiplier"] = groupRate
		data["peak_rate_enabled"] = false
		data["effective_rate_multiplier"] = groupRate
	}
	return data, nil
}

func (s *UpstreamBillingProbeService) enrichNewAPIBalance(
	ctx context.Context,
	account *Account,
	proxyURL string,
	tlsProfile *tlsfingerprint.Profile,
	baseURL string,
	data map[string]any,
) {
	statusEndpoint := upstreamProbeEndpoint{provider: "newapi", path: "/api/status", newAPI: true}
	statusURL := buildUpstreamProbeEndpointURL(baseURL, statusEndpoint)
	result, reason := s.fetchUpstreamProbeEndpoint(ctx, account, proxyURL, tlsProfile, "", statusURL)
	if reason != "" || result == nil || result.statusCode < 200 || result.statusCode >= 300 {
		return
	}

	decoder := json.NewDecoder(bytes.NewReader(result.body))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil {
		return
	}
	if success, ok := root["success"].(bool); ok && !success {
		return
	}
	if code, ok := root["code"].(bool); ok && !code {
		return
	}
	payload := root
	if nested, ok := root["data"].(map[string]any); ok {
		payload = nested
	}

	quotaPerUnit, ok := firstProbeNumber(payload, "quota_per_unit")
	if !ok || quotaPerUnit <= 0 || math.IsNaN(quotaPerUnit) || math.IsInf(quotaPerUnit, 0) {
		return
	}
	data["quota_per_unit"] = quotaPerUnit
	if value, ok := firstProbeString(payload, "quota_display_type"); ok {
		data["quota_display_type"] = value
	}
	if value, ok := firstProbeNumber(payload, "usd_exchange_rate"); ok && value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
		data["usd_exchange_rate"] = value
	}
	if unlimited, _ := data["unlimited_quota"].(bool); unlimited {
		delete(data, "balance")
		delete(data, "currency")
		return
	}
	remaining, ok := data["key_quota_remaining"].(float64)
	if !ok || math.IsNaN(remaining) || math.IsInf(remaining, 0) {
		return
	}
	data["balance"] = remaining / quotaPerUnit
	data["currency"] = "USD"
}

func (s *UpstreamBillingProbeService) enrichNewAPIGroupCheck(
	ctx context.Context,
	account *Account,
	proxyURL string,
	tlsProfile *tlsfingerprint.Profile,
	apiKey string,
	baseURL string,
	data map[string]any,
) {
	if supported, _ := data["group_check_supported"].(bool); supported {
		return
	}
	modelsURL := buildOpenAIEndpointURL(baseURL, "/v1/models")
	result, _ := s.fetchUpstreamProbeEndpoint(ctx, account, proxyURL, tlsProfile, apiKey, modelsURL)
	if result == nil {
		return
	}
	if result.statusCode >= 200 && result.statusCode < 300 {
		data["group_check_supported"] = true
		data["group_routing_healthy"] = true
		data["remote_group_status"] = "usable"
		return
	}
	if result.statusCode == http.StatusBadRequest || result.statusCode == http.StatusForbidden {
		message := strings.ToLower(string(result.body))
		mentionsGroup := strings.Contains(message, "group") || strings.Contains(message, "\u5206\u7ec4")
		if mentionsGroup {
			data["group_check_supported"] = true
			data["group_routing_healthy"] = false
			data["remote_group_status"] = "unavailable"

			missing := strings.Contains(message, "not exist") ||
				strings.Contains(message, "deleted") ||
				strings.Contains(message, "deprecated") ||
				strings.Contains(message, "\u4e0d\u5b58\u5728") ||
				strings.Contains(message, "\u5df2\u88ab\u5f03\u7528")
			if missing {
				data["remote_group_exists"] = false
				data["remote_group_status"] = "missing"
				return
			}

			forbidden := strings.Contains(message, "forbidden") ||
				strings.Contains(message, "no access") ||
				strings.Contains(message, "unauthorized") ||
				strings.Contains(message, "\u65e0\u6743\u8bbf\u95ee")
			if forbidden {
				data["remote_group_status"] = "forbidden"
			}
		}
	}
}

func copyOptionalProbeNumber(target map[string]any, key string, value *float64) {
	if value != nil {
		target[key] = *value
	}
}

func firstProbeNumber(values map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		switch value := values[key].(type) {
		case json.Number:
			parsed, err := value.Float64()
			if err == nil {
				return parsed, true
			}
		case float64:
			return value, true
		case float32:
			return float64(value), true
		case int:
			return float64(value), true
		case int64:
			return float64(value), true
		}
	}
	return 0, false
}

func firstProbeString(values map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), true
		}
	}
	return "", false
}

func firstProbeBool(values map[string]any, keys ...string) (bool, bool) {
	for _, key := range keys {
		if value, ok := values[key].(bool); ok {
			return value, true
		}
	}
	return false, false
}

func firstProbeScalar(values map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		switch value := values[key].(type) {
		case string:
			if strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value), true
			}
		case json.Number:
			return value.String(), true
		case float64, int, int64:
			return value, true
		}
	}
	return nil, false
}

func parseUpstreamBillingProbeResponse(body []byte) (map[string]any, error) {
	var response upstreamBillingProbeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if response.Object != "sub2api.key_billing" || response.SchemaVersion != 1 || response.BillingScope != "token" {
		return nil, fmt.Errorf("unexpected billing response schema")
	}
	if response.GroupRateMultiplier == nil || response.ResolvedRateMultiplier == nil ||
		response.PeakRateEnabled == nil || response.EffectiveRateMultiplier == nil {
		return nil, fmt.Errorf("incomplete billing response")
	}
	for _, value := range []float64{
		*response.GroupRateMultiplier,
		*response.ResolvedRateMultiplier,
		*response.EffectiveRateMultiplier,
	} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("invalid billing multiplier")
		}
	}
	if response.UserRateMultiplier != nil && (*response.UserRateMultiplier < 0 || math.IsNaN(*response.UserRateMultiplier) || math.IsInf(*response.UserRateMultiplier, 0)) {
		return nil, fmt.Errorf("invalid user billing multiplier")
	}
	expectedResolved := *response.GroupRateMultiplier
	if response.UserRateMultiplier != nil {
		expectedResolved = *response.UserRateMultiplier
	}
	if !equalBillingMultiplier(*response.ResolvedRateMultiplier, expectedResolved) {
		return nil, fmt.Errorf("inconsistent resolved billing multiplier")
	}
	observedAt, err := time.Parse(time.RFC3339Nano, response.ObservedAt)
	if err != nil || observedAt.IsZero() {
		return nil, fmt.Errorf("invalid observed_at")
	}
	data := map[string]any{
		"object":                    response.Object,
		"schema_version":            response.SchemaVersion,
		"provider":                  "sub2api",
		"group_check_supported":     false,
		"billing_scope":             response.BillingScope,
		"group_rate_multiplier":     *response.GroupRateMultiplier,
		"resolved_rate_multiplier":  *response.ResolvedRateMultiplier,
		"peak_rate_enabled":         *response.PeakRateEnabled,
		"effective_rate_multiplier": *response.EffectiveRateMultiplier,
		"observed_at":               observedAt.UTC().Format(time.RFC3339Nano),
	}
	if response.UserRateMultiplier != nil {
		data["user_rate_multiplier"] = *response.UserRateMultiplier
	}
	if *response.PeakRateEnabled {
		if response.PeakStart == nil || response.PeakEnd == nil || response.Timezone == nil ||
			response.PeakRateMultiplier == nil || response.AppliedPeakMultiplier == nil ||
			*response.PeakStart == "" || *response.PeakEnd == "" || *response.Timezone == "" ||
			*response.PeakRateMultiplier < 0 || *response.AppliedPeakMultiplier < 0 ||
			math.IsNaN(*response.PeakRateMultiplier) || math.IsInf(*response.PeakRateMultiplier, 0) ||
			math.IsNaN(*response.AppliedPeakMultiplier) || math.IsInf(*response.AppliedPeakMultiplier, 0) {
			return nil, fmt.Errorf("incomplete peak billing response")
		}
		data["peak_start"] = *response.PeakStart
		data["peak_end"] = *response.PeakEnd
		data["peak_rate_multiplier"] = *response.PeakRateMultiplier
		data["applied_peak_multiplier"] = *response.AppliedPeakMultiplier
		data["timezone"] = *response.Timezone
	}
	appliedPeak, ok := upstreamBillingPeakMultiplierAt(data, observedAt)
	if !ok {
		return nil, fmt.Errorf("invalid peak billing response")
	}
	if response.PeakRateEnabled != nil && *response.PeakRateEnabled {
		if !equalBillingMultiplier(*response.AppliedPeakMultiplier, appliedPeak) {
			return nil, fmt.Errorf("inconsistent applied peak multiplier")
		}
	} else if response.AppliedPeakMultiplier != nil && !equalBillingMultiplier(*response.AppliedPeakMultiplier, 1) {
		return nil, fmt.Errorf("inconsistent applied peak multiplier")
	}
	if !equalBillingMultiplier(*response.EffectiveRateMultiplier, *response.ResolvedRateMultiplier*appliedPeak) {
		return nil, fmt.Errorf("inconsistent effective billing multiplier")
	}
	return data, nil
}

func upstreamBillingRateAt(data map[string]any, now time.Time) (float64, bool) {
	if scope, _ := data["billing_scope"].(string); scope != "token" {
		return 0, false
	}
	base, ok := resolveAccountExtraNumber(data, "resolved_rate_multiplier")
	if !ok || base < 0 || math.IsNaN(base) || math.IsInf(base, 0) {
		return 0, false
	}
	appliedPeak, ok := upstreamBillingPeakMultiplierAt(data, now)
	if !ok {
		return 0, false
	}
	base *= appliedPeak
	if math.IsNaN(base) || math.IsInf(base, 0) {
		return 0, false
	}
	return base, true
}

// upstreamBillingProbeSyncRate converts the declared multiplier into the value
// the automatic write-back may store in accounts.rate_multiplier, at the
// precision that column supports (DECIMAL(10,4)).
//
// It reads resolved_rate_multiplier, not effective_rate_multiplier: the
// effective value folds in the peak coefficient that happened to apply at the
// instant of the probe, so writing it would freeze one probe cycle's peak (or
// off-peak) factor into a static column, while display and scheduling
// recompute the peak factor for the current time through upstreamBillingRateAt.
//
// The accepted range is deliberately narrower than the column:
//   - 0 is rejected. accountCost multiplies the request cost by this value, so
//     an upstream-declared 0 would stop quota_used from ever growing and every
//     admin-configured account quota and cost alert would silently stop
//     working. Admins may still set 0 by hand; only the automatic path refuses.
//   - anything above upstreamBillingRateSyncMaxMultiplier is rejected.
//
// A rejected declaration leaves the current multiplier untouched; the probe
// still records an OK snapshot carrying the raw declaration for display.
func upstreamBillingProbeSyncRate(data map[string]any) (float64, bool) {
	value, ok := resolveAccountExtraNumber(data, "resolved_rate_multiplier")
	if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	rounded := math.Round(value*upstreamBillingProbeAccountRateScale) / upstreamBillingProbeAccountRateScale
	if rounded <= 0 || rounded > upstreamBillingRateSyncMaxMultiplier {
		return 0, false
	}
	return rounded, true
}

func upstreamBillingPeakMultiplierAt(data map[string]any, now time.Time) (float64, bool) {
	peakEnabled, ok := data["peak_rate_enabled"].(bool)
	if !ok {
		return 0, false
	}
	if !peakEnabled {
		return 1, true
	}

	start, startOK := data["peak_start"].(string)
	end, endOK := data["peak_end"].(string)
	timezoneName, timezoneOK := data["timezone"].(string)
	peakMultiplier, multiplierOK := resolveAccountExtraNumber(data, "peak_rate_multiplier")
	startMinute, validStart := parseMinutes(start)
	endMinute, validEnd := parseMinutes(end)
	if !startOK || !endOK || !timezoneOK || !multiplierOK || !validStart || !validEnd ||
		startMinute >= endMinute || peakMultiplier < 0 || math.IsNaN(peakMultiplier) || math.IsInf(peakMultiplier, 0) {
		return 0, false
	}
	location, err := time.LoadLocation(timezoneName)
	if err != nil {
		return 0, false
	}

	local := now.In(location)
	minute := local.Hour()*60 + local.Minute()
	if minute >= startMinute && minute < endMinute {
		return peakMultiplier, true
	}
	return 1, true
}

func equalBillingMultiplier(left, right float64) bool {
	if math.IsNaN(left) || math.IsNaN(right) || math.IsInf(left, 0) || math.IsInf(right, 0) {
		return false
	}
	scale := math.Max(1, math.Max(math.Abs(left), math.Abs(right)))
	return math.Abs(left-right) <= 1e-9*scale
}

func decodeUpstreamBillingProbeSnapshot(extra map[string]any) *UpstreamBillingProbeSnapshot {
	if extra == nil {
		return nil
	}
	value, ok := extra[UpstreamBillingProbeExtraKey]
	if !ok {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var snapshot UpstreamBillingProbeSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil || snapshot.Status == "" {
		return nil
	}
	if snapshot.Status != UpstreamBillingProbeStatusOK &&
		snapshot.Status != UpstreamBillingProbeStatusUnsupported &&
		snapshot.Status != UpstreamBillingProbeStatusFailed {
		return nil
	}
	return &snapshot
}

// IsUpstreamBillingProbeAccount reports whether an account belongs to the
// non-OAuth upstream account class managed by the billing probe.
func IsUpstreamBillingProbeAccount(account *Account) bool {
	return account != nil && !account.IsOAuth()
}

func isUpstreamBillingProbeAccount(account *Account) bool {
	return IsUpstreamBillingProbeAccount(account)
}

// IsUpstreamBillingProbeIdentity reports whether an account identity may opt
// in to the upstream billing probe. `/v1/sub2api/billing` is a key-scoped
// sub2api convention shared by the supported API-key platforms (including the
// CN providers, whose official-domain accounts are short-circuited to
// "unsupported" by upstreamBillingProbeTargetIsOfficialAPI).
// Non-sub2api upstreams return 404 and the snapshot records "unsupported".
// Only AccountTypeAPIKey is in scope. OAuth/Bedrock hold no static API key to
// present at all; AccountTypeUpstream (antigravity relay accounts) does carry
// a base_url plus a static api_key, but it is deliberately left out of the
// current supported set. New antigravity relay accounts are created with
// type=apikey by the admin form, so only pre-existing type=upstream rows
// cannot turn the probe on.
func IsUpstreamBillingProbeIdentity(platform, accountType string) bool {
	return accountType == AccountTypeAPIKey && domain.IsConcretePlatform(platform)
}

// upstreamBillingProbeOfficialAPIDomains lists the root domains of official
// provider APIs. The create form fills empty base_url values with official
// defaults (and offers official regional presets like us-east-1.api.x.ai),
// so probing them would send the account key to an official API path that
// cannot exist. Matching is by registrable root domain — exact host or any
// subdomain, after stripping the port and a trailing DNS dot — because no
// third-party sub2api relay can live under these domains, while custom
// relays (the only targets that can answer /v1/sub2api/billing) always do
// probe. OpenAI-platform accounts never reach this check: they keep the
// upstream-official behavior of probing api.openai.com.
// ollama.com is a first-class configuration here (Ollama Cloud accounts are
// platform openai/anthropic with base_url https://ollama.com/v1), and it is
// an official provider API just like the rest, so it belongs on this list.
// CN provider domains (moonshot.cn / kimi.com / bigmodel.cn / deepseek.com)
// serve the same role: official APIs that can never host /v1/sub2api/billing,
// so their accounts short-circuit to "unsupported" without a request.
var upstreamBillingProbeOfficialAPIDomains = []string{
	"anthropic.com",
	"googleapis.com",
	"x.ai",
	"grok.com",
	"openai.com",
	"ollama.com",
	"moonshot.cn",
	"kimi.com",
	"bigmodel.cn",
	"deepseek.com",
	"opencode.ai",
	"typesafe.ai",
	"commandcode.ai",
	"cline.bot",
}

func upstreamBillingProbeTargetIsOfficialAPI(baseURL string) bool {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return true
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "" {
		return true
	}
	for _, domain := range upstreamBillingProbeOfficialAPIDomains {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

func upstreamBillingProbeEnabled(account *Account) bool {
	if account == nil {
		return false
	}
	if account.Extra == nil {
		return true
	}
	enabled, ok := account.Extra[UpstreamBillingProbeEnabledExtraKey].(bool)
	return !ok || enabled
}

func upstreamBillingProbeConfigured(account *Account) bool {
	return account != nil &&
		strings.TrimSpace(account.GetCredential("api_key")) != "" &&
		strings.TrimSpace(account.GetCredential("base_url")) != ""
}

// upstreamBillingRateSyncEnabled is the probe-side pre-filter deciding whether
// a rate is even proposed for write-back. It is a necessary condition, not the
// authority: the repository CAS re-checks both switches against the row it
// updates, so a switch flipped between load and write can never sneak a rate in.
func upstreamBillingRateSyncEnabled(account *Account) bool {
	if account == nil || account.Extra == nil {
		return false
	}
	enabled, ok := account.Extra[UpstreamBillingRateSyncEnabledExtraKey].(bool)
	return ok && enabled && upstreamBillingProbeEnabled(account)
}

func (s *UpstreamBillingProbeService) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now()
	}
	return time.Now()
}

func nextProbeDelay(intervalMinutes int, retryAfterDuration time.Duration) time.Duration {
	if retryAfterDuration < 0 {
		retryAfterDuration = 0
	}
	if retryAfterDuration > upstreamBillingProbeMaxDelay {
		retryAfterDuration = upstreamBillingProbeMaxDelay
	}
	interval := time.Duration(intervalMinutes) * time.Minute
	if interval < upstreamBillingProbeMinIntervalMinutes*time.Minute {
		interval = upstreamBillingProbeMinIntervalMinutes * time.Minute
	}
	if interval > upstreamBillingProbeMaxDelay {
		interval = upstreamBillingProbeMaxDelay
	}
	// A one-minute interval must remain due on every runner tick. Positive
	// jitter would otherwise turn some refreshes into two-minute gaps.
	jitterRange := time.Duration(0)
	if interval > time.Minute {
		jitterRange = interval / 5
	}
	if jitterRange > 5*time.Minute {
		jitterRange = 5 * time.Minute
	}
	if jitterRange > 0 {
		interval += time.Duration(rand.Int64N(int64(jitterRange)*2+1)) - jitterRange
	}
	if retryAfterDuration > interval {
		// Retry-After is an explicit upstream instruction; honor it over the
		// local backoff interval, clamped to the same 24h ceiling.
		return retryAfterDuration
	}
	if interval > upstreamBillingProbeMaxDelay {
		return upstreamBillingProbeMaxDelay
	}
	return interval
}

// unsupportedProbeDelay 拉长 unsupported 账号的重探间隔，让无效候选自然退出
// 热队列，不再和真正接入 sub2api 的中转账号抢每周期的探测名额。
// 仍按 upstreamBillingProbeMaxDelay 封顶，保证上游后来接入 sub2api 时最迟一天
// 内会被重新发现；base 本身亦受 24h 封顶保护。
func unsupportedProbeDelay(intervalMinutes int, retryAfterDuration time.Duration) time.Duration {
	base := nextProbeDelay(intervalMinutes, retryAfterDuration)
	if base >= upstreamBillingProbeMaxDelay {
		return base
	}
	stretched := base * upstreamBillingProbeUnsupportedDelayFactor
	if stretched > upstreamBillingProbeMaxDelay {
		return upstreamBillingProbeMaxDelay
	}
	return stretched
}

func retryAfter(header http.Header, now time.Time) time.Duration {
	value := strings.TrimSpace(header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := at.Sub(now); delay > 0 {
			return delay
		}
	}
	return 0
}

func probeTimePtr(value time.Time) *time.Time {
	return &value
}

func safeProbeError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrUpstreamBillingProbeAccountInvalid) {
		return ErrUpstreamBillingProbeAccountInvalid.Error()
	}
	if errors.Is(err, ErrUpstreamBillingProbeUnavailable) {
		return ErrUpstreamBillingProbeUnavailable.Error()
	}
	return "probe_failed"
}
