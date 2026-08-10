package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
	"github.com/acaloiaro/neoq/testutils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ConcurrentWorkers = 8
)

var errPeriodicTimeout = errors.New("timed out waiting for periodic job")

// prepareAndCleanupDB should be run at the beginning of each test. It will check to see if the TEST_DATABASE_URL is
// present and has a valid connection string. If it does it will connect to the DB and clean up any jobs that might be
// lingering in the jobs table if that table exists. It will then return the connection string it found. If the
// connection string is not present then it will cause the current test to skip automatically. If the connection string
// is invalid or it cannot connect to the DB it will fail the current test.
func prepareAndCleanupDB(t *testing.T) (dbURL string, conn *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	dbURL = os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL environment variable is missing, test requires a PostgreSQL database to continue")
		return "", nil
	}

	var err error
	var poolConfig *pgxpool.Config
	poolConfig, err = pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatalf("unable to parse database url: '%s': %+v", dbURL, err)
		return dbURL, nil
	}
	poolConfig.MaxConns = 2

	conn, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("failed to connect to the database in TEST_DATABASE_URL: %+v", err)
		return dbURL, nil
	}

	// Delete everything in the neoq_jobs table if it exists
	// We don't _need_ to concern ourselves with an error here because the only way this query would fail is if the table
	// does not exist. Which is fine because anything within these tests would simply create that table immediately upon
	// starting.
	_, _ = conn.Exec(context.Background(), "DELETE FROM neoq_jobs") // nolint: gocritic

	// Since this is running at the beginning of each test, make sure that when the test is finished we clean up anything
	// we allocated here.
	t.Cleanup(func() {
		conn.Close()
	})

	// Return the conn url so that the calling test can use it.
	return dbURL, conn
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

// TestMigrationsRunWithPgxPoolParams ensures migrations succeed when the connection string contains pgxpool-specific
// parameters such as pool_max_conns. Those params are used by pgxpool but are not Postgres runtime parameters, so the
// migration connection must not forward them to the server. This is a regression test as we move migrations from lib/pq
// to  pgx/v5.
func TestMigrationsRunWithPgxPoolParams(t *testing.T) {
	connString, _ := prepareAndCleanupDB(t)

	// Guarantee a pgxpool-specific param is present regardless of what TEST_DATABASE_URL provides
	if !strings.Contains(connString, "pool_max_conns") {
		sep := "?"
		if strings.Contains(connString, "?") {
			sep = "&"
		}
		connString += sep + "pool_max_conns=5"
	}

	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatalf("migrations failed with pgxpool params in connection string: %v", err)
	}
	nq.Shutdown(ctx)
}

// TestMigrationsRunWithMigrationsTableParam ensures the golang-migrate x-migrations-table connection-string
// parameter is both accepted and does not reach Postgres as a runtime parameter (which would fail with "unrecognized
// configuration parameter").
func TestMigrationsRunWithMigrationsTableParam(t *testing.T) {
	connString, conn := prepareAndCleanupDB(t)

	sep := "?"
	if strings.Contains(connString, "?") {
		sep = "&"
	}
	connString += sep + "x-migrations-table=neoq_schema_migrations"

	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatalf("migrations failed with x-migrations-table in connection string: %v", err)
	}
	defer nq.Shutdown(ctx)

	// The worker pool must also tolerate the param: a successful query proves it wasn't forwarded to Postgres.
	if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("worker pool connection failed with x-migrations-table in connection string: %v", err)
	}
}

// TestBasicJobProcessing tests that the postgres backend is able to process the most basic jobs with the
// most basic configuration.
func TestBasicJobProcessing(t *testing.T) {
	connString, conn := prepareAndCleanupDB(t)
	const queue = "testing"
	maxRetries := 5
	done := make(chan bool)
	defer close(done)

	timeoutTimer := time.After(5 * time.Second)

	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	deadline := time.Now().UTC().Add(5 * time.Second)
	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		Deadline:   &deadline,
		MaxRetries: &maxRetries,
	})
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e)
	}

	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}
	if err != nil {
		t.Error(err)
	}

	// ensure job has fields set correctly
	var jdl time.Time
	var jmxrt int

	err = conn.
		QueryRow(context.Background(), "SELECT deadline,max_retries FROM neoq_jobs WHERE id = $1", jid).
		Scan(&jdl, &jmxrt)
	if err != nil {
		t.Error(err)
	}

	jdl = jdl.In(time.UTC)
	// dates from postgres come out with only 6 decimal places of millisecond precision, naively format dates as
	// strings for comparison reasons. Ref https://www.postgresql.org/docs/current/datatype-datetime.html
	if jdl.Format(time.RFC3339) != deadline.Format(time.RFC3339) {
		t.Error(fmt.Errorf("job deadline does not match its expected value: %v != %v", jdl, deadline)) // nolint: goerr113
	}

	if jmxrt != maxRetries {
		t.Error(fmt.Errorf("job MaxRetries does not match its expected value: %v != %v", jmxrt, maxRetries)) // nolint: goerr113
	}
}

func TestMultipleProcessors(t *testing.T) {
	const queue = "testing"
	var execCount uint32
	var wg sync.WaitGroup

	connString, _ := prepareAndCleanupDB(t)

	h := handler.New(queue, func(_ context.Context) (err error) {
		atomic.AddUint32(&execCount, 1)
		wg.Done()
		return
	})
	// Make sure that each neoq worker only works on one thing at a time.
	h.Concurrency = 1

	neos := make([]neoq.Neoq, 0, ConcurrentWorkers)
	// Create several neoq processors such that we can enqueue several jobs and have them consumed by multiple different
	// workers. We want to make sure that a job is not processed twice in a pool of many different neoq workers.
	for i := 0; i < ConcurrentWorkers; i++ {
		ctx := context.Background()
		nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			nq.Shutdown(ctx)
		})

		err = nq.Start(ctx, h)
		if err != nil {
			t.Error(err)
		}

		neos = append(neos, nq)
	}

	// From one of the neoq clients, enqueue several jobs. At least one per processor registered above.
	nq := neos[0]
	wg.Add(ConcurrentWorkers)
	for i := 0; i < ConcurrentWorkers; i++ {
		ctx := context.Background()
		deadline := time.Now().UTC().Add(10 * time.Second)
		jid, e := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("hello world: %d", i),
			},
			Deadline: &deadline,
		})
		if e != nil || jid == jobs.DuplicateJobID {
			t.Error(e)
		}
	}

	// Wait for all jobs to complete.
	wg.Wait()

	// Make sure that we executed the expected number of jobs.
	if atomic.LoadUint32(&execCount) != uint32(ConcurrentWorkers) {
		t.Fatalf("mismatch number of executions. Expected: %d Found: %d", ConcurrentWorkers, execCount)
	}
}

// TestDuplicateJobRejection tests that the backend rejects jobs that are duplicates
func TestDuplicateJobRejection(t *testing.T) {
	const queue = "testing"

	connString, _ := prepareAndCleanupDB(t)

	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})
	if e != nil || jid == jobs.DuplicateJobID {
		err = e
	}

	_, e2 := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})

	// we submitted two duplicate jobs; the error should be a duplicate job error
	if !errors.Is(e2, postgres.ErrDuplicateJob) {
		err = e2
	}

	if err != nil {
		t.Error(err)
	}
}

// TestBasicJobMultipleQueue tests that the postgres backend is able to process jobs on multiple queues
func TestBasicJobMultipleQueue(t *testing.T) {
	const queue = "testing"
	const queue2 = "testing2"
	done := make(chan bool)
	doneCnt := 0

	timeoutTimer := time.After(5 * time.Second)

	connString, _ := prepareAndCleanupDB(t)

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelDebug),
		postgres.WithConnectionTimeout(1*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		return
	})

	h2 := handler.New(queue2, func(_ context.Context) (err error) {
		done <- true
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	err = nq.Start(ctx, h2)
	if err != nil {
		t.Error(err)
	}

	go func() {
		jid, e := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("hello world: %d", internal.RandInt(10000000000)),
			},
		})
		if e != nil || jid == jobs.DuplicateJobID {
			t.Error(e)
		}

		jid2, e := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue2,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("hello world: %d", internal.RandInt(10000000000)),
			},
		})
		if e != nil || jid2 == jobs.DuplicateJobID {
			t.Error(e)
		}
	}()

results_loop:
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			break results_loop
		case <-done:
			doneCnt++
			if doneCnt == 2 {
				break results_loop
			}
		}
	}

	if err != nil {
		t.Error(err)
	}
}

func TestCron(t *testing.T) {
	done := make(chan bool, 1)
	defer close(done)
	const cron = "* * * * * *"
	connString, _ := prepareAndCleanupDB(t)

	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.NewPeriodic(func(ctx context.Context) (err error) {
		done <- true
		return
	})

	h.WithOptions(
		handler.JobTimeout(500*time.Millisecond),
		handler.Concurrency(1),
	)

	err = nq.StartCron(ctx, cron, h)
	if err != nil {
		t.Error(err)
	}

	// allow time for listener to start
	time.Sleep(5 * time.Millisecond)

	select {
	case <-time.After(1 * time.Second):
		err = errPeriodicTimeout
	case <-done:
	}

	if err != nil {
		t.Error(err)
	}
}

func TestMultipleCronNodes(t *testing.T) {
	jobsProcessed := sync.Map{}
	const cron = "* * * * * *"
	connString, _ := prepareAndCleanupDB(t)

	workers := make([]neoq.Neoq, ConcurrentWorkers)
	var jobsCompleted uint32
	var duplicateJobs uint32
	for i := 0; i < ConcurrentWorkers; i++ {
		ctx := context.TODO()
		nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			nq.Shutdown(ctx)
		})
		h := handler.NewPeriodic(func(ctx context.Context) (err error) {
			job, err := jobs.FromContext(ctx)
			if err != nil {
				t.Fatalf("failed to extract job details from context: %+v", err)
				return nil
			}
			_, exists := jobsProcessed.LoadOrStore(job.ID, "foo")
			if exists {
				t.Fatalf("job (%d) has already been processed by another worker!", job.ID)
			}
			atomic.AddUint32(&jobsCompleted, 1)
			return
		})

		h.WithOptions(
			handler.JobTimeout(500*time.Millisecond),
			handler.Concurrency(1),
		)

		err = nq.StartCron(ctx, cron, h)
		if err != nil {
			t.Error(err)
		}

		workers[i] = nq
	}

	const WaitForJobTime = 1100 * time.Millisecond

	// allow time for listener to start and for at least one job to process
	time.Sleep(WaitForJobTime)
	if atomic.LoadUint32(&jobsCompleted) == 0 {
		t.Fatalf("no jobs were completed after %v", WaitForJobTime)
	}

	if atomic.LoadUint32(&duplicateJobs) > 0 {
		t.Fatalf("some jobs were processed more than once")
	}
}

// TestBasicJobProcessingWithErrors tests that the postgres backend is able to update the status of jobs that fail
func TestBasicJobProcessingWithErrors(t *testing.T) {
	const queue = "testing"
	logsChan := make(chan string, 100)
	timeoutTimer := time.After(5 * time.Second)
	connString, _ := prepareAndCleanupDB(t)

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelError))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		err = errors.New("something bad happened") // nolint: goerr113
		return
	})

	nq.SetLogger(testutils.NewTestLogger(logsChan))

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e)
	}

results_loop:
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			break results_loop
		case actualLogMsg := <-logsChan:
			expectedLogMsg := "job failed to process: something bad happened"
			if strings.Contains(actualLogMsg, expectedLogMsg) {
				err = nil
				break results_loop
			}
			err = fmt.Errorf("'%s' NOT CONTAINS '%s'", actualLogMsg, expectedLogMsg) //nolint:all
		}
	}

	if err != nil {
		t.Error(err)
	}
}

// Test_MoveJobsToDeadQueue tests that when a job's MaxRetries is reached, that the job is moved ot the dead queue successfully
// https://github.com/acaloiaro/neoq/issues/98
func Test_MoveJobsToDeadQueue(t *testing.T) {
	connString, conn := prepareAndCleanupDB(t)
	const queue = "testing"
	maxRetries := 0
	done := make(chan bool)
	defer close(done)

	timeoutTimer := time.After(5 * time.Second)

	ctx := context.Background()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		postgres.WithTransactionTimeout(60000))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		panic("no good")
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		MaxRetries: &maxRetries,
	})
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e)
	}

	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}
	if err != nil {
		t.Error(err)
	}

	// ensure job has fields set correctly
	maxWait := time.Now().Add(30 * time.Second)
	var status string
	for {
		if time.Now().After(maxWait) {
			break
		}

		err = conn.
			QueryRow(context.Background(), "SELECT status FROM neoq_dead_jobs WHERE id = $1", jid).
			Scan(&status)

		if err == nil {
			break
		}

		if err != nil && errors.Is(err, pgx.ErrNoRows) {
			time.Sleep(50 * time.Millisecond)
			continue
		} else if err != nil {
			t.Error(err)
		}
	}

	if status != internal.JobStatusFailed {
		t.Error("should be dead")
	}
}

func TestJobEnqueuedSeparately(t *testing.T) {
	connString, _ := prepareAndCleanupDB(t)
	const queue = "SyncThing"
	maxRetries := 5
	done := make(chan bool)
	defer close(done)

	timeoutTimer := time.After(5 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	enqueuer, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		postgres.WithSynchronousCommit(false),
		neoq.WithLogLevel(logging.LogLevelDebug),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer enqueuer.Shutdown(ctx)

	consumer, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		postgres.WithSynchronousCommit(false),
		neoq.WithLogLevel(logging.LogLevelDebug),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Shutdown(ctx)
	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		return
	})

	err = consumer.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	// Wait a bit more before enqueueing
	jid, e := enqueuer.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		MaxRetries: &maxRetries,
	})
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e)
	}

	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}
	if err != nil {
		t.Error(err)
	}
}

// TestBasicJobMultipleQueueWithError tests that the postgres backend is able to process jobs on multiple queues
// and retries occur
// https://github.com/acaloiaro/neoq/issues/98
// nolint: gocyclo
func TestBasicJobMultipleQueueWithError(t *testing.T) {
	connString, conn := prepareAndCleanupDB(t)
	const queue = "testing"
	const queue2 = "testing2"

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		neoq.WithLogLevel(logging.LogLevelDebug),
		postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		return
	})

	h2 := handler.New(queue2, func(_ context.Context) (err error) {
		panic("no good")
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	err = nq.Start(ctx, h2)
	if err != nil {
		t.Error(err)
	}

	job2Chan := make(chan string, 100)
	go func() {
		jid, err := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("should not fail: %d", internal.RandInt(10000000000)),
			},
		})
		if err != nil || jid == jobs.DuplicateJobID {
			t.Error(err)
		}

		maxRetries := 1
		jid2, err := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue2,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("should fail: %d", internal.RandInt(10000000000)),
			},
			MaxRetries: &maxRetries,
		})
		if err != nil || jid2 == jobs.DuplicateJobID {
			t.Error(err)
		}

		job2Chan <- jid2
	}()

	// wait for the job to process before waiting for updates
	jid2 := <-job2Chan

	// ensure job has fields set correctly
	maxWait := time.Now().Add(30 * time.Second)
	var status string
	for {
		if time.Now().After(maxWait) {
			break
		}

		err = conn.
			QueryRow(context.Background(), "SELECT status FROM neoq_dead_jobs WHERE id = $1", jid2).
			Scan(&status)

		if err == nil {
			break
		}

		// jid2 is empty until the job gets queued
		if jid2 == "" || err != nil && errors.Is(err, pgx.ErrNoRows) {
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err != nil {
			t.Error(err)
			return
		}
	}

	if status != internal.JobStatusFailed {
		t.Error("should be dead")
	}
}

// Test_MoveJobsToDeadQueue tests that when a job's MaxRetries is reached, that the job is moved ot the dead queue successfully
// https://github.com/acaloiaro/neoq/issues/98
func Test_ConnectionTimeout(t *testing.T) {
	connString, _ := prepareAndCleanupDB(t)

	const queue = "testing"
	done := make(chan bool)
	defer close(done)

	ctx := context.Background()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		postgres.WithConnectionTimeout(0*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		_, err = nq.Enqueue(ctx, &jobs.Job{Queue: queue})
		done <- true
	}()

	timeoutTimer := time.After(5 * time.Second)
	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}

	if !errors.Is(err, postgres.ErrExceededConnectionPoolTimeout) {
		t.Error(err)
	}
}

// TestBasicJobProcessing tests that the postgres backend is able to process the most basic jobs with the
// most basic configuration.
func TestFutureJobProcessing(t *testing.T) {
	connString, _ := prepareAndCleanupDB(t)
	const queue = "testing"
	done := make(chan bool)
	defer close(done)

	timeoutTimer := time.After(5 * time.Second)

	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}
	job := &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		RunAfter: time.Now().Add(time.Second * 1),
	}
	jid, e := nq.Enqueue(ctx, job)
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e)
	}

	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}
	if err != nil {
		t.Error(err)
	}

	if time.Now().Before(job.RunAfter) {
		t.Error("job ran before RunAfter")
	}
}

// TestJobWithPastDeadline ensures that when a job is scheduled and its deadline is in the past, that the job is updated
// with an error indicating that its deadline was not met
// https://github.com/acaloiaro/neoq/issues/123
func TestJobWithPastDeadline(t *testing.T) {
	connString, conn := prepareAndCleanupDB(t)
	const queue = "testing"
	timeoutTimer := time.After(5 * time.Second)
	maxRetries := 5
	done := make(chan bool)
	defer close(done)

	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	// deadline in the past
	deadline := time.Now().UTC().Add(time.Duration(-5) * time.Second)
	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		Deadline:   &deadline,
		MaxRetries: &maxRetries,
	})
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e) // nolint: goerr113
	}

	if e != nil && !errors.Is(e, jobs.ErrJobExceededDeadline) {
		t.Error(err) // nolint: goerr113
	}

	var status string
	go func() {
		// ensure job has failed/has the correct status
		for {
			err = conn.
				QueryRow(context.Background(), "SELECT status FROM neoq_jobs WHERE id = $1", jid).
				Scan(&status)
			if err != nil {
				break
			}

			if status == internal.JobStatusFailed {
				done <- true
				break
			}

			time.Sleep(50 * time.Millisecond)
		}
	}()

	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}
	if err != nil {
		t.Errorf("job should have resulted in a status of 'failed', but its status is %s", status)
	}
}

func TestHandlerRecoveryCallback(t *testing.T) {
	connString, _ := prepareAndCleanupDB(t)
	const queue = "testing"
	timeoutTimer := time.After(5 * time.Second)
	recoveryFuncCalled := make(chan bool, 1)
	defer close(recoveryFuncCalled)

	ctx := context.Background()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		neoq.WithRecoveryCallback(func(ctx context.Context, _ error) (err error) {
			recoveryFuncCalled <- true
			return
		}))

	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(ctx context.Context) (err error) {
		panic("abort mission!")
	})
	h.WithOptions(
		handler.JobTimeout(500*time.Millisecond),
		handler.Concurrency(1),
	)

	// process jobs on the test queue
	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	jid, err := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})
	if err != nil || jid == jobs.DuplicateJobID {
		t.Fatal("job was not enqueued. either it was duplicate or this error caused it:", err)
	}

	select {
	case <-timeoutTimer:
		err = errors.New("timed out waiting for job") // nolint: goerr113
		return
	case <-recoveryFuncCalled:
		break
	}

	if err != nil {
		t.Error(err)
	}
}

// TestProcessPendingJobs tests that unanounced jobs with a run_after before the current timestamp get run periodically
// This ensures that when LISTENER connections fails, that jobs that would have been announced still get processed eventually
func TestProcessPendingJobs(t *testing.T) {
	connString, conn := prepareAndCleanupDB(t)
	const queue = "testing"
	const queue2 = "testing2"
	timeoutTimer := time.After(5 * time.Second)
	done := make(chan bool)
	defer close(done)

	ctx := context.Background()

	nq, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		neoq.WithPendingJobCheckInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	// Adding jobs into the job queue before noeq is listening on any queues ensures that the new job is not announced, and when
	// neoq _is_ started, that there is a pending jobs waiting to be processed
	payload := map[string]interface{}{
		"message": "hello world",
	}
	var pendingJobID string
	pendingJobID, err = nq.Enqueue(ctx, &jobs.Job{
		Queue:   queue,
		Payload: payload,
	})
	if err != nil {
		t.Error(fmt.Errorf("unable to add job to queue: %w", err))
		return
	}

	h := handler.New(queue, func(_ context.Context) (err error) {
		return
	})

	h2 := handler.New(queue2, func(_ context.Context) (err error) {
		return
	})

	// Start ensures that pending jobs will be processed
	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	// Start listening on a second queued to make sure that pending jobs can be pulled from multiple queues
	err = nq.Start(ctx, h2)
	if err != nil {
		t.Error(err)
	}

	go func() {
		var err error
		var status string
		// ensure job has failed/has the correct status
		for {
			err = conn.
				QueryRow(context.Background(), "SELECT status FROM neoq_jobs WHERE id = $1", pendingJobID).
				Scan(&status)
			if err != nil {
				break
			}

			if status != internal.JobStatusNew {
				done <- true
				break
			}

			time.Sleep(50 * time.Millisecond)
		}
	}()

	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}
	if err != nil {
		t.Errorf("job should have resulted in a status of 'processed', but did not")
	}
}
