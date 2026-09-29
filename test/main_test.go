package test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/maxpert/marmot/protocol"
)

// testDataRoot holds every run's clusters and binaries: Marmot's runtime
// files live under /tmp/marmot.
const testDataRoot = "/tmp/marmot/test"

// buildTags are the tags every Marmot binary is built with.
const buildTags = "sqlite_preupdate_hook sqlite_fts5 sqlite_json sqlite_math_functions sqlite_foreign_keys sqlite_stat4 sqlite_vacuum_incr"

// preLogPullCommit is the last commit before nodes pulled each other's commit
// logs: the release a rolling upgrade to the log-pull protocol starts from.
const preLogPullCommit = "7a66596"

var (
	// runDir is this package run's directory: one subdirectory per test.
	runDir string
	// marmotBin is the binary built from the tree under test.
	marmotBin string
	// preLogPullBin is the preLogPullCommit binary.
	preLogPullBin string
)

// TestMain builds the binaries every cluster test runs, once per package
// run, so no test pays for a build.
func TestMain(m *testing.M) {
	if os.Getenv("MARMOT_RUN_CLUSTER_INTEGRATION_TESTS") != "1" {
		fmt.Fprintln(os.Stderr, "skipping cluster integration tests; set MARMOT_RUN_CLUSTER_INTEGRATION_TESTS=1 to run")
		os.Exit(0)
	}
	if err := protocol.InitializePipeline(10000, nil); err != nil {
		fmt.Fprintln(os.Stderr, "initialize query pipeline:", err)
		os.Exit(1)
	}
	// A re-exec'd kill -9 child (TestClaimRangeKillNineChild) runs one
	// in-process test in its parent's directories: it builds nothing and owns
	// no run directory.
	if os.Getenv(killNineDataRootEnv) != "" {
		os.Exit(m.Run())
	}
	if err := setUp(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	if code == 0 {
		_ = os.RemoveAll(runDir)
	}
	os.Exit(code)
}

// setUp creates runDir and builds marmotBin and preLogPullBin.
func setUp() error {
	if err := os.MkdirAll(testDataRoot, 0o755); err != nil {
		return err
	}
	dir, err := os.MkdirTemp(testDataRoot, time.Now().Format("run-20060102-150405-"))
	if err != nil {
		return err
	}
	runDir = dir
	marmotBin = filepath.Join(runDir, "marmot")
	if err := build(repoRoot(), marmotBin); err != nil {
		return err
	}
	preLogPullBin, err = buildPreLogPull()
	return err
}

// repoRoot is the module root, taken from this file's own path so the
// harness builds the tree it is compiled from.
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}

// build builds the Marmot binary from src into bin.
func build(src, bin string) error {
	cmd := exec.Command("go", "build", "-tags", buildTags, "-o", bin, ".")
	cmd.Dir = src
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build %s: %v\n%s", src, err, out)
	}
	return nil
}

// buildPreLogPull returns the preLogPullCommit binary, building it from a
// read-only `git archive` of that commit unless a previous run left it under
// testDataRoot.
func buildPreLogPull() (string, error) {
	dir := filepath.Join(testDataRoot, "marmot_pref2_"+preLogPullCommit)
	bin := filepath.Join(dir, "marmot")
	if _, err := os.Stat(bin); err == nil {
		return bin, nil
	}
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		return "", err
	}
	archive := exec.Command("sh", "-c", fmt.Sprintf("git archive %s | tar -x -C %s", preLogPullCommit, src))
	archive.Dir = repoRoot()
	if out, err := archive.CombinedOutput(); err != nil {
		return "", fmt.Errorf("git archive %s: %v\n%s", preLogPullCommit, err, out)
	}
	return bin, build(src, bin)
}
