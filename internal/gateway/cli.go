package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"distributed-kv-store/internal/redundancy"

	pb "distributed-kv-store/proto"
)

// Service is the client surface the CLI needs. It lets command parsing and
// rendering be tested against a fake without a gRPC transport.
type Service interface {
	Put(ctx context.Context, req *pb.PutRequest) (string, *pb.PutResponse, error)
	Get(ctx context.Context, key string) (string, *pb.GetResponse, error)
	Delete(ctx context.Context, req *pb.DeleteRequest) (string, *pb.DeleteResponse, error)
	GetConfig(ctx context.Context) (string, *pb.ConfigResponse, error)
	SetConfig(ctx context.Context, mode string) (string, *pb.ConfigResponse, error)
	ClusterHealth(ctx context.Context) (string, *pb.ClusterHealthResponse, error)
	Stats(ctx context.Context) (string, *pb.StatsResponse, error)
	Repair(ctx context.Context, req *pb.RepairRequest) (string, *pb.RepairResponse, error)
}

// CLI parses commands, calls the gateway client, and renders the result.
type CLI struct {
	svc    Service
	out    io.Writer
	errOut io.Writer
}

// NewCLI builds a CLI bound to a gateway service and its output streams.
func NewCLI(svc Service, out, errOut io.Writer) *CLI {
	return &CLI{svc: svc, out: out, errOut: errOut}
}

// Run executes one command. A nil error means exit status zero; help returns
// nil as well. Any returned error should be printed by the caller and mapped to
// a non-zero exit status.
func (c *CLI) Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(c.out, Usage())
		return errors.New("a command is required")
	}

	switch args[0] {
	case "help", "--help", "-h":
		fmt.Fprint(c.out, Usage())
		return nil
	case "put":
		return c.runPut(ctx, args[1:])
	case "get":
		return c.runGet(ctx, args[1:])
	case "delete":
		return c.runDelete(ctx, args[1:])
	case "mode":
		return c.runMode(ctx, args[1:])
	case "status":
		return c.runStatus(ctx, args[1:])
	case "health":
		return c.runHealth(ctx, args[1:])
	case "stats":
		return c.runStats(ctx, args[1:])
	case "repair":
		return c.runRepair(ctx, args[1:])
	default:
		fmt.Fprint(c.out, Usage())
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// flagDef describes one --flag accepted by a command.
type flagDef struct {
	takesValue bool
	set        func(string) error
}

// parseInterspersed parses flags wherever they appear, so both
// "put --mode rs32 k v" and "put k v --mode rs32" work. A bare -- ends flag
// parsing. Non-flag arguments are returned in order.
func parseInterspersed(args []string, defs map[string]flagDef) ([]string, error) {
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if arg == "-h" {
			arg = "--help"
		}
		if !strings.HasPrefix(arg, "--") {
			positional = append(positional, arg)
			continue
		}

		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		def, ok := defs[name]
		if !ok {
			return nil, fmt.Errorf("unknown flag --%s", name)
		}
		if def.takesValue {
			if !hasValue {
				i++
				if i >= len(args) {
					return nil, fmt.Errorf("flag --%s requires a value", name)
				}
				value = args[i]
			}
			if err := def.set(value); err != nil {
				return nil, err
			}
			continue
		}
		if hasValue {
			return nil, fmt.Errorf("flag --%s does not take a value", name)
		}
		if def.set != nil {
			if err := def.set(""); err != nil {
				return nil, err
			}
		}
	}
	return positional, nil
}

func boolFlag(target *bool) flagDef {
	return flagDef{set: func(string) error {
		*target = true
		return nil
	}}
}

func stringFlag(target *string) flagDef {
	return flagDef{takesValue: true, set: func(value string) error {
		*target = value
		return nil
	}}
}

func versionFlag(target *int64) flagDef {
	return flagDef{takesValue: true, set: func(value string) error {
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || parsed < 0 {
			return fmt.Errorf("--version must be a non-negative integer, got %q", value)
		}
		*target = parsed
		return nil
	}}
}

func validateMode(mode string) error {
	parsed, err := redundancy.ParseMode(mode)
	if err != nil {
		return err
	}
	if !parsed.Valid() {
		return fmt.Errorf("invalid mode %q: expected rs32 or replication", mode)
	}
	return nil
}

func (c *CLI) runPut(ctx context.Context, args []string) error {
	var (
		mode      string
		version   int64
		valueFile string
		asJSON    bool
		help      bool
	)
	positional, err := parseInterspersed(args, map[string]flagDef{
		"mode":       stringFlag(&mode),
		"version":    versionFlag(&version),
		"value-file": stringFlag(&valueFile),
		"json":       boolFlag(&asJSON),
		"help":       boolFlag(&help),
	})
	if err != nil {
		return err
	}
	if help {
		fmt.Fprint(c.out, commandUsage("put"))
		return nil
	}
	if len(positional) == 0 {
		return errors.New("put requires a key")
	}
	if mode != "" {
		if err := validateMode(mode); err != nil {
			return err
		}
	}

	key := positional[0]
	var value []byte
	if valueFile != "" {
		if len(positional) > 1 {
			return errors.New("put accepts either a positional value or --value-file, not both")
		}
		value, err = os.ReadFile(valueFile)
		if err != nil {
			return fmt.Errorf("read value file %s: %w", valueFile, err)
		}
	} else {
		if len(positional) != 2 {
			return errors.New("put requires a key and value (or use --value-file)")
		}
		value = []byte(positional[1])
	}

	node, resp, err := c.svc.Put(ctx, &pb.PutRequest{
		Key:             key,
		Value:           value,
		Mode:            mode,
		ExpectedVersion: version,
	})
	if err != nil {
		return fmt.Errorf("put failed: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("put rejected by %s: %s", node, resp.GetMessage())
	}
	return renderPut(c.out, node, key, resp, asJSON)
}

func (c *CLI) runGet(ctx context.Context, args []string) error {
	var (
		asJSON bool
		help   bool
	)
	positional, err := parseInterspersed(args, map[string]flagDef{
		"json": boolFlag(&asJSON),
		"help": boolFlag(&help),
	})
	if err != nil {
		return err
	}
	if help {
		fmt.Fprint(c.out, commandUsage("get"))
		return nil
	}
	if len(positional) != 1 {
		return errors.New("get requires exactly one key")
	}

	node, resp, err := c.svc.Get(ctx, positional[0])
	if err != nil {
		return fmt.Errorf("get failed: %w", err)
	}
	return renderGet(c.out, c.errOut, node, positional[0], resp, asJSON)
}

func (c *CLI) runDelete(ctx context.Context, args []string) error {
	var (
		version int64
		asJSON  bool
		help    bool
	)
	positional, err := parseInterspersed(args, map[string]flagDef{
		"version": versionFlag(&version),
		"json":    boolFlag(&asJSON),
		"help":    boolFlag(&help),
	})
	if err != nil {
		return err
	}
	if help {
		fmt.Fprint(c.out, commandUsage("delete"))
		return nil
	}
	if len(positional) != 1 {
		return errors.New("delete requires exactly one key")
	}

	key := positional[0]
	node, resp, err := c.svc.Delete(ctx, &pb.DeleteRequest{Key: key, ExpectedVersion: version})
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("delete rejected by %s: %s", node, resp.GetMessage())
	}
	return renderDelete(c.out, node, key, resp, asJSON)
}

func (c *CLI) runMode(ctx context.Context, args []string) error {
	var (
		asJSON bool
		help   bool
	)
	positional, err := parseInterspersed(args, map[string]flagDef{
		"json": boolFlag(&asJSON),
		"help": boolFlag(&help),
	})
	if err != nil {
		return err
	}
	if help {
		fmt.Fprint(c.out, commandUsage("mode"))
		return nil
	}
	if len(positional) > 1 {
		return errors.New("mode accepts at most one argument: rs32 or replication")
	}

	if len(positional) == 0 {
		node, resp, err := c.svc.GetConfig(ctx)
		if err != nil {
			return fmt.Errorf("read mode: %w", err)
		}
		if !resp.GetSuccess() {
			return fmt.Errorf("read mode rejected by %s: %s", node, resp.GetMessage())
		}
		return renderMode(c.out, node, resp, false, asJSON)
	}

	mode := positional[0]
	if err := validateMode(mode); err != nil {
		return err
	}
	node, resp, err := c.svc.SetConfig(ctx, mode)
	if err != nil {
		return fmt.Errorf("set mode: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("set mode rejected by %s: %s", node, resp.GetMessage())
	}
	return renderMode(c.out, node, resp, true, asJSON)
}

func (c *CLI) runStatus(ctx context.Context, args []string) error {
	var (
		asJSON bool
		help   bool
	)
	positional, err := parseInterspersed(args, map[string]flagDef{
		"json": boolFlag(&asJSON),
		"help": boolFlag(&help),
	})
	if err != nil {
		return err
	}
	if help {
		fmt.Fprint(c.out, commandUsage("status"))
		return nil
	}
	if len(positional) != 0 {
		return errors.New("status takes no arguments")
	}

	node, resp, err := c.svc.ClusterHealth(ctx)
	if err != nil {
		return fmt.Errorf("status failed: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("status rejected by %s: %s", node, resp.GetMessage())
	}
	return renderStatus(c.out, node, resp, asJSON)
}

func (c *CLI) runHealth(ctx context.Context, args []string) error {
	var (
		asJSON bool
		help   bool
	)
	positional, err := parseInterspersed(args, map[string]flagDef{
		"json": boolFlag(&asJSON),
		"help": boolFlag(&help),
	})
	if err != nil {
		return err
	}
	if help {
		fmt.Fprint(c.out, commandUsage("health"))
		return nil
	}
	if len(positional) != 0 {
		return errors.New("health takes no arguments")
	}

	node, resp, err := c.svc.ClusterHealth(ctx)
	if err != nil {
		return fmt.Errorf("health failed: %w", err)
	}
	healthy := clusterHealthy(resp)
	if err := renderHealth(c.out, node, resp, healthy, asJSON); err != nil {
		return err
	}
	if !healthy {
		return fmt.Errorf("cluster is not healthy: %s", resp.GetMessage())
	}
	return nil
}

func (c *CLI) runStats(ctx context.Context, args []string) error {
	var (
		asJSON bool
		help   bool
	)
	positional, err := parseInterspersed(args, map[string]flagDef{
		"json": boolFlag(&asJSON),
		"help": boolFlag(&help),
	})
	if err != nil {
		return err
	}
	if help {
		fmt.Fprint(c.out, commandUsage("stats"))
		return nil
	}
	if len(positional) != 0 {
		return errors.New("stats takes no arguments")
	}

	node, resp, err := c.svc.Stats(ctx)
	if err != nil {
		return fmt.Errorf("stats failed: %w", err)
	}
	return renderStats(c.out, node, resp, asJSON)
}

func (c *CLI) runRepair(ctx context.Context, args []string) error {
	var (
		stripeID string
		force    bool
		asJSON   bool
		help     bool
	)
	positional, err := parseInterspersed(args, map[string]flagDef{
		"stripe": stringFlag(&stripeID),
		"force":  boolFlag(&force),
		"json":   boolFlag(&asJSON),
		"help":   boolFlag(&help),
	})
	if err != nil {
		return err
	}
	if help {
		fmt.Fprint(c.out, commandUsage("repair"))
		return nil
	}
	if len(positional) != 1 {
		return errors.New("repair requires exactly one key")
	}

	key := positional[0]
	node, resp, err := c.svc.Repair(ctx, &pb.RepairRequest{
		Key:      key,
		StripeId: stripeID,
		Force:    force,
	})
	if err != nil {
		return fmt.Errorf("repair failed: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("repair rejected by %s: %s", node, resp.GetMessage())
	}
	return renderRepair(c.out, node, key, stripeID, force, resp, asJSON)
}

// clusterHealthy is the script-facing health definition: every voter is
// reachable, a leader is known, and no committed stripe is degraded.
func clusterHealthy(resp *pb.ClusterHealthResponse) bool {
	return resp.GetSuccess() &&
		resp.GetLeaderId() != "" &&
		resp.GetTotalNodes() > 0 &&
		resp.GetHealthyNodes() == resp.GetTotalNodes() &&
		!resp.GetDegraded()
}

// Usage returns the top-level command help text.
func Usage() string {
	return `Usage:
  gateway <command> [arguments] [flags]

Commands:
  put     Store a value: put <key> <value> [--mode rs32|replication] [--version N] [--value-file PATH] [--json]
  get     Fetch a value: get <key> [--json]
  delete  Delete a key: delete <key> [--version N] [--json]
  mode    Read or set the cluster default: mode [rs32|replication] [--json]
  status  Show leader, mode, health, degraded state, and repair queue
  health  Print one health line; exits non-zero when the cluster is unhealthy
  stats   Show storage and repair counters from the leader
  repair  Enqueue a repair: repair <key> [--stripe ID] [--force] [--json]
  help    Show this help

Run "gateway <command> --help" for command details.

Environment:
  GATEWAY_NODES    required, id=host:port,... (for example node1=localhost:50051,node2=localhost:50052)
  GATEWAY_VNODES   virtual nodes per host (default 100)
  GATEWAY_TIMEOUT  per-attempt RPC timeout (default 15s)
  GATEWAY_RETRIES  retries after the first attempt (default 3)
  AUTH_TOKEN       optional bearer token
  MAX_MESSAGE_SIZE maximum gRPC message size (default 32 MiB)
`
}

func commandUsage(command string) string {
	switch command {
	case "put":
		return `Usage: gateway put <key> <value> [flags]
       gateway put <key> --value-file PATH [flags]

Flags:
  --mode rs32|replication  Override the cluster default for this write
  --version N              Only write when the stored version is N
  --value-file PATH        Read the value from a file
  --json                   Emit machine-readable JSON
`
	case "get":
		return `Usage: gateway get <key> [--json]

Prints the value on stdout and forwarding metadata on stderr.
`
	case "delete":
		return `Usage: gateway delete <key> [--version N] [--json]

Deletion is idempotent. --version makes it compare-and-delete.
`
	case "mode":
		return `Usage: gateway mode [rs32|replication] [--json]

With no argument, prints the cluster default. With an argument, commits a new
cluster-wide default through Raft.
`
	case "status":
		return `Usage: gateway status [--json]

Shows the leader, global mode, healthy node count, degraded stripes, repair
queue depth, and one row per Raft voter.
`
	case "health":
		return `Usage: gateway health [--json]

Prints one line and exits non-zero unless every voter is healthy, a leader is
known, and no committed stripe is degraded.
`
	case "stats":
		return `Usage: gateway stats [--json]

Shows keys, stripes, fragments, storage bytes, repair counters, and uptime from
the leader (or the first reachable node when no leader is known).
`
	case "repair":
		return `Usage: gateway repair <key> [--stripe ID] [--force] [--json]

Asks the leader to enqueue repair for a key and returns immediately. --stripe
requires the key's current stripe; --force re-encodes even present shards.
`
	default:
		return Usage()
	}
}
