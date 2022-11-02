package supervisor

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/oklog/ulid/v2"
	"github.com/open-telemetry/opamp-go/client"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/internal/examples/supervisor/supervisor/commander"
	"github.com/open-telemetry/opamp-go/internal/examples/supervisor/supervisor/config"
	"github.com/open-telemetry/opamp-go/protobufs"
)

// This Supervisor is developed specifically for OpenTelemetry Collector.
const agentType = "io.opentelemetry.collector"

// TODO: fetch agent version from Collector executable or by some other means.
const agentVersion = "1.0.0"

// A Collector config that should be always applied.
// Enables JSON log output for the Agent.
const localOverrideAgentConfig = `
service:
  telemetry:
    logs:
      encoding: json
`

const initialAgentConfig = `
extensions:
  health_check:

receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: 'io.opentelemetry.collector'
          scrape_interval: 10s
          static_configs:
            - targets: ['0.0.0.0:8888']
processors:
  batch:
  resourcedetection:
    detectors: [env, system]
  metricstransform:
    # these transforms are necessary to generate host entities.
    # in the future, they won't be necessary (hopefully)
    transforms:
      - include: ^otelcol_
        match_type: regexp
        action: update
        operations:
          - action: add_label
            new_label: nr.entity_type
            new_value: otelcol
          - action: add_label
            new_label: service.instance.id
            new_value: "$AGENT_UID"
          - action: update_label
            label: service_version
            new_label: service.version
exporters:
  otlp:
    endpoint: staging-otlp.nr-data.net:4317
    headers:
      api-key: "$API_KEY"

service:
  pipelines:
    metrics/own:
      receivers: [prometheus]
      processors: [batch, resourcedetection, metricstransform]
      exporters: [otlp]
  extensions: [health_check]
  telemetry:
    metrics:
      level: detailed
      address: 0.0.0.0:8888
    logs:
      level: debug
      encoding: json
      output_paths: collector_log.json
      initial_fields:
        service.name: io.opentelemetry.collector
        nr.entity_type: otelcol
        service.instance.id: "$AGENT_UID"
`

// Supervisor implements supervising of OpenTelemetry Collector and uses OpAMPClient
// to work with an OpAMP Server.
type Supervisor struct {
	logger types.Logger

	// Commander that starts/stops the Agent process.
	commander *commander.Commander

	startedAt time.Time

	// Supervisor's own config.
	config config.Supervisor

	// The version of the Agent being Supervised.
	agentVersion string

	// Agent's instance id.
	instanceId ulid.ULID

	// where to store data files, e.g. agent logs or agent ulid
	dataDir string

	// where to store agent binaries
	agentDir string

	// A config section to be added to the Collector's config to fetch its own metrics.
	// TODO: store this persistently so that when starting we can compose the effective
	// config correctly.
	agentConfigOwnMetricsSection atomic.Value

	// Final effective config of the Collector.
	effectiveConfig atomic.Value

	// Location of the effective config file.
	effectiveConfigFilePath string

	// config that the collector had independently of the supervisor
	existingConfig atomic.Value

	// Last received remote config.
	remoteConfig *protobufs.AgentRemoteConfig

	// A channel to indicate there is a new config to apply.
	hasNewConfig chan struct{}

	// The OpAMP client to connect to the OpAMP Server.
	opampClient client.OpAMPClient
}

func NewSupervisor(logger types.Logger) (*Supervisor, error) {
	s := &Supervisor{
		logger:                  logger,
		agentVersion:            agentVersion,
		dataDir:                 "data",
		agentDir:                "agent",
		hasNewConfig:            make(chan struct{}, 1),
		effectiveConfigFilePath: filepath.Join("data", "effective.yaml"),
	}

	if err := s.loadConfig(); err != nil {
		return nil, fmt.Errorf("Error loading config: %v", err)
	}

	s.createInstanceId(s.dataDir)
	logger.Debugf("Supervisor starting, id=%v, type=%s, version=%s.",
		s.instanceId.String(), agentType, agentVersion)

	s.loadAgentEffectiveConfig()
	fmt.Println("Loaded effective config")

	installError := s.installAgent(s.agentDir)
	if installError != nil {
		panic(installError)
	}
	fmt.Println("Installed otel collector")

	if err := s.startOpAMP(); err != nil {
		return nil, fmt.Errorf("Cannot start OpAMP client: %v", err)
	}

	var err error
	apiKey := s.config.Server.ApiKey
	if s.config.Agent != nil && s.config.Agent.ApiKey != "" {
		apiKey = s.config.Agent.ApiKey
	}
	executable := filepath.Join(s.agentDir, "otelcol-contrib")
	if s.config.Agent != nil && s.config.Agent.Executable != "" {
		executable = s.config.Agent.Executable
	}

	agent := &config.Agent{
		ApiKey:     apiKey,
		Executable: executable,
	}
	s.commander, err = commander.NewCommander(
		s.logger,
		s.dataDir,
		s.instanceId.String(),
		agent,
		"--config", filepath.Join(s.effectiveConfigFilePath),
	)
	if err != nil {
		return nil, err
	}

	go s.runAgentProcess()

	return s, nil
}

func (s *Supervisor) loadConfig() error {
	const configFile = "supervisor.yaml"

	k := koanf.New("::")
	if err := k.Load(file.Provider(configFile), yaml.Parser()); err != nil {
		return err
	}

	if err := k.Unmarshal("", &s.config); err != nil {
		return fmt.Errorf("cannot parse %v: %w", configFile, err)
	}

	return nil
}

func (s *Supervisor) startOpAMP() error {

	//s.opampClient = client.NewWebSocket(s.logger)
	s.opampClient = client.NewHTTP(s.logger)

	settings := types.StartSettings{
		OpAMPServerURL: s.config.Server.Endpoint,
		InstanceUid:    s.instanceId.String(),
		Header:         http.Header{"api-key": {s.config.Server.ApiKey}},
		Callbacks: types.CallbacksStruct{
			OnConnectFunc: func() {
				s.logger.Debugf("Connected to the server.")
			},
			OnConnectFailedFunc: func(err error) {
				s.logger.Errorf("Failed to connect to the server: %v", err)
			},
			OnErrorFunc: func(err *protobufs.ServerErrorResponse) {
				s.logger.Errorf("Server returned an error response: %v", err.ErrorMessage)
			},
			GetEffectiveConfigFunc: func(ctx context.Context) (*protobufs.EffectiveConfig, error) {
				return s.createEffectiveConfigMsg(), nil
			},
			OnMessageFunc: s.onMessage,
		},
		Capabilities: protobufs.AgentCapabilities_AcceptsRemoteConfig |
			protobufs.AgentCapabilities_ReportsRemoteConfig |
			protobufs.AgentCapabilities_ReportsEffectiveConfig |
			protobufs.AgentCapabilities_ReportsOwnMetrics |
			protobufs.AgentCapabilities_ReportsHealth,
	}
	fmt.Println("Instance Id")
	fmt.Println(s.instanceId.String())
	err := s.opampClient.SetAgentDescription(s.createAgentDescription())
	if err != nil {
		return err
	}

	err = s.opampClient.SetHealth(&protobufs.AgentHealth{Up: false})
	if err != nil {
		return err
	}

	s.logger.Debugf("Starting OpAMP client...")

	err = s.opampClient.Start(context.Background(), settings)
	if err != nil {
		return err
	}

	s.logger.Debugf("OpAMP Client started.")

	return nil
}

func (s *Supervisor) createInstanceId(dir string) {
	var ulidFileName = filepath.Join(dir, "agent.ulid")
	if _, err := os.Stat(ulidFileName); err == nil {
		f, err := os.ReadFile(ulidFileName)
		if err != nil {
			panic(err)
		}
		rawUlid := strings.TrimSuffix(string(f), "\n")
		s.logger.Debugf("Read ulid %s %s", rawUlid, len(rawUlid))
		s.instanceId = ulid.MustParse(rawUlid)
		s.logger.Debugf("Found ulid %s", s.instanceId)
	} else if errors.Is(err, os.ErrNotExist) {
		// Generate instance id.
		entropy := ulid.Monotonic(rand.New(rand.NewSource(0)), 0)
		s.instanceId = ulid.MustNew(ulid.Timestamp(time.Now()), entropy)
		s.logger.Debugf("Created ulid %s", s.instanceId)
		err := os.WriteFile(ulidFileName, []byte(s.instanceId.String()), 0644)
		if err != nil {
			panic(err)
		}
	}
}

func keyVal(key, val string) *protobufs.KeyValue {
	return &protobufs.KeyValue{
		Key: key,
		Value: &protobufs.AnyValue{
			Value: &protobufs.AnyValue_StringValue{StringValue: val},
		},
	}
}

func (s *Supervisor) createAgentDescription() *protobufs.AgentDescription {
	hostname, _ := os.Hostname()

	// Create Agent description.
	return &protobufs.AgentDescription{
		IdentifyingAttributes: []*protobufs.KeyValue{
			keyVal("service.name", agentType),
			keyVal("service.version", s.agentVersion),
		},
		NonIdentifyingAttributes: []*protobufs.KeyValue{
			keyVal("os.family", runtime.GOOS),
			keyVal("host.name", hostname),
		},
	}
}

func (s *Supervisor) loadAgentEffectiveConfig() error {
	var effectiveConfigBytes []byte
	effectiveConfigBytes = []byte(initialAgentConfig)
	s.effectiveConfig.Store(string(effectiveConfigBytes))
	s.writeEffectiveConfigToFile(string(effectiveConfigBytes), s.effectiveConfigFilePath)

	return nil
}

func (s *Supervisor) installAgent(dir string) error {
	//if len(s.config.Agent.Executable) == 0 {
	//	return nil
	//}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "otelcol-contrib")); os.IsNotExist(err) {
		s.logger.Debugf("Downloading the otel collector")
		resp, err := http.Get(getAgentUrl())
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		err = extractCollector(dir, resp.Body)
		if err != nil {
			return err
		}
	}
	return nil
}

func getAgentUrl() string {
	//return "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.62.1/otelcol_0.62.1_darwin_arm64.tar.gz"
	return "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.63.1/otelcol-contrib_0.63.1_linux_amd64.tar.gz"
}

func extractCollector(dst string, zipped io.Reader) error {
	unzipped, err := gzip.NewReader(zipped)
	defer unzipped.Close()
	if err != nil {
		panic(err)
	}
	tr := tar.NewReader(unzipped)
	for {
		header, err := tr.Next()
		switch {

		// if no more files are found return
		case err == io.EOF:
			return nil

		// return any other error
		case err != nil:
			return err

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}
		if "otelcol-contrib" == header.Name {
			target := filepath.Join(dst, header.Name)
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				return err
			}
			f.Close()
		}
	}
	return nil
}

// createEffectiveConfigMsg create an EffectiveConfig with the content of the
// current effective config.
func (s *Supervisor) createEffectiveConfigMsg() *protobufs.EffectiveConfig {
	cfgStr, ok := s.effectiveConfig.Load().(string)
	if !ok {
		cfgStr = ""
	}

	// This is where the opamp message is created
	cfg := &protobufs.EffectiveConfig{
		ConfigMap: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"": {Body: []byte(cfgStr)},
			},
		},
	}

	return cfg
}

func (s *Supervisor) setupOwnMetrics(ctx context.Context, settings *protobufs.TelemetryConnectionSettings) (configChanged bool) {
	var cfg string
	if settings.DestinationEndpoint == "" {
		// No destination. Disable metric collection.
		s.logger.Debugf("Disabling own metrics pipeline in the config")
		cfg = ""
	} else {
		s.logger.Debugf("Enabling own metrics pipeline in the config")

		// TODO: choose the scraping port dynamically instead of hard-coding to 8888.
		cfg = fmt.Sprintf(
			`
receivers:
  # Collect own metrics
  prometheus/own_metrics:
    config:
      scrape_configs:
        - job_name: 'otel-collector'
          scrape_interval: 10s
          static_configs:
            - targets: ['0.0.0.0:8888']  
exporters:
  otlphttp/own_metrics:
    endpoint: %s

service:
  pipelines:
    metrics/own_metrics:
      receivers: [prometheus/own_metrics]
      exporters: [otlphttp/own_metrics]
`, settings.DestinationEndpoint,
		)
	}

	s.agentConfigOwnMetricsSection.Store(cfg)

	// Need to recalculate the Agent config so that the metric config is included in it.
	configChanged, err := s.recalcEffectiveConfig()
	if err != nil {
		return
	}

	return configChanged
}

// composeEffectiveConfig composes the effective config from multiple sources:
// 1) the remote config from OpAMP Server, 2) the own metrics config section,
// 3) the local override config that is hard-coded in the Supervisor.
func (s *Supervisor) composeEffectiveConfig(config *protobufs.AgentRemoteConfig) (configChanged bool, err error) {
	var k = koanf.New(".")

	// Begin with empty config. We will merge received configs on top of it.
	if err := k.Load(rawbytes.Provider([]byte{}), yaml.Parser()); err != nil {
		return false, err
	}

	// Sort to make sure the order of merging is stable.
	var names []string
	for name := range config.Config.ConfigMap {
		if name == "" {
			continue
		}
		names = append(names, name)
	}

	sort.Strings(names)

	// Append instance config as the last item.
	names = append(names, "")

	existingCfg, ok := s.existingConfig.Load().(string)
	if ok {
		if err := k.Load(rawbytes.Provider([]byte(existingCfg)), yaml.Parser()); err != nil {
			return false, err
		}
	}

	// Merge received configs.
	for _, name := range names {
		fmt.Println(name)
		item := config.Config.ConfigMap[name]
		if item == nil {
			continue
		}
		var k2 = koanf.New(".")
		err := k2.Load(rawbytes.Provider(item.Body), yaml.Parser())
		if err != nil {
			return false, fmt.Errorf("cannot parse config named %s: %v", name, err)
		}
		err = k.Merge(k2)
		if err != nil {
			return false, fmt.Errorf("cannot merge config named %s: %v", name, err)
		}
	}

	// Merge own metrics config.
	ownMetricsCfg, ok := s.agentConfigOwnMetricsSection.Load().(string)
	if ok {
		if err := k.Load(rawbytes.Provider([]byte(ownMetricsCfg)), yaml.Parser()); err != nil {
			return false, err
		}
	}

	// Merge local config last since it has the highest precedence.
	if err := k.Load(rawbytes.Provider([]byte(initialAgentConfig)), yaml.Parser()); err != nil {
		return false, err
	}

	// The merged final result is our effective config.
	effectiveConfigBytes, err := k.Marshal(yaml.Parser())
	if err != nil {
		return false, err
	}

	// Check if effective config is changed.
	newEffectiveConfig := string(effectiveConfigBytes)
	configChanged = false
	if s.effectiveConfig.Load().(string) != newEffectiveConfig {
		s.logger.Debugf("Effective config changed.")
		s.effectiveConfig.Store(newEffectiveConfig)
		configChanged = true
	}

	return configChanged, nil
}

// Recalculate the Agent's effective config and if the config changes signal to the
// background goroutine that the config needs to be applied to the Agent.
func (s *Supervisor) recalcEffectiveConfig() (configChanged bool, err error) {

	configChanged, err = s.composeEffectiveConfig(s.remoteConfig)
	if err != nil {
		s.logger.Errorf("Error composing effective config. Ignoring received config: %v", err)
		return configChanged, err
	}

	return configChanged, nil
}

func (s *Supervisor) startAgent() {
	err := s.commander.Start(context.Background())
	if err != nil {
		errMsg := fmt.Sprintf("Cannot start the agent: %v", err)
		s.logger.Errorf(errMsg)
		s.opampClient.SetHealth(&protobufs.AgentHealth{Up: false, LastError: errMsg})
		return
	}
	s.startedAt = time.Now()
	s.opampClient.SetHealth(
		&protobufs.AgentHealth{
			Up:                true,
			StartTimeUnixNano: uint64(s.startedAt.UnixNano()),
		},
	)
}

func (s *Supervisor) runAgentProcess() {
	if _, err := os.Stat(s.effectiveConfigFilePath); err == nil {
		// We have an effective config file saved previously. Use it to start the agent.
		s.startAgent()
	}

	restartTimer := time.NewTimer(0)
	restartTimer.Stop()

	for {
		select {
		case <-s.hasNewConfig:
			restartTimer.Stop()
			s.applyConfigWithAgentRestart()

		case <-s.commander.Done():
			errMsg := fmt.Sprintf(
				"Agent process PID=%d exited unexpectedly, exit code=%d. Will restart in a bit...",
				s.commander.Pid(), s.commander.ExitCode(),
			)
			s.logger.Debugf(errMsg)
			s.opampClient.SetHealth(&protobufs.AgentHealth{Up: false, LastError: errMsg})

			// TODO: decide why the agent stopped. If it was due to bad config, report it to server.

			// Wait 5 seconds before starting again.
			restartTimer.Stop()
			restartTimer.Reset(5 * time.Second)

		case <-restartTimer.C:
			s.startAgent()
		}
	}
}

func (s *Supervisor) applyConfigWithAgentRestart() {
	s.logger.Debugf("Restarting the agent with the new config.")
	cfg := s.effectiveConfig.Load().(string)
	s.commander.Stop(context.Background())
	s.writeEffectiveConfigToFile(cfg, s.effectiveConfigFilePath)
	s.startAgent()
}

func (s *Supervisor) writeEffectiveConfigToFile(cfg string, filePath string) {
	f, err := os.Create(filePath)
	if err != nil {
		s.logger.Errorf("Cannot write effective config file: %v", err)
	}
	defer f.Close()

	f.WriteString(cfg)
}

func (s *Supervisor) Shutdown() {
	s.logger.Debugf("Supervisor shutting down...")
	if s.commander != nil {
		s.commander.Stop(context.Background())
	}
	if s.opampClient != nil {
		s.opampClient.SetHealth(
			&protobufs.AgentHealth{
				Up: false, LastError: "Supervisor is shutdown",
			},
		)
		_ = s.opampClient.Stop(context.Background())
	}
}

func (s *Supervisor) onMessage(ctx context.Context, msg *types.MessageData) {
	configChanged := false
	if msg.RemoteConfig != nil {
		s.remoteConfig = msg.RemoteConfig
		s.logger.Debugf("Received remote config from server, hash=%x.", s.remoteConfig.ConfigHash)

		var err error
		configChanged, err = s.recalcEffectiveConfig()
		if err != nil {
			s.opampClient.SetRemoteConfigStatus(&protobufs.RemoteConfigStatus{
				LastRemoteConfigHash: msg.RemoteConfig.ConfigHash,
				Status:               protobufs.RemoteConfigStatus_FAILED,
				ErrorMessage:         err.Error(),
			})
		} else {
			s.opampClient.SetRemoteConfigStatus(&protobufs.RemoteConfigStatus{
				LastRemoteConfigHash: msg.RemoteConfig.ConfigHash,
				Status:               protobufs.RemoteConfigStatus_APPLIED,
			})
		}
	}

	if msg.OwnMetricsConnSettings != nil {
		configChanged = s.setupOwnMetrics(ctx, msg.OwnMetricsConnSettings) || configChanged
	}

	if msg.AgentIdentification != nil {
		newInstanceId, err := ulid.Parse(msg.AgentIdentification.NewInstanceUid)
		if err != nil {
			s.logger.Errorf(err.Error())
		}

		s.logger.Debugf("Agent identify is being changed from id=%v to id=%v",
			s.instanceId.String(),
			newInstanceId.String())
		s.instanceId = newInstanceId

		// TODO: update metrics pipeline by altering configuration and setting
		// the instance id when Collector implements https://github.com/open-telemetry/opentelemetry-collector/pull/5402.
	}

	if configChanged {
		err := s.opampClient.UpdateEffectiveConfig(ctx)
		if err != nil {
			s.logger.Errorf(err.Error())
		}

		s.logger.Debugf("Config is changed. Signal to restart the agent.")
		// Signal that there is a new config.
		select {
		case s.hasNewConfig <- struct{}{}:
		default:
		}
	}
}
