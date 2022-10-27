package config

// Supervisor is the Supervisor config file format.
type Supervisor struct {
	Server *OpAMPServer
	Agent  *Agent
}

type OpAMPServer struct {
	Endpoint string
	ApiKey   string
}

type Agent struct {
	Executable        string
	InitialConfigPath string
	ApiKey            string
}
