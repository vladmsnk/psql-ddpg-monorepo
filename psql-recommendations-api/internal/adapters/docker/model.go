package docker

type Postgres struct {
	Host          string `yaml:"host"`
	Port          int    `yaml:"port"`
	User          string `yaml:"user"`
	Password      string `yaml:"password"`
	Database      string `yaml:"database"`
	SSLMode       string `yaml:"sslmode"`
	ContainerName string `yaml:"container_name"`
}
