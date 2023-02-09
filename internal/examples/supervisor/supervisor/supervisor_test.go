package supervisor

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestApmConfig(t *testing.T) {
	apmConfig := "pid: 123"
	flexConfig := string(TransformApmConfigToFlex("APM attach", []byte(apmConfig)))
	expectedConfig := `integrations:
  - name: nri-flex
    config:
        name: APM attach
        apis:
            - name: JavaAttacher
              event_type: JavaAttacherSample
              commands:
                - run: >
                    curl -s localhost:9999/attach/123
`
	assert.Equal(t, expectedConfig, flexConfig)
}

func TestApmConfigWithAppName(t *testing.T) {
	apmConfig := "pid: 123\nappName: PetClinic"
	flexConfig := string(TransformApmConfigToFlex("APM attach", []byte(apmConfig)))
	expectedConfig := `integrations:
  - name: nri-flex
    config:
        name: APM attach
        apis:
            - name: JavaAttacher
              event_type: JavaAttacherSample
              commands:
                - run: >
                    curl -s localhost:9999/attach/123?appName=PetClinic
`
	assert.Equal(t, expectedConfig, flexConfig)
}
