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
                    curl -s localhost:9999/attach/1234
`
	assert.Equal(t, expectedConfig, flexConfig)
}
