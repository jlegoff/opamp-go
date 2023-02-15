#!/bin/sh
if [ -z $API_KEY ]
then
	echo "Please provide the API_KEY environment variable"
	exit 1
fi
echo "server:\n  endpoint: https://opamp.staging-service.newrelic.com:443/v1/opamp\n  apiKey: $API_KEY" > supervisor.yaml
./supervisor
