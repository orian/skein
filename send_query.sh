#!/bin/bash

# Configuration
PROXY_URL="http://localhost:8080"
QUERY="SELECT 1 as one, 2 as two;"
USER_ID="test-user-bash"

# Construct the JSON payload
JSON_PAYLOAD=$(cat <<EOF
{
  "user_id": "${USER_ID}",
  "query": "${QUERY}",
  "priority": 10
}
EOF
)

echo "Sending query to ${PROXY_URL}/query"
echo "Payload: ${JSON_PAYLOAD}"

# Send the query using curl
curl -X POST "${PROXY_URL}/query" \
     -H "Content-Type: application/json" \
     -d "${JSON_PAYLOAD}" \
     -w "\nHTTP Status: %{http_code}\n" \
     -o -

echo ""
echo "Done."

