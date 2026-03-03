#!/bin/bash
# Deploy the genetics results API to Cloud Run

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-europe-west1}"
SERVICE_NAME="${SERVICE_NAME:-genetics-results-api}"
DATASET_ID="${DATASET_ID:-genetics_results}"
echo "Deploying ${SERVICE_NAME} to ${PROJECT_ID} in ${REGION}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

# build and push container
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
echo "Building container image..."
gcloud builds submit --tag "${IMAGE}" .

# deploy to cloud run
echo "Deploying to Cloud Run..."

ENV_VARS="PROJECT_ID=${PROJECT_ID},DATASET_ID=${DATASET_ID}"

gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars "${ENV_VARS}" \
  --memory 512Mi \
  --cpu 1 \
  --timeout 300 \
  --max-instances 10

# get service URL
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --region "${REGION}" \
  --format 'value(status.url)')

echo ""
echo "Deployment complete!"
echo "Service URL: ${SERVICE_URL}"
echo ""
echo "Test endpoints:"
echo "  Health: curl ${SERVICE_URL}/health"
echo "  Schema: curl ${SERVICE_URL}/schema"
echo "  Query:  curl -X POST ${SERVICE_URL}/query -H 'Content-Type: application/json' -d '{\"sql\": \"SELECT COUNT(*) FROM credible_sets\"}'"
