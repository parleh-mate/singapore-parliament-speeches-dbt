deploy:
	@gcloud functions deploy run-dbt \
  --gen2 \
  --region=us-central1 \
  --runtime=python310 \
  --source=. \
  --entry-point=run_dbt \
  --trigger-http \
  --allow-unauthenticated \
  --memory=1Gi \
  --timeout=600s \
  --concurrency=1 \
  --service-account=dbt-user@singapore-parliament-speeches.iam.gserviceaccount.com
