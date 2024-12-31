.PHONY: gcp-latest

gcp-latest:
	docker build --platform "linux/amd64" -t us-east1-docker.pkg.dev/catalyst-cooperative-pudl/pudl-viewer/pudl-viewer:latest .
	docker push us-east1-docker.pkg.dev/catalyst-cooperative-pudl/pudl-viewer/pudl-viewer:latest

  