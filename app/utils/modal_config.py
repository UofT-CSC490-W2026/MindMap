import modal


APP_NAME_ML = "mindmap-ml-workers"
APP_NAME_INGESTION = "ingestion-worker"
APP_NAME_TRANSFORMATION = "transformation-worker"

SECRET_NAME_SNOWFLAKE = "snowflake-creds"

app_ml = modal.App(APP_NAME_ML)
app_ingestion = modal.App(APP_NAME_INGESTION)
app_transformation = modal.App(APP_NAME_TRANSFORMATION)

secret_snowflake = modal.Secret.from_name(SECRET_NAME_SNOWFLAKE)


def build_image(*packages: str, python_version: str = "3.10") -> modal.Image:
	image = modal.Image.debian_slim(python_version=python_version)
	if packages:
		image = image.pip_install(*packages)
	return image


image_ingestion = build_image("snowflake-connector-python", "arxiv")
image_transformation = build_image("snowflake-connector-python")

image_embedding = build_image(
	"sentence-transformers==2.7.0",
	"torch",
	"snowflake-connector-python[pandas]==3.12.0",
	"pandas",
)

image_semantic_search = build_image(
	"snowflake-connector-python[pandas]==3.12.0",
	"pandas",
)

image_citation = build_image(
	"requests",
	"feedparser",
	"pymupdf",
)

image_citation_aware = build_image(
	"sentence-transformers==2.7.0",
	"torch",
	"snowflake-connector-python[pandas]==3.12.0",
	"pandas",
	"requests",
	"feedparser",
	"pymupdf",
	"numpy",
)
