"""EA data management pipeline."""

from ea_pipeline.register import register_uploaded_file
from ea_pipeline.validate import validate_registered_upload
from ea_pipeline.bronze import load_upload_to_bronze