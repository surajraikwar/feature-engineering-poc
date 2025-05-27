from typing import List, Dict, Any, Optional
import yaml
from pydantic import BaseModel, validator, Field, Extra
import logging # Added import as per self-correction

# --- Pydantic Models for Configuration Validation ---

class BaseConfigModel(BaseModel):
    class Config:
        extra = Extra.forbid # Disallow extra fields not defined in the model

class InputSourceParams(BaseConfigModel):
    location: str
    format: str = "delta"
    options: Optional[Dict[str, Any]] = None
    # Allow specific connection override, though often global Databricks env vars are used
    connection_config: Optional[Dict[str, Any]] = None 

class InputSourceConfig(BaseConfigModel):
    source_type: str = "databricks_spark" # Default, but can be specified
    config: InputSourceParams

class FeatureTransformerParams(BaseConfigModel):
    # Pydantic allows extra fields if not 'forbid' in Config, useful for varying transformer params
    class Config:
        extra = Extra.allow 
        # This allows transformers to have arbitrary params not explicitly defined here,
        # which is useful since each transformer has different __init__ args.
        # The actual validation of these params will happen when instantiating the transformer.

class FeatureTransformerConfig(BaseConfigModel):
    name: str # Name/key to identify the FeatureTransformer class
    params: FeatureTransformerParams # Parameters for the transformer's __init__
                                     # As per subtask: proceed with this as required.

class OutputSinkParams(BaseConfigModel):
    # Params for sink_type: "display"
    num_rows: Optional[int] = 20
    truncate: Optional[bool] = False

    # Params for sink_type: "delta_table" or "parquet_files" (examples)
    path: Optional[str] = None
    mode: Optional[str] = "overwrite" # "overwrite" or "append"
    partition_by: Optional[List[str]] = None
    options: Optional[Dict[str, Any]] = None # Spark write options

    # Allow other fields for future sink types
    class Config:
        extra = Extra.allow


class OutputSinkConfig(BaseConfigModel):
    sink_type: str = "display" # Default sink type
    config: OutputSinkParams = Field(default_factory=OutputSinkParams)


class JobConfig(BaseConfigModel):
    job_name: Optional[str] = "Untitled Job"
    description: Optional[str] = ""
    input_source: InputSourceConfig
    feature_transformers: List[FeatureTransformerConfig] = Field(default_factory=list)
    output_sink: OutputSinkConfig = Field(default_factory=OutputSinkConfig)

    @validator('feature_transformers', pre=True, always=True)
    def ensure_feature_transformers_is_list(cls, v):
        # Handles case where feature_transformers might be omitted in YAML (becomes None)
        return v if v is not None else []


# --- Loading Function ---

def load_job_config(config_path: str) -> JobConfig:
    """
    Loads a job configuration from a YAML file and validates it using Pydantic models.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        A JobConfig object.

    Raises:
        FileNotFoundError: If the config_path does not exist.
        yaml.YAMLError: If the YAML is malformed.
        pydantic.ValidationError: If the configuration does not match the schema.
    """
    try:
        with open(config_path, 'r') as f:
            raw_config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at path: {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML configuration file at {config_path}: {e}")
        raise

    if raw_config is None:
        logging.error(f"Configuration file at {config_path} is empty or invalid.")
        # You might want to raise a specific error here or return a default JobConfig
        raise ValueError(f"Configuration file {config_path} is empty or invalid.")

    try:
        validated_config = JobConfig(**raw_config)
        logging.info(f"Successfully loaded and validated job configuration from {config_path}")
        return validated_config
    except Exception as e: # Catches Pydantic's ValidationError and others
        logging.error(f"Validation error for job configuration from {config_path}: {e}")
        # Log the detailed Pydantic error if possible (e.errors())
        if hasattr(e, 'errors'): # This is specific to Pydantic's ValidationError
           for error_detail in e.errors(): # Renamed 'error' to 'error_detail' to avoid conflict
               logging.error(f"  Field: {'.'.join(map(str,error_detail['loc']))}, Message: {error_detail['msg']}")
        raise


if __name__ == "__main__":
    # logging import is at the top
    logging.basicConfig(level=logging.INFO)
    
    # Example usage:
    # Assuming 'configs/jobs/sample_financial_features_job.yaml' exists from previous step.
    # Create a dummy one for testing if it doesn't.
    dummy_config_path = "configs/jobs/sample_financial_features_job.yaml" # Path from repo root

    # For standalone testing, ensure the dummy config file exists relative to this script,
    # or use an absolute path. If running this file directly, CWD is .../feature-platform/feature_platform/jobs
    # So, dummy_config_path should be "../../configs/jobs/sample_financial_features_job.yaml"
    
    # Let's adjust path for direct execution of this file for testing
    import os
    # Path adjustment logic from subtask description
    if not os.path.exists(dummy_config_path) and os.path.exists("../../configs/jobs/sample_financial_features_job.yaml"):
         dummy_config_path = "../../configs/jobs/sample_financial_features_job.yaml"
    elif not os.path.exists(dummy_config_path) and os.path.exists("../../../configs/jobs/sample_financial_features_job.yaml"):
        # If running from a subdirectory like /app (where the repo root is) for testing in some environments
         dummy_config_path = "../../../configs/jobs/sample_financial_features_job.yaml"


    if not os.path.exists(dummy_config_path):
        logging.warning(f"Test config file '{dummy_config_path}' not found. Creating a dummy one for demonstration.")
        # Use a path relative to the script's location for creation if it's a relative path
        path_for_creation = dummy_config_path
        if not os.path.isabs(dummy_config_path) and dummy_config_path.startswith("configs/"):
            # If script is in feature_platform/jobs, and path is "configs/...", go up two levels
            path_for_creation = os.path.join(os.path.dirname(__file__), "..", "..", dummy_config_path)
            path_for_creation = os.path.normpath(path_for_creation)

        os.makedirs(os.path.dirname(path_for_creation), exist_ok=True)
        dummy_yaml_content = {
            "job_name": "dummy_test_job",
            "input_source": {
                "source_type": "databricks_spark",
                "config": {
                    "location": "test.catalog.test_table"
                }
            },
            "feature_transformers": [
                {
                    "name": "TestTransformer",
                    # params is required by FeatureTransformerConfig as per current definition
                    "params": {"param1": "value1"} 
                }
            ],
            "output_sink": {
                "sink_type": "display",
                "config": {"num_rows": 5}
            }
        }
        with open(path_for_creation, 'w') as f:
            yaml.dump(dummy_yaml_content, f)
        dummy_config_path = path_for_creation # ensure we use the path we might have created
    
    try:
        job_config = load_job_config(dummy_config_path)
        logging.info(f"Loaded job: {job_config.job_name}")
        logging.info(f"Input source type: {job_config.input_source.source_type}")
        logging.info(f"Input location: {job_config.input_source.config.location}")
        if job_config.feature_transformers:
            logging.info(f"First transformer name: {job_config.feature_transformers[0].name}")
            # params is not Optional, so it should always exist.
            logging.info(f"First transformer params: {job_config.feature_transformers[0].params.dict()}")
        logging.info(f"Output sink type: {job_config.output_sink.sink_type}")
        # config in OutputSinkConfig has a default_factory, so it should always exist.
        logging.info(f"Output sink config: {job_config.output_sink.config.dict()}")

    except Exception as e:
        logging.error(f"An error occurred during example usage with '{dummy_config_path}': {e}", exc_info=True)

    # Test with a deliberately bad config
    # Adjust path for creation if running from feature_platform/jobs
    bad_config_base_name = "bad_job_config.yaml"
    bad_config_dir = os.path.dirname(dummy_config_path) # Use same dir as dummy_config_path
    bad_config_path = os.path.join(bad_config_dir, bad_config_base_name)

    # This content will cause a validation error because 'params' is missing in 'feature_transformers[0]'
    # and 'location' is missing in 'input_source.config'
    bad_yaml_content_missing_required_fields = {
         "job_name": "bad_job_missing_fields",
         "input_source": {
             "source_type": "databricks_spark",
             "config": {
                 # "location": "test.table" # Location is missing
                 "format": "delta"
             }
         },
         "feature_transformers": [
             {
                 "name": "MissingParamsTransformer"
                 # "params" field is missing here, which is required by FeatureTransformerConfig
             }
         ]
    }

    with open(bad_config_path, 'w') as f:
        yaml.dump(bad_yaml_content_missing_required_fields, f)
    
    try:
        logging.info(f"\nAttempting to load deliberately bad config from {bad_config_path}...")
        load_job_config(bad_config_path)
    except Exception as e: # Expected to fail (Pydantic ValidationError)
        logging.error(f"Successfully caught error for bad config: {type(e).__name__} - {e}")
    finally:
        if os.path.exists(bad_config_path):
            os.remove(bad_config_path)
        
        # Clean up dummy config if it was created by this script's __main__
        if os.path.exists(dummy_config_path) and dummy_config_path.endswith("sample_financial_features_job.yaml"):
            try:
                with open(dummy_config_path, 'r') as f_check:
                    content = yaml.safe_load(f_check)
                if content and content.get("job_name") == "dummy_test_job": # Check if it's the one we created
                    os.remove(dummy_config_path)
                    logging.info(f"Removed dummy config file: {dummy_config_path}")
            except Exception as e_clean:
                logging.warning(f"Could not check/remove dummy config {dummy_config_path}: {e_clean}")
