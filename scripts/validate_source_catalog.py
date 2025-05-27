import logging
import sys
from pathlib import Path

from feature_platform.core.source_registry import SourceRegistry

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

def main():
    """
    Validates the source catalog by attempting to load all source definitions
    from the 'source/' directory.
    """
    # Assuming the script is run from the repository root
    # or that 'source/' is accessible from the current working directory.
    # For robustness, one might want to determine the repo root dynamically.
    repo_root = Path(__file__).resolve().parent.parent 
    source_dir_path = repo_root / "source" # Or simply Path("source/") if script is run from repo root

    logger.info(f"Starting validation of source catalog in directory: {source_dir_path.resolve()}")

    try:
        registry = SourceRegistry.from_yaml_dir(source_dir_path)
        num_loaded = len(registry.get_all_source_definitions())
        logger.info(f"Successfully validated and loaded {num_loaded} source definitions.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to validate source catalog: {e}", exc_info=True)
        # Additional specific error handling can be added if needed,
        # e.g. for ValidationError from Pydantic or YAMLError from PyYAML
        # However, from_yaml_dir already logs these specific errors.
        sys.exit(1)

if __name__ == "__main__":
    main()
