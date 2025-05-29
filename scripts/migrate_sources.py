"""Script to migrate source configurations to the new versioned structure."""
import shutil
from pathlib import Path

# Define source and target directories
BASE_DIR = Path(__file__).parent.parent
SOURCE_DIR = BASE_DIR / "source"
TARGET_DIR = SOURCE_DIR

# Define the version to migrate to
VERSION = "v1"

def migrate_source(source_type: str) -> None:
    """Migrate a single source type to the new structure."""
    source_path = SOURCE_DIR / source_type
    if not source_path.exists():
        print(f"No {source_type} directory found, skipping...")
        return

    # Create versioned directory
    versioned_dir = TARGET_DIR / source_type / VERSION
    versioned_dir.mkdir(parents=True, exist_ok=True)
    
    # Move files to versioned directory
    for file_path in source_path.glob("*"):
        if file_path.is_file() and file_path.suffix in [".yaml", ".yml"]:
            target_path = versioned_dir / file_path.name
            if not target_path.exists():
                shutil.move(str(file_path), str(target_path))
                print(f"Moved {file_path} to {target_path}")
            else:
                print(f"Skipping {file_path} - target already exists")

    # Remove original directory if empty
    try:
        source_path.rmdir()
        print(f"Removed empty directory: {source_path}")
    except OSError:
        print(f"Directory not empty, keeping: {source_path}")

def main():
    """Main migration function."""
    print("Starting source configuration migration...")
    
    # Ensure source directory exists
    if not SOURCE_DIR.exists():
        print(f"Source directory not found: {SOURCE_DIR}")
        return
    
    # Migrate each source type
    for source_type in ["email", "sms", "transaction", "user"]:
        print(f"\nProcessing {source_type}...")
        migrate_source(source_type)
    
    print("\nMigration complete!")

if __name__ == "__main__":
    main()
