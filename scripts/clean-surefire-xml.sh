#!/bin/bash

# Script to clean Maven Surefire XML reports by removing comma separators from time values
# This fixes parsing issues with the maven-surefire-report-plugin

set -e

# Function to clean XML files in a directory
clean_xml_files() {
    local dir="$1"
    
    if [ ! -d "$dir" ]; then
        echo "Directory $dir does not exist, skipping..."
        return 0
    fi
    
    echo "Cleaning XML files in: $dir"
    
    # Find all TEST-*.xml files and clean them
    find "$dir" -name "TEST-*.xml" -type f | while read -r file; do
        echo "Processing: $file"
        
        # Create backup
        cp "$file" "$file.backup"
        
        # Remove comma separators from time attributes
        # Pattern matches: time="1,234.567" -> time="1234.567"
        sed -i.tmp 's/time="\([0-9]*\),\([0-9]*\.[0-9]*\)"/time="\1\2"/g' "$file"
        
        # Remove temporary file created by sed
        rm -f "$file.tmp"
        
        echo "Cleaned: $file"
    done
}

# Main execution
echo "Starting Surefire XML cleanup..."

# Clean root target directory
clean_xml_files "target/surefire-reports"

# Clean all module target directories
find . -type d -name "target" -path "*/target" | while read -r target_dir; do
    surefire_dir="$target_dir/surefire-reports"
    clean_xml_files "$surefire_dir"
done

echo "Surefire XML cleanup completed!"