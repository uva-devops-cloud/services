#!/bin/bash

# Script to build a deployment package for the Python Lambda function

set -e

# Create a temporary directory for the build
TEMP_DIR=$(mktemp -d)
echo "Created temporary directory: $TEMP_DIR"

# Install dependencies to the temp directory
pip install -r requirements.txt -t "$TEMP_DIR"

# Copy lambda code to the temp directory
cp lambda_function.py "$TEMP_DIR"

# Create a zip file in the parent directory
cd "$TEMP_DIR"
zip -r9 "../deployment_package.zip" .

echo "Created deployment package: ../deployment_package.zip"

# Cleanup
cd -
rm -rf "$TEMP_DIR"
echo "Cleaned up temporary directory"
