#!/bin/bash

# Test script to verify XML cleaning functionality

set -e

echo "Testing XML cleaning functionality..."

# Create test directory
test_dir="/tmp/surefire-test"
mkdir -p "$test_dir"

# Create sample XML file with comma-separated time values
cat > "$test_dir/TEST-sample.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<testsuite xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd" version="3.0" name="com.example.SampleTest" time="1,024.612" tests="3" errors="0" skipped="0" failures="0">
  <properties>
    <property name="java.specification.version" value="11"/>
  </properties>
  <testcase classname="com.example.SampleTest" name="testMethod1" time="512.345"/>
  <testcase classname="com.example.SampleTest" name="testMethod2" time="2,048.123"/>
  <testcase classname="com.example.SampleTest" name="testMethod3" time="1,536.789"/>
</testsuite>
EOF

echo "Original XML content:"
cat "$test_dir/TEST-sample.xml"
echo ""

# Run the cleaning script on test directory
echo "Running XML cleaning..."
sed -i.tmp 's/time="\([0-9]*\),\([0-9]*\.[0-9]*\)"/time="\1\2"/g' "$test_dir/TEST-sample.xml"
rm -f "$test_dir/TEST-sample.xml.tmp"

echo "Cleaned XML content:"
cat "$test_dir/TEST-sample.xml"
echo ""

# Verify the cleaning worked
if grep -q 'time="[0-9]*,[0-9]*\.[0-9]*"' "$test_dir/TEST-sample.xml"; then
    echo "❌ FAILED: Comma separators still present in XML"
    exit 1
else
    echo "✅ SUCCESS: All comma separators removed from time values"
fi

# Cleanup
rm -rf "$test_dir"

echo "XML cleaning test completed successfully!"