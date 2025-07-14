---
name: Load Test Failure
about: Report a failure in the scheduled load tests.
title: 'Load Test Failure: {{ env.DATE }}'
labels: 'bug, load-test-failure'
assignees: ''

---

**Load Test Failed**

**Date:** {{ env.DATE }}
**Job URL:** [Link to GitHub Actions Run]({{ env.JOB_URL }})

**Error Details:**

Please provide any relevant error messages, logs, or context below.
