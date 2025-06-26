# PyPI Publishing Setup Guide

This guide walks through setting up PyPI trusted publishing for the async-python-cassandra project.

## Current Setup

- **TestPyPI**: ✅ Already configured and working
- **PyPI**: Ready to configure

## Setting Up PyPI Trusted Publishing

### 1. Create PyPI Account (if needed)
1. Go to https://pypi.org/account/register/
2. Create account and verify email
3. Enable 2FA (required for maintainers)

### 2. Configure Trusted Publishing
1. Log in to PyPI: https://pypi.org/account/login/
2. Go to your account settings
3. Navigate to "Publishing" → "Add a new pending publisher"
4. Fill in:
   - **PyPI Project Name**: `async-cassandra`
   - **GitHub Repository Owner**: `axonops`
   - **GitHub Repository Name**: `async-python-cassandra-client`
   - **Workflow name**: `release.yml`
   - **Environment name**: (leave blank)
5. Click "Add"

### 3. Publishing Workflow

The GitHub Actions workflow is already configured:

#### For pre-releases (current approach):
- Tag: `v0.0.1rc3` → Publishes to **TestPyPI**
- Users install with: `pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple async-cassandra`

#### For stable releases:
- Tag: `v0.0.1` → Publishes to **PyPI**
- Users install with: `pip install async-cassandra`

### 4. Release Process

```bash
# Pre-release testing (TestPyPI)
git tag v0.0.1rc3
git push origin v0.0.1rc3

# Stable release (PyPI) - when ready
git tag v0.0.1
git push origin v0.0.1
```

## Alternative: Publishing Pre-releases to PyPI

If you want to publish RCs to real PyPI (for easier testing), modify `.github/workflows/release.yml`:

Change the `publish-testpypi` job condition from:
```yaml
if: contains(github.ref, 'rc') || contains(github.ref, 'a') || contains(github.ref, 'b')
```

To publish to PyPI instead of TestPyPI. Users can then install with:
```bash
pip install --pre async-cassandra
```

## Important Notes

1. **First PyPI Release**: The first push to PyPI creates the project
2. **Name Reservation**: Once published, the name is permanently reserved
3. **Version History**: All versions are permanent (can't delete/reuse)
4. **Pre-release Safety**: Pre-releases won't be installed without `--pre` flag

## Troubleshooting

### "Project already exists" error
- Someone else owns the name on PyPI
- You need to choose a different name

### "Trusted publisher not found" error
- Double-check the workflow filename matches exactly
- Ensure GitHub repo names match exactly (case-sensitive)

### "Invalid version" error
- Ensure tags follow PEP 440 format (v0.0.1rc3, not v0.0.1-rc3)
