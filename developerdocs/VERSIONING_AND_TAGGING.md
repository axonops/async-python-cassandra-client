# Versioning and Tagging Guide

This document explains the versioning and tagging conventions for the async-python-cassandra project.

## Overview

We use [Semantic Versioning](https://semver.org/) and [PEP 440](https://www.python.org/dev/peps/pep-0440/) compliant version numbers. Git tags are the single source of truth for versions through `setuptools-scm`.

## Version Format

### Production Releases
- **Format**: `MAJOR.MINOR.PATCH`
- **Examples**: `0.1.0`, `1.0.0`, `1.2.3`
- **Git tag**: `v0.1.0`, `v1.0.0`, `v1.2.3`

### Pre-release Versions
- **Alpha**: `0.1.0a1`, `0.1.0a2` (early development, API may change)
- **Beta**: `0.1.0b1`, `0.1.0b2` (feature complete, testing phase)
- **Release Candidate**: `0.1.0rc1`, `0.1.0rc2` (production ready, final testing)
- **Git tags**: `v0.1.0a1`, `v0.1.0b1`, `v0.1.0rc1`

### Development Versions
Between tags, `setuptools-scm` automatically generates development versions:
- **Format**: `0.1.0.dev1+g1234567.d20250626`
- **Parts**:
  - `0.1.0` - Next version
  - `.dev1` - Development version
  - `+g1234567` - Git commit hash
  - `.d20250626` - Date stamp

## Tagging Conventions

### ✅ CORRECT Tag Formats (PEP 440 Compliant)
```bash
# Alpha releases
git tag v0.1.0a1
git tag v0.1.0a2

# Beta releases
git tag v0.1.0b1
git tag v0.1.0b2

# Release candidates
git tag v0.1.0rc1
git tag v0.1.0rc2

# Production releases
git tag v0.1.0
git tag v1.0.0
```

### ❌ INCORRECT Tag Formats (Will Cause Issues)
```bash
# DON'T use hyphens
git tag v0.1.0-rc1    # WRONG - causes version mismatch
git tag v0.1.0-beta1  # WRONG - not PEP 440 compliant
git tag v0.1.0-alpha1 # WRONG - use 'a' not 'alpha'

# DON'T use underscores or other separators
git tag v0.1.0_rc1    # WRONG
git tag v0.1.0.rc1    # WRONG - no dot before pre-release
```

## Release Workflow

### 1. Pre-release Testing (Alpha/Beta/RC)
```bash
# 1. Make sure all changes are committed
git status

# 2. Create and push a pre-release tag
git tag v0.1.0rc1
git push origin v0.1.0rc1

# 3. CI/CD will:
#    - Run full test suite
#    - Build package with version 0.1.0rc1
#    - Publish to TestPyPI (for rc/beta/alpha tags)
#    - Validate the package installs correctly
```

### 2. Production Release
```bash
# 1. After pre-release testing passes
git tag v0.1.0
git push origin v0.1.0

# 2. CI/CD will:
#    - Run full test suite
#    - Build package with version 0.1.0
#    - Publish to PyPI (Phase 7 - when configured)
```

### 3. Patch Releases
```bash
# For bug fixes
git tag v0.1.1
git push origin v0.1.1
```

### 4. Minor Releases
```bash
# For new features (backward compatible)
git tag v0.2.0
git push origin v0.2.0
```

### 5. Major Releases
```bash
# For breaking changes
git tag v1.0.0
git push origin v1.0.0
```

## Version Precedence

PEP 440 defines version precedence as:
1. `0.1.0a1` < `0.1.0a2` < `0.1.0b1` < `0.1.0b2` < `0.1.0rc1` < `0.1.0rc2` < `0.1.0`
2. Pre-releases are not installed by default: `pip install async-cassandra` skips pre-releases
3. To install pre-releases: `pip install --pre async-cassandra`

## CI/CD Integration

Our GitHub Actions workflows trigger based on tag patterns:

### TestPyPI Publishing (Pre-releases)
- **Triggers on**: Tags matching `v*` containing `rc`, `beta`, or `alpha`
- **Examples**: `v0.1.0rc1`, `v0.1.0b1`, `v0.1.0a1`
- **Publishes to**: https://test.pypi.org/project/async-cassandra/

### PyPI Publishing (Production)
- **Triggers on**: Tags matching `v*` WITHOUT pre-release suffixes
- **Examples**: `v0.1.0`, `v1.0.0`
- **Publishes to**: https://pypi.org/project/async-cassandra/

## Common Issues and Solutions

### Issue: Version mismatch between tag and package
**Symptom**: Tag `v0.1.0-rc1` but package shows as `0.1.0`
**Solution**: Use PEP 440 format: `v0.1.0rc1` (no hyphen)

### Issue: Package not installing from TestPyPI
**Symptom**: `ERROR: No matching distribution found for async-cassandra==0.1.0-rc1`
**Solution**: Version format is `0.1.0rc1` not `0.1.0-rc1`

### Issue: Old version showing after new tag
**Symptom**: Package still shows old version after tagging
**Solution**:
1. Ensure tag is pushed: `git push origin v0.1.0rc1`
2. Check you're on the tagged commit: `git describe --tags`
3. Clean build: `rm -rf build/ dist/ *.egg-info`

## Best Practices

1. **Always test with release candidates** before production releases
2. **Use ascending version numbers** - never reuse or delete tags
3. **Document breaking changes** in CHANGELOG for major versions
4. **Keep tags lightweight** - use `git tag` not `git tag -a`
5. **Push tags explicitly** - `git push` doesn't push tags by default

## Examples from Our Release History

```bash
# Our progression for 0.1.0 release:
v0.1.0rc1  # First release candidate (TestPyPI)
v0.1.0rc2  # Fixed CI issues
v0.1.0rc3  # Fixed more CI issues
v0.1.0rc4  # Removed benchmark tests
v0.1.0rc5  # Fixed performance tests
v0.1.0rc6  # Fixed reconnection tests
v0.1.0rc7  # Added dynamic versioning (next)
v0.1.0     # Production release (future)
```

## Development Checkpoint Tags

You can safely use tags for development checkpoints without triggering CI/CD:

### Safe Checkpoint Tags (Won't Trigger Release CI)
```bash
# These are ignored by release workflow
git tag checkpoint-feature-x
git tag wip-2024-12-26
git tag dev-before-refactor
git tag backup-working-state

# Tags starting with 'v' but not followed by number also safe
git tag v-checkpoint
git tag v-testing
```

### Tags That WILL Trigger CI
```bash
# Any tag matching v[0-9]* triggers release CI
git tag v0.1.0rc7   # Triggers full CI + TestPyPI
git tag v1.0.0      # Triggers full CI + PyPI
git tag v2024-checkpoint  # TRIGGERS CI (starts with v + number)
```

## Quick Reference

| Version Type | Git Tag | PyPI Version | Install Command |
|-------------|---------|--------------|-----------------|
| Alpha | `v0.1.0a1` | `0.1.0a1` | `pip install --pre async-cassandra` |
| Beta | `v0.1.0b1` | `0.1.0b1` | `pip install --pre async-cassandra` |
| RC | `v0.1.0rc1` | `0.1.0rc1` | `pip install --pre async-cassandra` |
| Production | `v0.1.0` | `0.1.0` | `pip install async-cassandra` |

## Additional Resources

- [PEP 440 - Version Identification](https://www.python.org/dev/peps/pep-0440/)
- [Semantic Versioning](https://semver.org/)
- [setuptools-scm documentation](https://github.com/pypa/setuptools_scm)
