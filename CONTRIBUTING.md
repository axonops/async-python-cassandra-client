# Contributing to async-python-cassandra-client

First off, thank you for considering contributing to async-python-cassandra-client! It's people like you that make async-python-cassandra-client such a great tool.

## Code of Conduct

This project and everyone participating in it is governed by the [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior through [GitHub Issues](https://github.com/axonops/async-python-cassandra-client/issues).

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible. Fill out [the required template](.github/ISSUE_TEMPLATE/bug_report.md), the information it asks for helps us resolve issues faster.

**Note:** If you find a **Closed** issue that seems like it is the same thing that you're experiencing, open a new issue and include a link to the original issue in the body of your new one.

#### How Do I Submit A Good Bug Report?

Bugs are tracked as [GitHub issues](https://github.com/axonops/async-python-cassandra-client/issues). Create an issue and provide the following information:

* **Use a clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps which reproduce the problem** in as many details as possible.
* **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy/pasteable snippets, which you use in those examples.
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Explain which behavior you expected to see instead and why.**
* **Include details about your configuration and environment:**
  * Which version of async-python-cassandra-client are you using?
  * What's the version of Python you're running?
  * What's the version of Cassandra you're connecting to?
  * Are you running in a virtual environment?

### Suggesting Enhancements

Enhancement suggestions are tracked as [GitHub issues](https://github.com/axonops/async-python-cassandra-client/issues). Create an issue and provide the following information:

* **Use a clear and descriptive title** for the issue to identify the suggestion.
* **Provide a step-by-step description of the suggested enhancement** in as many details as possible.
* **Provide specific examples to demonstrate the steps**.
* **Describe the current behavior** and **explain which behavior you expected to see instead** and why.
* **Explain why this enhancement would be useful** to most async-python-cassandra-client users.

### Your First Code Contribution

Unsure where to begin contributing? You can start by looking through these `beginner` and `help-wanted` issues:

* [Beginner issues][beginner] - issues which should only require a few lines of code, and a test or two.
* [Help wanted issues][help-wanted] - issues which should be a bit more involved than `beginner` issues.

### Pull Requests

The process described here has several goals:

- Maintain async-python-cassandra-client's quality
- Fix problems that are important to users
- Engage the community in working toward the best possible async-python-cassandra-client
- Enable a sustainable system for async-python-cassandra-client's maintainers to review contributions

Please follow these steps to have your contribution considered by the maintainers:

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

#### Pull Request Title Format

Please use the following format for your PR titles. This is important because PR titles become commit messages when squash-merged:

```
<type>: <description>

# Examples:
feat: Add support for async prepared statements
fix: Resolve connection timeout in retry logic
docs: Update streaming documentation
test: Add integration tests for connection pooling
refactor: Simplify error handling in AsyncSession
chore: Update dependencies to latest versions
perf: Optimize batch query execution
ci: Add Python 3.13 to test matrix
```

**Types:**
- `feat`: New feature or enhancement
- `fix`: Bug fix
- `docs`: Documentation only changes
- `test`: Adding or updating tests
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `chore`: Changes to build process, dependencies, or tooling
- `perf`: Performance improvements
- `ci`: Changes to CI configuration files and scripts
- `style`: Code style changes (formatting, missing semi-colons, etc)
- `revert`: Reverting a previous commit

**Description Guidelines:**
- Use imperative mood ("Add feature" not "Added feature")
- Don't capitalize first letter after the type
- No period at the end
- Keep under 50 characters
- Be specific but concise

#### Pull Request Description

Your PR description should include:
- **What**: Brief summary of changes
- **Why**: The motivation for the changes
- **How**: Technical approach (if not obvious)
- **Testing**: How you tested the changes
- **Breaking changes**: Note any breaking changes

## Development Setup

For detailed development instructions, see our [Developer Documentation](developerdocs/).

Here's how to set up `async-python-cassandra-client` for local development:

1. Fork the `async-python-cassandra-client` repo on GitHub.
2. Clone your fork locally:
   ```bash
   git clone git@github.com:your_name_here/async-python-cassandra-client.git
   ```

3. Install your local copy into a virtualenv:
   ```bash
   cd async-python-cassandra-client/
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   pip install -e ".[dev,test]"
   ```

4. Create a branch for local development:
   ```bash
   git checkout -b name-of-your-bugfix-or-feature
   ```

5. Make your changes locally.

6. When you're done making changes, check that your changes pass the tests:
   ```bash
   # Run linting
   ruff check src tests
   black --check src tests
   isort --check-only src tests
   mypy src

   # Run tests
   make test-unit  # Unit tests only (no Cassandra needed)
   make test-integration  # Integration tests (starts Cassandra automatically)
   make test  # All tests except stress tests
   make test-all  # Complete test suite including linting
   ```

7. Commit your changes and push your branch to GitHub:
   ```bash
   git add .
   git commit -m "Your detailed description of your changes."
   git push origin name-of-your-bugfix-or-feature
   ```

8. Submit a pull request through the GitHub website.

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated.
3. The pull request should work for Python 3.12. Check the GitHub Actions results.
4. Code should follow the project's style guidelines (enforced by black, isort, and ruff).
5. All tests should pass.
6. Coverage should not decrease.

## Testing

### Running Tests Locally

```bash
# Install test dependencies
pip install -e ".[test]"

# Run unit tests (no Cassandra needed)
make test-unit

# Run integration tests (automatically starts Cassandra)
make test-integration

# Run specific test file
pytest tests/unit/test_session.py -v

# Run with coverage
pytest --cov=src/async_cassandra --cov-report=html
```

### Cassandra Management for Testing

Integration tests require a running Cassandra instance. The test infrastructure handles this automatically:

```bash
# Automatically handled by test commands:
make test-integration  # Starts Cassandra if needed
make test             # Starts Cassandra if needed

# Manual Cassandra management:
make cassandra-start  # Start Cassandra container
make cassandra-stop   # Stop and remove container
make cassandra-status # Check if Cassandra is running

# Using your own Cassandra instance:
export CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2
make test-integration
```

The test infrastructure supports both Docker and Podman automatically.

## Code Style

This project uses several tools to maintain code quality and consistency:

- **black** for code formatting
- **isort** for import sorting
- **ruff** for linting
- **mypy** for type checking

Before submitting a PR, ensure your code passes all checks:

```bash
# Format code
black src tests
isort src tests

# Check linting
ruff check src tests

# Check types
mypy src
```

## Documentation

- Use Google-style docstrings for all public APIs
- Include type hints for all function arguments and return values
- Add inline comments for complex logic
- Update README.md if adding new features
- Update docs/ for significant changes

## Commit Message Guidelines

Since we use squash merging, your PR title becomes the commit message. However, if you're working locally:

- Follow the same format as PR titles: `type: description`
- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

**Note**: Individual commit messages within a PR don't need to be perfect since they'll be squashed. Focus on making the PR title excellent!

## Additional Notes

### Issue and Pull Request Labels

This section lists the labels we use to help us track and manage issues and pull requests.

* `bug` - Issues that are bugs.
* `enhancement` - Issues that are feature requests.
* `documentation` - Issues or pull requests related to documentation.
* `good first issue` - Good for newcomers.
* `help wanted` - Extra attention is needed.
* `question` - Further information is requested.
* `wontfix` - This will not be worked on.

## Recognition

Contributors who submit significant pull requests will be added to the project's contributors list. We value all contributions, whether they're code, documentation, or bug reports!

Thank you for contributing to async-python-cassandra-client! 🎉

[beginner]:https://github.com/axonops/async-python-cassandra-client/labels/beginner
[help-wanted]:https://github.com/axonops/async-python-cassandra-client/labels/help%20wanted
