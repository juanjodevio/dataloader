# Contributing to DataLoader

Thank you for your interest in contributing to DataLoader! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for all contributors.

## How to Contribute

### Reporting Bugs

1. **Check existing issues**: Before creating a new issue, search existing issues to see if the bug has already been reported.
2. **Create an issue**: Use the bug report template and include:
   - Clear description of the bug
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Python version, DataLoader version)
   - Relevant error messages or logs

### Suggesting Features

1. **Check existing issues**: Search for similar feature requests.
2. **Create an issue**: Use the feature request template and include:
   - Clear description of the feature
   - Use case and motivation
   - Proposed implementation approach (if you have ideas)
   - Examples of how it would be used

### Contributing Code

#### Setting Up Development Environment

1. **Fork the repository** on GitHub
2. **Clone your fork**:
   ```bash
   git clone https://github.com/your-username/dataloader.git
   cd dataloader
   ```
3. **Install in development mode**:
   ```bash
   pip install -e ".[all,dev]"
   ```
4. **Run tests** to ensure everything works:
   ```bash
   pytest
   ```

#### Development Workflow

1. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

2. **Make your changes**:
   - Follow the existing code style
   - Add tests for new features
   - Update documentation as needed
   - Ensure all tests pass

3. **Run tests and linting**:
   ```bash
   # Run all tests
   pytest
   
   # Run with coverage
   pytest --cov=dataloader --cov-report=html
   
   # Run specific test file
   pytest tests/unit/test_your_feature.py
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Add feature: description of your change"
   ```
   
   Use clear, descriptive commit messages:
   - Start with a verb (Add, Fix, Update, Remove)
   - Be specific about what changed
   - Reference issue numbers if applicable: `Fix #123: description`

5. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request**:
   - Go to the original repository on GitHub
   - Click "New Pull Request"
   - Select your branch
   - Fill out the PR template
   - Link any related issues

#### Code Style

- **Python style**: Follow PEP 8
- **Type hints**: Use type hints for function parameters and return types
- **Docstrings**: Use Google-style docstrings for all public functions and classes
- **Line length**: Keep lines under 100 characters when possible
- **Imports**: Group imports (stdlib, third-party, local) with blank lines

Example:
```python
"""Module docstring."""

from typing import Any, Iterable

from dataloader.core.batch import Batch
from dataloader.core.state import State


def my_function(param: str) -> list[str]:
    """Function docstring.
    
    Args:
        param: Description of parameter.
    
    Returns:
        Description of return value.
    """
    return [param]
```

#### Testing Guidelines

- **Write tests** for all new features and bug fixes
- **Test coverage**: Aim for high test coverage, especially for core functionality
- **Test organization**:
  - Unit tests in `tests/unit/`
  - Integration tests in `tests/integration/`
  - Use descriptive test names: `test_what_it_tests_when_condition`
- **Use fixtures**: Leverage pytest fixtures from `conftest.py`
- **Mock external dependencies**: Use mocks for external services (S3, databases, etc.)

Example test:
```python
def test_connector_reads_batches_correctly():
    """Test that connector reads batches with correct format."""
    connector = MyConnector(config)
    batches = list(connector.read_batches(state))
    assert len(batches) > 0
    assert isinstance(batches[0], ArrowBatch)
```

#### Documentation

- **Update README.md** if you add new features or change behavior
- **Update ARCHITECTURE.md** for architectural changes
- **Update CHANGELOG.md** in the `[Unreleased]` section
- **Add docstrings** to all public functions, classes, and methods
- **Include examples** in docstrings for complex functionality

#### Pull Request Guidelines

- **Keep PRs focused**: One feature or bug fix per PR
- **Keep PRs small**: Easier to review and merge
- **Update documentation**: Include doc updates in the same PR
- **Add tests**: Include tests for your changes
- **Update CHANGELOG**: Add entry in `[Unreleased]` section
- **Describe changes**: Clear description of what changed and why

PR Template:
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Tests pass locally
- [ ] Added tests for new functionality
- [ ] Updated documentation

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex code
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] No new warnings
```

## Project Structure

```
dataloader/
├── dataloader/          # Main package
│   ├── api.py          # Public API
│   ├── cli/            # CLI commands
│   ├── connectors/     # Connector implementations
│   ├── core/           # Core engine, state, batch
│   ├── models/         # Pydantic models
│   └── transforms/     # Transform implementations
├── tests/              # Test suite
│   ├── unit/           # Unit tests
│   └── integration/   # Integration tests
├── examples/           # Example recipes
└── docs/              # Documentation (if applicable)
```

## Adding New Connectors

1. **Create connector module** in `dataloader/connectors/your_connector/`
2. **Implement Connector protocol**:
   - `read_batches(state: State) -> Iterable[Batch]`
   - `write_batch(batch: Batch, state: State) -> None`
3. **Create config model** using Pydantic
4. **Register connector** using `@register_connector` decorator
5. **Add tests** in `tests/unit/test_your_connector.py`
6. **Update documentation** (README, ARCHITECTURE)

## Adding New Transforms

1. **Create transform class** in `dataloader/transforms/`
2. **Implement Transform protocol**:
   - `apply(batch: Batch) -> Batch`
3. **Register transform** using `@register_transform` decorator
4. **Add tests** in `tests/unit/test_transforms.py`
5. **Update documentation**

## Questions?

- Open an issue for questions about implementation
- Check existing issues and discussions
- Review ARCHITECTURE.md for design decisions

Thank you for contributing to DataLoader! 🎉

