.PHONY: help install test lint build clean publish-test publish

help:
	@echo "Available commands:"
	@echo "  install       Install dependencies"
	@echo "  test          Run tests"
	@echo "  lint          Run linters"
	@echo "  build         Build package"
	@echo "  clean         Clean build artifacts"
	@echo "  publish-test  Publish to TestPyPI"
	@echo "  publish       Publish to PyPI"

install:
	pip install -e ".[dev,test]"

test:
	pytest tests/

lint:
	ruff check src tests
	black --check src tests
	isort --check-only src tests
	mypy src

build: clean
	python -m build

clean:
	rm -rf dist/ build/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

publish-test: build
	python -m twine upload --repository testpypi dist/*

publish: build
	python -m twine upload dist/*
