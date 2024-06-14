SHELL := /bin/bash

checks:
	black .
	mypy .
	ruff check . --fix

black:
	black .
