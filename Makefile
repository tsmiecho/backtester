SHELL := /bin/bash

checks:
	black .
	ruff check . --fix
	mypy .
