run:
	python ./src/main.py

test:
	python -m pytest

requirements:
	python -m pip freeze > requirements.txt
