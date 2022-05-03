run:
	python ./src/main.py

test: ./src/scheduler.py ./src/test_scheduler.py
	python -m pytest

requirements:
	python -m pip freeze > requirements.txt
