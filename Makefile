PEP8=pep8
COVERAGE_FILES=`find juju -name "*py" | grep -v "tests\|lib/mocker.py\|lib/testing.py"`

all:
	@echo "You've just watched the fastest build on earth."

html:
	make -C docs html
	#@gnome-open docs/build/html/index.html

tests:
	./test

coverage:
	python -c "import coverage as c; c.main()" run ./test
	python -c "import coverage as c; c.main()" html -d htmlcov $(COVERAGE_FILES)
	gnome-open htmlcov/index.html

ftests:
	./test --functional

tags:
	@ctags --python-kinds=-iv -R juju

etags:
	@ctags -e --python-kinds=-iv -R juju

modified=$(shell bzr status -S  |grep -P '^\s*M' | awk '{print $$2;}'| grep -P ".py$$")
check:
	@test -n "$(modified)" && echo $(modified) | xargs $(PEP8) --repeat
	@test -n "$(modified)" && echo $(modified) | xargs pyflakes


modified=$(shell bzr status -S -r ancestor:$(JUJU_TRUNK) |grep -P '^\s*M' | awk '{print $$2;}'| grep -P ".py$$")
review:
	@test -n "$(modified)" && echo $(modified) | xargs $(PEP8) --repeat
	@test -n "$(modified)" && echo $(modified) | xargs pyflakes


modified=$(shell bzr status -S -r branch::prev  |grep -P '^\s*\+?[MN]' | awk '{print $$2;}'| grep -P "test_.*\.py$$")
ptests:
	@test -n "$(modified)" && echo $(modified) | xargs ./test

modified=$(shell  bzr status -S -r ancestor:$(JUJU_TRUNK)/|grep -P 'test.*\.py' |awk '{print $$2;}')
btests:
	@./test $(modified)

.PHONY: tags check review
