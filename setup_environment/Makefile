SHELL = bash

PROJ_ENV := proj-env

python_packages.txt: python_environment.yml
	-conda env remove --prefix=$(PROJ_ENV) --yes
	conda env create -f $< --prefix=$(PROJ_ENV)
	mkdir -p ../temp
	conda list --prefix=$(PROJ_ENV) > ../temp/$(@F)
	echo "All requested packages are installed" >> ../temp/$(@F) && mv ../temp/$(@F) $@
