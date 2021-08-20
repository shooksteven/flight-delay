# Only tested in MacOS

start:
	@echo Creating pysparkNotebook...

# The start.sh line disables authentication for jupyter
	@docker create \
		-p 8888:8888 \
		-p 4040:4040 \
		--name pysparkNotebook \
		jupyter/pyspark-notebook:spark-3.1.1 \
		start.sh jupyter notebook --NotebookApp.token=''

	@echo Copying data and notebooks to server...
	@docker cp data/ pysparkNotebook:/home/jovyan/work
	@docker cp Flight_Delay.ipynb pysparkNotebook:/home/jovyan/work/

	@echo Starting pysparkNotebook...
	@docker start pysparkNotebook
	@echo pysparkNotebook started...

stop:
	docker stop pysparkNotebook && docker rm pysparkNotebook

restart:stop start

