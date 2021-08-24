# Starts a spark jupyter notebook for testing/exploring data
# Tested on MacOS only

start:
	@echo Creating pysparkNotebook...

# The start.sh line disables authentication for jupyter
	@docker create \
		-p 8888:8888 \
		-p 4040:4040 \
		--name pysparkNotebook \
		--rm \
		jupyter/pyspark-notebook:spark-3.1.1 \
		start.sh jupyter notebook --NotebookApp.token=''

	@echo Copying data and notebooks to server...
	@docker cp ../original_data/ pysparkNotebook:/home/jovyan/work
	@docker cp ../notebooks/'Flight-Delay CSV to Parquet.ipynb' pysparkNotebook:/home/jovyan/work/

	@echo Starting pysparkNotebook...
	@docker start pysparkNotebook
	@echo pysparkNotebook started...

stop:
	docker stop pysparkNotebook && docker rm pysparkNotebook

restart:stop start
