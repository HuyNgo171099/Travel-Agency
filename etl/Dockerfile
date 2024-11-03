# use the Apache Airflow version 2.5.1 image with the Python version 3.9 image
FROM apache/airflow:2.5.1-python3.9

# copy the Python script and the requirements file to the container
COPY ETL_Pipeline.py /usr/local/airflow/dags/
COPY requirements.txt /requirements.txt

# install all the Python packages specified in the requirements file
RUN pip install -r /requirements.txt

# run Apache Airflow scheduler
CMD ["airflow", "scheduler"]