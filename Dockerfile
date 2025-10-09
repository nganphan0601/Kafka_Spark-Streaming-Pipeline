FROM unigap/spark:3.5

WORKDIR /spark-project

COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

ENV PYTHONPATH="/spark-project"
