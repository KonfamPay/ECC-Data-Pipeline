FROM python:3.9.13
WORKDIR /src

COPY src/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "/src/main.py" , "worker", "-l", "info"]