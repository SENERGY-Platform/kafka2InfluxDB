FROM python:3.7 as builder

RUN apt-get update \
    && apt-get install -y git \
    && git clone https://github.com/edenhill/librdkafka.git \
    && cd librdkafka \
    && ./configure --prefix=/usr \
    && make \
    && make install

ADD . /app
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.7-alpine
WORKDIR /app
COPY --from=builder /usr/lib/librdkafka* /usr/lib/
COPY --from=builder /app /app
CMD [ "python", "./main.py" ]