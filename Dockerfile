FROM python:3.7 as builder

RUN apt-get update \
    && apt-get install -y git \
    && git clone https://github.com/edenhill/librdkafka.git \
    && cd librdkafka \
    && ./configure --prefix=/usr \
    && make \
    && make install

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

ADD . /app
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.7-alpine
WORKDIR /app
COPY --from=builder /usr/lib/librdkafka* /usr/lib/
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app
RUN apk update && apk add --no-cache gcompat
ENV PATH="/opt/venv/bin:$PATH"
CMD [ "python", "./main.py" ]