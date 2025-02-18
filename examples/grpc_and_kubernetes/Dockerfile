# Use a more recent version of Go that supports go 1.23 and the toolchain directive.
FROM golang:1.23-alpine

RUN mkdir /app
COPY . /app
WORKDIR /app
RUN go build -o bagginsdb .

CMD ["/app/bagginsdb"]
