FROM golang:1.24 AS builder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . .
RUN make build

FROM gcr.io/distroless/static-debian11:nonroot
COPY --from=builder /app/cf-logs-loki-uploader /cloudfront-logs-shipper
ENTRYPOINT ["/cloudfront-logs-shipper"]
