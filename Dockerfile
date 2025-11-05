FROM golang:1.24 AS builder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . .
RUN make build

FROM gcr.io/distroless/static-debian11:nonroot
COPY --from=builder /app/cloudfront-logs-shipper /cloudfront-logs-shipper
ENTRYPOINT ["/cloudfront-logs-shipper"]
