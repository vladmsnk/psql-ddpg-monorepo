FROM golang:1.22.2-alpine3.19 as modules
COPY ../go.mod go.sum /modules/
WORKDIR /modules
RUN go mod download

FROM golang:1.22.2-alpine3.19 as builder
COPY --from=modules /go/pkg /go/pkg
COPY .. /app
WORKDIR /app
RUN GOOS=linux GOARCH=amd64 go build -o /bin/app ./cmd/app/main.go

FROM scratch
COPY --from=builder /app/config/config.yaml /config/config.yaml
COPY --from=builder /bin/app /app
CMD ["/app"]
