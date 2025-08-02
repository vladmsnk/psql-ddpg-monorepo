# Stage 1: Download modules to cache dependencies.
FROM golang:1.22.2-alpine3.19 as modules
COPY go.mod go.sum /modules/
WORKDIR /modules
RUN go mod download

# Stage 2: Build the application.
FROM golang:1.22.2-alpine3.19 as builder
COPY --from=modules /go/pkg /go/pkg
COPY . /app
WORKDIR /app
RUN GOOS=linux GOARCH=amd64 go build -o /bin/app ./cmd/app/main.go

# Stage 3: Setup the final application environment.
FROM alpine:3.19
# Install PostgreSQL client (includes pgbench)
RUN apk add --no-cache postgresql-client

# Copy necessary files from the builder stage.
COPY --from=builder /app/config/config.yaml /config/config.yaml
COPY --from=builder /bin/app /app

# Set up environment variables and/or default command
CMD ["/app"]
