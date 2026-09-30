FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /controller .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /controller /controller
USER 65532:65532
ENTRYPOINT ["/controller"]