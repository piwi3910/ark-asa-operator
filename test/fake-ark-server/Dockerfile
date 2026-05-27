FROM golang:1.25 AS build
WORKDIR /src
COPY test/fake-ark-server/ ./
RUN CGO_ENABLED=0 go build -o /fake-ark .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /fake-ark /fake-ark
USER 65532:65532
ENTRYPOINT ["/fake-ark"]
