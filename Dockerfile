FROM golang:1.18-alpine AS build
ENV CGO_ENABLED=0
WORKDIR /home
COPY . .
RUN go mod download
RUN go install .
RUN adduser -D -g '' user

FROM scratch AS bowfin
LABEL Author="Michael Wilson"
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /go/bin/bowfin /bin/bowfin
USER user
ENTRYPOINT ["/bin/bowfin"]