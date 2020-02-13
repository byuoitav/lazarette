FROM gcr.io/distroless/static
MAINTAINER Daniel Randall <danny_randall@byu.edu>

COPY lazarette-linux-amd64 /lazarette

ENTRYPOINT ["/lazarette"]
