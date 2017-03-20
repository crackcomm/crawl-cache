FROM alpine:latest
MAINTAINER Łukasz Kurowski <crackcomm@gmail.com>

# Copy application
COPY ./dist/crawl-cache /crawl-cache

#
# Environment variables
# for crawl cache
#
ENV NSQ_ADDR nsq:4150
ENV NSQLOOKUP_ADDR nsqlookup:4161

ENTRYPOINT ["/crawl-cache"]
