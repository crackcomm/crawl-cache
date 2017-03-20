# crawl-cache

[![Circle CI](https://img.shields.io/circleci/project/crackcomm/crawl-cache.svg)](https://circleci.com/gh/crackcomm/crawl-cache)

NSQ crawl queue interceptor caching requests.

## Usage

Example usage from command line:

```sh
# Install command line application for crawl scheduling
$ go install github.com/crackcomm/crawl/nsq/crawl-schedule
# It will consumer `google_search_cache` and produce `google_search`
$ crawl-cache --topic google_search_cache:google_search &
# Schedule crawl of google search results
$ crawl-schedule \
      --topic google_search_cache \
      --callback github.com/crackcomm/go-google-search/spider.Google \
      "https://www.google.com/search?q=Github"
```

Callbacks are currently ignored, only URLs are cached.

## License

                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

## Authors

* [Łukasz Kurowski](https://github.com/crackcomm)
