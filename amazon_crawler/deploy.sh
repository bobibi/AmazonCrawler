#!/bin/bash

curl http://localhost:6800/delproject.json -d project=my_amazon_crawler

scrapy deploy -p my_amazon_crawler
