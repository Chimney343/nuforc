# nuforc

### Setup with `poetry`

To set up the project, run:
```commandline
poetry shell
poetry install
```

### Scraping the NUFORC dataset
To run the `scrapy` Spider `nuforc-spider` that downloads the raw NUFORC event data:
```commandline
cd nuforc_scrapy
scrapy crawl nuforc-spider -o nuforc.csv
```
This will run the `nuforc-spider` in the terminal and save the output in `nuforc_scrapy` directory. This data is ready for further analysis. 
