# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import pickle

class PickleExportPipeline:
    def __init__(self, file_path):
        self.file_path = file_path
        self.items = []

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        file_path = settings.get('PICKLE_EXPORT_FILE')
        return cls(file_path)

    def process_item(self, item, spider):
        self.items.append(item)
        return item

    def close_spider(self, spider):
        with open(self.file_path, 'wb') as f:
            pickle.dump(self.items, f)