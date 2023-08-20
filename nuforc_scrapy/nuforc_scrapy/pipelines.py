# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os
import shutil
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime


class CsvPipeline:
    def __init__(self):
        load_dotenv()
        self.validate_directory_tree()
        self.output_dir = Path(os.getenv("DATA_DIR"))

        current_date = datetime.now().strftime('%Y_%m_%d')
        self.output_filepath = self.output_dir / "raw_scrapy_output" / f"events_{current_date}.csv"
        self.output_copy_filepath = self.output_dir / "raw_events" / f"events_{current_date}.csv"
        self.file = open(self.output_filepath, "w", newline="", encoding="utf-8")
        self.writer = None

    def validate_directory_tree(self):
        paths = [
            Path(os.getenv("DATA_DIR")),
            Path(os.getenv("DATA_DIR")) / "raw_scrapy_output",
            Path(os.getenv("DATA_DIR")) / "raw_events",
            Path(os.getenv("DATA_DIR")) / "gis",
            Path(os.getenv("DATA_DIR")) / "visualisation",
        ]
        for path in paths:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)

    def process_item(self, item, spider):
        if self.writer is None:
            fieldnames = item.keys()
            self.writer = csv.DictWriter(self.file, fieldnames=fieldnames)
            self.writer.writeheader()
        self.writer.writerow(item)
        return item

    def close_spider(self, spider):
        self.file.close()
        shutil.copy2(self.output_filepath, self.output_copy_filepath)
