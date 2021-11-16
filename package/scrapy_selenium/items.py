import scrapy

class SqlItem(scrapy.Item):
    _table_ = None
    _save_ = True

    def as_dict(self) -> dict:
        return dict(self)

