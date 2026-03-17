import scrapy


class MachineItem(scrapy.Item):
    website_id   = scrapy.Field()
    machine_name = scrapy.Field()
    brand        = scrapy.Field()
    model        = scrapy.Field()
    machine_type = scrapy.Field()
    condition    = scrapy.Field()
    year         = scrapy.Field()
    price        = scrapy.Field()
    currency     = scrapy.Field()
    stock_number = scrapy.Field()
    description  = scrapy.Field()
    specifications = scrapy.Field()  # dict {key: value}
    images       = scrapy.Field()    # list of absolute image URLs
    source_url   = scrapy.Field()
    content_hash = scrapy.Field()
