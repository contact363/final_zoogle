import scrapy


class MachineItem(scrapy.Item):
    # Required
    machine_url = scrapy.Field()
    website_id = scrapy.Field()
    website_source = scrapy.Field()

    # Core
    machine_type = scrapy.Field()
    brand = scrapy.Field()
    model = scrapy.Field()
    price = scrapy.Field()
    currency = scrapy.Field()
    location = scrapy.Field()
    description = scrapy.Field()

    # Media
    images = scrapy.Field()        # list of URLs
    image_paths = scrapy.Field()   # filled by ImageDownloadPipeline

    # Structured specs
    specs = scrapy.Field()         # dict: {key: value}
