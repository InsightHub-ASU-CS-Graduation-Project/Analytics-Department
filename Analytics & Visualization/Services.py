class PageBuilder:
    def __init__(self, page_config: dict):
        self.page_config = page_config

    def build(self, request_data):
        result = {}
        
        for key, func in self.page_config.items():
            result[key] = func(request_data)
            
        return result