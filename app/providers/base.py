class BaseProvider:
    def __init__(self, symbols):
        self.symbols = symbols

    async def start(self, publish):
        raise NotImplementedError