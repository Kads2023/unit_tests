class MockLogger:
    class MockLogHandler:
        def info(self, log_string):
            # print(log_string)
            pass

        def error(self, log_string):
            # print(log_string)
            pass

    def __init__(self):
        self.logger = self.MockLogHandler()
