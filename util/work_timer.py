import time

class WorkTimer():
    def __init__(self) -> None:
        self.start_time: float = 0
        self.seconds_total: float = 0
        self.days: float = 0
        self.hours: float = 0
        self.minutes: float = 0
        self.seconds: float = 0

    def start(self) -> None:
        self.start_time = time.time()

    def calculate_time(self, index: int, count: int) -> None:
        seconds_delta = (time.time() - self.start_time)
        self.seconds_total += seconds_delta
        seconds_mean = (self.seconds_total / (index + 1)) * (count - index)
        self.days = round(seconds_mean / 86400)
        self.hours = round((seconds_mean - self.days  * 86400) / 3600)
        self.minutes = round((seconds_mean - self.days  * 86400 - self.hours * 3600) / 60)
        self.seconds = round((seconds_mean - self.days  * 86400 - self.hours * 3600 - self.minutes * 60))
        if (self.seconds < 0):
            self.seconds = 0
        if (self.minutes < 0):
            self.minutes = 0
        if (self.seconds < 0):
            self.seconds = 0
