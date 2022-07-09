class NUFORCEvent:
    def __init__(self,  url=None, occurred_time=None, reported_time=None, entered_as_time=None, shape=None, duration=None,
                 city=None, state=None, state_abbreviation=None, country=None, description=None, report_ok=None, raw_event=None,):
        self.report_ok = report_ok
        self.raw_event = raw_event
        self.url = url
        self.occurred_time = occurred_time
        self.reported_time = reported_time
        self.entered_as_time = entered_as_time
        self.shape = shape
        self.duration = duration
        self.city = city
        self.state = state
        self.state_abbreviation = state_abbreviation
        self.country = country
        self.description = description


    def show(self):
        print(f"""
Report OK: {self.report_ok}
URL: {self.url}
Occurred time: {self.occurred_time}
Reported time: {self.reported_time}
Entered-as time: {self.entered_as_time}
Shape: {self.shape}
Duration: {self.duration}
City: {self.city}
State: {self.state}
State abbreviation: {self.state_abbreviation}
Country: {self.country}
Description: {self.description}
""")