from datetime import datetime


date = datetime.fromtimestamp(1778134925197 / 1000.0).date()
print(date.date())