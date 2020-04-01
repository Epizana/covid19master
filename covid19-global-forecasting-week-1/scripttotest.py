from datetime import datetime
from pathlib import Path

folderpath = Path('/Users/Emanuel/Documents/2019 Plan/NYC Data Science Academy/Post Bootcamp/COVID-19/Forecasting/covid19-global-forecasting-week-1')
file_to_open = folderpath / 'testfile.txt'

with open(file_to_open,'r+') as f:
	lines = f.readlines()
	current = str(datetime.now().time())
	f.write('\n'+current)


