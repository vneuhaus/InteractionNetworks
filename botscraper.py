import requests
from bs4 import BeautifulSoup
import numpy as np

bots = []
for i in range(277):
    url = 'https://botrank.pastimes.eu/?sort=rank&page={}'.format(i)
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    for link in soup.find_all('a'):
        if 'https://www.reddit.com/user/' in link.get('href'):
            bots.append(link.string)
np.save('bots', bots)