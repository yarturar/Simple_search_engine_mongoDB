import requests, time
import cleaning_data
from bs4 import BeautifulSoup


## Get Pages of Lyrics

# In this function we iterate on the artist. We get the link and we call the function that extracts the songs.

def search_link(req, soup, url):
    div_group = soup.find("div", {"class":"col-sm-6 text-center artist-col"})
    for i in div_group.childGenerator():
        if i.name == 'a':
            link = url + i.get('href')
            artist_page = req.get(link)
            extract_songs(req, BeautifulSoup(artist_page.content, 'html.parser'), url)
            time.sleep(2)

# For each song, we get the page and we call the module to get the information that we need.

def extract_songs(req, soup, url):
    div_album = soup.find("div", {"id": "listAlbum"})
    for i in div_album.childGenerator():
        if i.name == 'a':
            try:
                link_song = url + i.get('href')[3:]
                song = req.get(link_song)
                cleaning_data.send_data(song.content)
                time.sleep(2)
            except:
                continue


def define_session():
    # The session is initializing and we get the home page of the web site.
    req = requests.Session()
    sub_url = 'https://www.azlyrics.com/'
    page = req.get(sub_url)

    # First of all we take links to the list of the authors per letter. For each iteration, we call time.sleep to allow the download of the page.
    soup = BeautifulSoup(page.content, 'html.parser')
    all_a = soup.find_all("a", {"class": "btn btn-menu"})
    for link in all_a:
        url = link.get('href')
        letter_page = req.get(url)
        search_link(req, BeautifulSoup(letter_page.content, 'html.parser'), sub_url)
        time.sleep(2)



