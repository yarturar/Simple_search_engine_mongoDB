#!/usr/bin/env python
# -*- coding: utf8 -*-

from bs4 import BeautifulSoup
from langdetect import detect
import mongo_file

#Parse the data with BeautifulSoup,html encoding
#Extract the artist,the url,the lyrics-title.
def get_title_artist(soup):
    cnt = soup.title
    title = None
    artist = None
    if cnt != None:
        lst = cnt.string.split(' - ')
        t = lst[-2].split(' Lyrics')
        title = t[0]
        artist = lst[-1]
    return title, artist

def get_url(soup):
    url = None
    for item in soup.find_all(attrs={'class': 'fb-like'}):
            url = item.get('data-href').encode('utf-8')
    return url

def get_lyrics(soup):
    div = soup.find("div", {"id": "content_h", "class": "dn"})
    lyrics = None
    if div != None:
        lyrics = ""
        for i in div.childGenerator():
            if str(type(i)) == '<class \'bs4.element.NavigableString\'>':
                lyrics += i + " "
    return lyrics

def get_language(lyrics):
    language = detect(lyrics)
    return language

#Exclude songs where no lyrics are found
#Include ones that are composed only in English
#Send the documents(songs) to the MongoDb
def send_data(content):
    soup = BeautifulSoup(content, 'html.parser' )

    title, artist = get_title_artist(soup)
    url = get_url(soup)
    lyrics = get_lyrics(soup)
    data = {}

    if lyrics == None or len(lyrics) > 0:
        try:
            lan = get_language(lyrics)
        except:
            lyrics = None
            lan = 'en'
        if lan == 'en' and lyrics != None:
            data = {'Url': url.decode("utf-8"), 'Artist': artist, 'Title': title,
                    'Lyrics': lyrics}
            mongo_file.MongoDB(data, 'ADMHMW3')

    return data