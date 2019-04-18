import os, index, statistics, search
from cleaning_data import send_data

def stat():
    statistics.most_songs()
    statistics.popular_words()
    statistics.famous_name()
    statistics.song_length()


if __name__ == '__main__':

    #Path of the file with the songs
    print("Insert the path where the songs are:")
    path = input()
    # path = r'C:/Users/Giulia/Uni/ADM/Hmw3/shayt/lyrics_collection'
    os.chdir(path)
    files = list(filter(lambda x: ".html" in x, os.listdir('.')))

    #Loop on songs to send to collection in mongo
    for f in files:
        inp = open(r'./%s' % (f), 'r', encoding = 'utf-8')
        content = inp.read()
        inp.close()
        send_data(content)

    #Call the function to execute the query of part 2
    stat()
    #Call the method to create the inverted index on songs in the collection
    index.index()

    #Part 3:

    #Query Type 1
    print("Insert query for type 1:")
    query = input()
    search.term_tf(query)

    #Query Type 2
    print("Insert your query for type 2:")
    query = input()
    search.query_type_two(query)

