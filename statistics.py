from pymongo import MongoClient
import matplotlib.pyplot as plt
from bson.son import SON
from bson.code import Code
from nltk.stem.snowball import SnowballStemmer


##PART 2: QUERIES

client = MongoClient('mongodb://con:72041sious@ds013405.mlab.com:13405/mongo')
db = client.mongo

stopwords=['a', 'don','ain','just', 'come' 'about', 'above', 'across', 'after', 'afterwards', 'again', 'against', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'amoungst', 'amount', 'an', 'and', 'another', 'any', 'anyhow', 'anyone', 'anything', 'anyway', 'anywhere', 'are', 'around', 'as', 'at', 'back', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'below', 'beside', 'besides', 'between', 'beyond', 'bill', 'both', 'bottom', 'but', 'by', 'call', 'can', 'cannot', 'cant', 'co', 'computer', 'con', 'could', 'couldnt', 'cry', 'de', 'describe', 'detail', 'do', 'done', 'down', 'due', 'during', 'each', 'eg', 'eight', 'either', 'eleven', 'else', 'elsewhere', 'empty', 'enough', 'etc', 'even', 'ever', 'every', 'everyone', 'everything', 'everywhere', 'except', 'few', 'fifteen', 'fify', 'fill', 'find', 'fire', 'first', 'five', 'for', 'former', 'formerly', 'forty', 'found', 'four', 'from', 'front', 'full', 'further', 'get', 'give', 'go', 'had', 'has', 'hasnt', 'have', 'he', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herse"', 'him', 'himse"', 'his', 'how', 'however', 'hundred', 'i', 'ie', 'if', 'in', 'inc', 'indeed', 'interest', 'into', 'is', 'it', 'its', 'itse"', 'keep', 'last', 'latter', 'latterly', 'least', 'less', 'ltd', 'made', 'many', 'may', 'me', 'meanwhile', 'might', 'mill', 'mine', 'more', 'moreover', 'most', 'mostly', 'move', 'much', 'must', 'my', 'myse"', 'name', 'namely', 'neither', 'never', 'nevertheless', 'next', 'nine', 'no', 'nobody', 'none', 'noone', 'nor', 'not', 'nothing', 'now', 'nowhere', 'of', 'off', 'often', 'on', 'once', 'one', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'part', 'per', 'perhaps', 'please', 'put', 'rather', 're', 'same', 'see', 'seem', 'seemed', 'seeming', 'seems', 'serious', 'several', 'she', 'should', 'show', 'side', 'since', 'sincere', 'six', 'sixty', 'so', 'some', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhere', 'still', 'such', 'system', 'take', 'ten', 'than', 'that', 'the', 'their', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'therefore', 'therein', 'thereupon', 'these', 'they', 'thick', 'thin', 'third', 'this', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'top', 'toward', 'towards', 'twelve', 'twenty', 'two', 'un', 'under', 'until', 'up', 'upon', 'us', 'very', 'via', 'was', 'we', 'well', 'were', 'what', 'whatever', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'with', 'within', 'without', 'would', 'yet', 'you', 'your', 'yours', 'yourself', 'yourselves', 'got', '1', 'oh', 'em', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'let', '50', 'yes', 'ha', 'yeah', 't', 's', 'm', 'ma', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']


#Plot of the results in histograms

def plot(names, counts):
    m = counts[0] + 1
    plt.bar(range(len(names)), counts, color='g')
    plt.xticks(range(len(names)), names, rotation='vertical')
    plt.yticks(range(m))
    plt.tight_layout()
    plt.show()

def plot_s_w(n, vec, f):
    plt.bar(range(n), vec.values())
    if f== 1:
        plt.xticks(range(n), vec.keys(), rotation=90)
    plt.show()

# Download of the data given filters

def download_data(pipeline):
    coll = list(db.ADMHMW3.aggregate(pipeline))
    dic = {}

    for i in coll:
        artist = str(i['_id'])
        dic[artist] = int(i['count'])
    dic = sorted(dic.items(), key=lambda value: value[1], reverse=True)

    names = list(zip(*dic))[0]
    counts = list(zip(*dic))[1]
    return names, counts

# Query about the artist that wrote more songs in the db and plot the artist in order from the most active to the less one

def most_songs():
    pipeline = [
        {"$group": {"_id": "$Artist", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1), ("_id", -1)])}
    ]
    
    names, counts = download_data(pipeline)
    plot(names, counts)

# Query about the name of the artist most popular in the db. There is a branch of data mining: Named Entity Recognition.
# We tried to use the library that python offers but they learn the structure of names and they understand if the word is a name of person from the context
# In our case, the only solutions are to create a dictionary with person's name or to decide by hand. We preferred the second option
def famous_name():
    pipeline = [
        {"$project": {"name": {"$arrayElemAt": [{"$split": ["$Artist", " "]}, 0]}}},
        {"$unwind": "$name"},
        {"$group": {"_id": "$name", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1), ("_id", 1)])}

    ]

    names, counts = download_data(pipeline)
    plot(names, counts)

#Query about the most popular word
def popular_words():
    word_count = {}

    # Map Reduce
    map = Code(" function map() {"
               "var cnt = this.Lyrics.toLowerCase();"
               "var words = cnt.match(/\w+/g);"
               "if (words == null) {"
               " return;"
               " }"
               "for (var i = 0; i < words.length; i++) {"
               "emit({ word:words[i] }, { count:1 });"
               "}"
               "}")

    reduce = Code(" function reduce(key, counts) {"
                  "var cnt = 0;"
                  "for (var i = 0; i < counts.length; i++) {"
                  "cnt = cnt + counts[i].count;"
                  " }"
                  "return { count:cnt };"
                  "}")
    # Getting sorted data
    results = db.ADMHMW3.map_reduce(map, reduce, 'rr')
    for i in db.rr.find().sort("value", -1):
        word_count[i['_id']['word']] = i['value']['count']
    # Filtering data: stopwords and stemming
    stemmer = SnowballStemmer("english")

    mydict = {k: v for k, v in word_count.items() if k not in stopwords}
    pop_word = {}
    final = {}
    for key, values in mydict.items():
        st_key = stemmer.stem(key)
        if st_key in pop_word.keys():
            pop_word[st_key] = pop_word[st_key] + mydict[key]
        else:
            pop_word[st_key] = mydict[key]
    twenty_words = sorted(pop_word.items(), key=lambda kv: kv[1], reverse=True)
    for i in range(20):
        final[twenty_words[i][0]] = twenty_words[i][1]
    print(final)

    plot_s_w(20, final, 1)

#If we run it, we can see that "love" is the most popular word in songs, which means that this is main theme for most songs

#Query about the length of songs
def song_length():
    
    map = Code(" function map() {"
               "var cnt = this.Lyrics.toLowerCase();"
               "var words = cnt.match(/\w+/g);"
               "if (words == null) {"
               " return;"
               " }"
               "emit( {len: words.length} ,  {count: 1} );"
               "}")

    reduce = Code(" function reduce(key, counts) {"
                  "var cnt = 0;"
                  "for (var i = 0; i < counts.length; i++) {"
                  "cnt = cnt + counts[i].count;"
                  " }"
                  "return { count:cnt };"
                  "}")

    # Getting sorted data
    song = {}
    results = db.ADMHMW3.map_reduce(map, reduce, 'anot')
    for i in db.anot.find().sort("value", -1):
        song[i['_id']['len']] = i['value']['count']

    plt.bar(list(song.keys()), song.values())

    plt.show()
#Most of the songs have length in range between 200-300 words!
