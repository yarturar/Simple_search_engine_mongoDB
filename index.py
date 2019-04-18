from pymongo import MongoClient
import nltk, mongo_file
from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.stem.snowball import SnowballStemmer



client = MongoClient('mongodb://con:72041sious@ds013405.mlab.com:13405/mongo')
db = client.mongo

#Removes punctuation,stopwords and applies stemming
def preprocess(sentence):
    sentence = sentence.lower()
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(sentence)
    stopset = set(stopwords.words('english'))
    filtered_words =filter(lambda token: token not in stopset, tokens)
    text=" ".join(SnowballStemmer("english").stem(word)  for word in filtered_words if not  word.isdigit() )   #stem
    #word=nltk.word_tokenize(text)
    return text

#Creates the index vector
#Computes the tf of each word and returns each term with its term frequency
def voc_lyrics(cleanList):
    words_counts = Counter(cleanList)
    l = len(words_counts)
    result = [(i, words_counts[i]/l) for i in words_counts]
    return(sorted(result, key=lambda x: x[1],reverse=True))

#Creates the inverted index
#Update the dictionary if the term belongs to the vocabulary values otherwise send it to the database
def inv_idx(vector, vocabulary, c):
    key = [i for i in vector][0]
    for i in vector[key]:
        term = i[0]
        tf = i[1]
        if term in vocabulary.values():
            # update
            term_id_ = list(vocabulary.keys())[list(vocabulary.values()).index(term)]
            db.INVERTED.update_one({term_id_: {"$exists": True}}, {"$push": {term_id_: {"$each": [(key, tf)]}}})

        else:
            # send
            _id = "term_id_" + str(c)
            vocabulary[_id] = term
            data = {_id: [(key, tf)]}
            mongo_file.MongoDB(data, 'INVERTED')
            c += 1


    return vocabulary, c

#The index function creates the vocabulary, which has the following structure:
#{[term_id: term]}
# Also call the other functions to create index vector, inverted index and to send the vectors to the db
#We created two collection: INDEX (with the index vector) and INVERTED (with the vocabulary and inverted index)
# We have decided to save also the index vector, because it is useful for the query of type two in the third part
def index():
    vocabulary, c = {}, 0
    f = 0
    try:
        for docs in db.ADMHMW3.find():
            cleanList = preprocess(docs['Lyrics'])
            _id = docs['_id']

            lst = voc_lyrics(cleanList)
            index_vec = {str(_id): lst}
            mongo_file.MongoDB(index_vec, 'INDEX')
            vocabulary, c = inv_idx(index_vec, vocabulary, c)
            print(f)
            f+=1
    except:
        data = {"vocabulary": vocabulary}
        mongo_file.MongoDB(data, 'INVERTED')
        return 'finished'
