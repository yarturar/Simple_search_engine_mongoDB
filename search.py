import math, bson, heapq
import numpy as np

from math import log, sqrt
from index import preprocess, voc_lyrics
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from pymongo import MongoClient
from bson.objectid import ObjectId



client = MongoClient('mongodb://con:72041sious@ds013405.mlab.com:13405/mongo')
db = client.mongo


##TYPE 1

def query_tf(query):
    '''This function will filter given query for stopwords and stem, then will return their tf-'''
    
    d_qr={}
    Q=preprocess(query).split()
   
    length=len(query.split())
    for keys in Q:
        if keys in d_qr:
            d_qr[keys]=d_qr[keys]+1/length
        else:
            d_qr[keys]=1/length
  
    return d_qr


def term_tf():
    '''This function will search for term ids of query words in vocabulary, output with their tf-s '''

    id_query = {}
    l_t = 0
    
    query = query_tf(input())
    vocabulary = db.INVERTED.find({"vocabulary": {"$exists": True}})[1]['vocabulary']

    for keys in query.keys():
        try:
            term_id = list(vocabulary.keys())[list(vocabulary.values()).index(keys)]

        except:
            continue
        id_query[term_id] = query[keys]

    return id_query


def tf_idf_cos():
    '''This function will calculate tf-idf-cosine similarity'''
    total_doc = 100  # consider total number of documents in the collection
    d = dict()
    d1 = {}  # dictionary for storing dot product of query and doc
    d3 = {}  # dictionary for euclidean norm of docs
    sarr = []
    term = term_tf()
    n = 0
    for keys, values in term.items():
        for docs in db.INVERTED.find({keys: {"$exists": True}}):
            #print(docs)
            idf = 1 + math.log(total_doc / len(docs[keys]))  # idf of a term
            term[keys] = term[keys] * idf
            for doc_id, tf in docs[keys]:

                if doc_id in d3.keys():
                    d3[doc_id] = d3[doc_id] + (tf * idf * idf * values)
                    d1[doc_id] = d1[doc_id] + (tf * idf) ** 2
                else:
                    d1[doc_id] = (tf * idf) ** 2
                    d3[doc_id] = (tf * idf * idf * values)

    for values in term.values():
        n += values ** 2

    for keys in d3.keys():
        heapq.heappush(sarr, (d3[keys] / ((n ** 0.5) * (d1[keys] ** 0.5)), keys))  # storing cos similarity in heap
    return sarr


def get_similar_doc_type1():
    '''This will give a list of similar documents: if there are 10-most related documents, 
    these 10 documents will be displayed; otherwise
    all documents with non-zero tf-idf-cosine similarity value will be displayed'''

    count = 0
    similar_docs = tf_idf_cos()
    t = heapq.nlargest(10, similar_docs, key=lambda x: x[0])
    if (len(similar_docs) > 10):
        docs = heapq.nlargest(10, similar_docs, key=lambda x: x[0])
    else:
        docs = heapq.nlargest(len(similar_docs), similar_docs, key=lambda x: x[0])

    print("Tf-idf-cosine_similarity111:", docs)
    return docs

def type_1():
    '''This function will show: firstly, tf-idf-cos similarity of related documents for query,
    secondly, contents of those contents'''
    doc=get_similar_doc_type1()
    for i in range (len(doc)):
        d=db.ADMHMW3.find_one({"_id": ObjectId(doc[i][1])})
        print("__________________________________________________________________")
        print ("Song", i+1)
        print(d)
    return "Type 1 search"

#Qyery Type 2

#generator of the word cloud
#the input is the text of the lyrics in a cluster in an unique string
def generate_word_cloud(text):
    wordcloud = WordCloud().generate(text)

    # Display the generated image:
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()

#Distance between two documents giving format [(_id, score), ..., (_id, score)]
def distance(d1, d2):

    d1_dict, d2_dict = {}, {}
    # Format dict[id] = score
    for _id, _value in d1:
        d1_dict[_id] = _value
    for _id, _value in d2:
        d2_dict[_id] = _value

    # Transform the _ids to sets
    _set_d1, _set_d2 = set(d1_dict.keys()), set(d2_dict.keys())
    # find the union of the sets
    _union = _set_d1.union(_set_d2)
    # Find those who are in both documents
    _intersect = set(d1_dict.keys()).intersection(set(d2_dict.keys()))

    # Find those that are either in one document or in the other
    _rest = _union.difference(_intersect)

    similarity = 0

    # Similarity between matching _ids is (score_1 - score_2)**2
    for _id in _intersect:
        similarity += (d1_dict[_id] - d2_dict[_id]) ** 2
    # similarity between non matching ids is (score)**2
    # if _id is not in d1 KeyError is raised then is in d2.
    for _id in _rest:
        try:
            similarity += (d1_dict[_id]) ** 2
        except KeyError:
            similarity += (d2_dict[_id]) ** 2

    # Return the sqrt of the sum of the values
    return sqrt(similarity)

def query_type_two(q):
    #we "clean" the query and we calculate the tf

    query = voc_lyrics(preprocess(q))
    vocabulary = db.INVERTED.find({"vocabulary": {"$exists": True}})[1]['vocabulary']

    tot_num_doc = db.ADMHMW3.count()
    tot_doc = []

    #we take the inverted index for each term of the query
    for q in query:
        term = q[0]

        try:
            #we take the id of the term from the vocabulary
            term_id = list(vocabulary.keys())[list(vocabulary.values()).index(term)]
            posting = db.INVERTED.find({term_id: {"$exists": True}})[1][term_id]

        except:
            print("The query is impossible, one or more terms aren't in the db")
            return

        res = list(map(tuple, posting))
        docs = list(zip(*res))[0]
        tot_doc.append(set(docs))

    # we take the intersection between the posting lists
    common_doc = list(set.intersection(*tot_doc))

    #print the result to receive the k from the users
    n = len(common_doc)

    print( "The number of the results is", n, "Insert the value of k")
    k = int(input())

    #we clusterize only if the number of documents is a value bigger than k
    if k < n:

        X = np.zeros((n, n))

        updated_common_docs = {}
        dict_docs = {}

        #for each document in the intersection, we take the index list, that has this form: doc_id = [(term_id, tf)], and we update the value with tf*idf
        #we should have done this operation for all songs and store in another collection. This approach would have produced a code faster in this phase, because
        #we would have avoid this double loop
        for idx, doc in enumerate(common_doc):
            dict_docs[idx] = doc
            doc_new = []
            list_term = db.INDEX.find({doc: {"$exists": True}})[0][doc]
            for term_tf in list_term:
                term = term_tf[0]
                tf = term_tf[1]

                term_id = list(vocabulary.keys())[list(vocabulary.values()).index(term)]
                posting_length = len(db.INVERTED.find({term_id: {"$exists": True}})[0][term_id])
                idf = 1 + log(tot_num_doc / posting_length)
                value = tf*idf
                doc_new.append((term, value))

            updated_common_docs[doc] = doc_new

    #we compute the distance matrix
        max_dis = 0
        for i in range(n-1):
            doc_i = updated_common_docs[common_doc[i]]
            for j in range(i+1, n):
                doc_j = updated_common_docs[common_doc[j]]
                d =  distance(doc_i, doc_j)
                max_dis = max(d, max_dis)
                X[i][j] = d
                X[j][i] = d

        #Cluster of songs

        clusters = KMeans(n_clusters=k).fit(PCA(n_components=n).fit_transform(X)).labels_

        clust_dict = {}

        for idx, x in enumerate(clusters):
            try:
                clust_dict[x].append(dict_docs[idx])
            except:
                clust_dict[x] = [dict_docs[idx]]

        res = {}
        text = ""

        #print of the result
        for k, v in clust_dict.items():
            for idx, doc_id in enumerate(v):
                d = db.ADMHMW3.find({"_id": bson.ObjectId(doc_id)})[0]
                try:
                    res[k].append((d['Artist'], d['Title']))
                except:
                    res[k] = [(d['Artist'], d['Title'])]

                text += d["Lyrics"] + " "

        print(res)

        generate_word_cloud(text)

    else:
        print("k is too big, bye!")
        return
