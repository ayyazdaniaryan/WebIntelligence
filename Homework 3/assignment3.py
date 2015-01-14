#!/usr/bin/env python
import mincemeat
import glob
import stopwords

text_files = glob.glob('/hw3data/*')

def sanitize(input_string):
	output = input_string
	for stopWord in stopwords.allStopWords.keys():
		while stopWord in output:
			output.remove(stopWord)
		while stopWord.upper() in output:
			output.remove(stopWord.upper())
	for word in output:
		word.strip(".,:;'")
		if ('-' in word):
			word.replace("-"," ")

	return output


def file_contents(file_name):
	f = open(file_name)
	try:
		return f.read()
	finally:
		f.close()

source = dict((file_name, file_contents(file_name))
			   for file_name in text_files)

# setup map and reduce functions

def mapfn(key, value):
    for line in value.splitlines():
    	each = line.split(':::')
    	journal = each[0]
    	authors = each[1].split('::')
    	title_words = each[2].split()
    	cleaned_words = sanitize(title_words)

    	for author in authors:
    		for word in cleaned_words:
    			yield author.upper(), word.upper()

def reducefn(key, values):
	result = {}
	for value in values:
		try:
			result[v] += 1
		except KeyError:
			result[v] = 1
	return result


s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")
for key in results.keys():
    words=results[key]
    for word in words.keys():
        print key, ',', word,',',words[word]
