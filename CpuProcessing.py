import collections
import multiprocessing
import re
import time
from multiprocessing import Pool


def ProcessLine(lines: list, stopWords: list):
    filteredData = collections.Counter()
    for line in lines:
        line = line.strip().lower()
        stopWords = [stopWord.strip().lower() for stopWord in stopWords]
        words = re.split(r"\W", line)
        for word in words:
            word = ''.join(e for e in word if e.isalnum())
            if 4 <= len(word) <= 8 and word not in stopWords:
                filteredData[word] += 1
    return filteredData


def SplitIntoChunks(lines: list, noOfChunks):
    for i in range(0, noOfChunks):
        yield lines[i::noOfChunks]


def PrintStatistics(filteredData: collections.Counter, start, stop):
    mostFrequentWord = filteredData.most_common()[0]

    leastFrequentWord = filteredData.most_common()[-1]

    sumOfWords = sum(filteredData.values())

    mostFrequentWords = list(
        filter(lambda x: True if filteredData.get(x) == mostFrequentWord[1] else False, filteredData))
    leastFrequentWords = list(
        filter(lambda x: True if filteredData.get(x) == leastFrequentWord[1] else False, filteredData))

    if len(mostFrequentWords) == 1:
        print(f"Most frequent word is: '{mostFrequentWord[0]}' and occurred {mostFrequentWord[1]} times.")
    else:
        print(f"Most frequent word occurred {mostFrequentWord[1]} times and the example is '{mostFrequentWord[0]}'.")
    if len(leastFrequentWords) == 1:
        print(f"Least frequent word is: '{leastFrequentWord[0]}' and occurred {leastFrequentWord[1]} times.")
    else:
        print(f"Least frequent word occurred {leastFrequentWord[1]} times and the example is '{leastFrequentWord[0]}'.")

    print(f"Total number of filtered words is: {sumOfWords}.")
    print(f"Total processing time was: {round(stop - start, 7)*1000} ms.\n")


def SingleThread(dataPath, stopWordsPath):
    start = time.time()
    with open(stopWordsPath, "r") as stopWords:
        with open(dataPath, "r") as data:
            filteredData = ProcessLine(data.readlines(), stopWords.readlines())
    stop = time.time()
    print("---CPU SINGLE THREAD---")
    PrintStatistics(filteredData, start, stop)

    return filteredData


def MultiThread(dataPath, stopWordsPath):
    start = time.time()
    result = collections.Counter()

    with open(stopWordsPath, "r") as stopWordsFile:
        stopWords = stopWordsFile.readlines()
    with open(dataPath, "r") as data:
        lines = SplitIntoChunks(data.readlines(), multiprocessing.cpu_count())
    with Pool(multiprocessing.cpu_count()) as pool:
        filteredData = pool.starmap(ProcessLine, [(line, stopWords) for line in lines])
    for data in filteredData:
        result += data
    stop = time.time()
    print("---CPU MULTI THREAD---")
    PrintStatistics(result, start, stop)


SingleThread("data.txt", "stop_words.txt")
MultiThread("data.txt", "stop_words.txt")
