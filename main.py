# This is a sample Python script.
import findspark
from pyspark import SparkConf, SparkContext
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

findspark.init()

def hello_spark():
    print(f"Hello Apache Spark")
    conf = SparkConf().setMaster('local').setAppName(value="Word Count Example")

    # Create Spark context with necessary configurations
    sc = SparkContext(sparkHome='local', conf=conf)

    # read the text file
    text_file = sc.textFile("file:///Users/rudrak/03-rxDeveloper/pyHelloApacheSpark/README.md")

    # Split each line into words
    token = text_file.flatMap(lambda line: line.split(" "))

    # reduce the map by each key i.e word and count it
    word_count = token.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # save the result
    word_count.saveAsTextFile('file:///Users/rudrak/03-rxDeveloper/pyHelloApacheSpark/output')

    print(f"textfile >> {text_file}")
    print(f"word_count >> {word_count}")


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    hello_spark()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
