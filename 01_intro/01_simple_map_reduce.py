from mrjob.job import MRJob

# Taki zapis pozwala na dziedziczenie metod z klasy MRJob.
class MRWordCount(MRJob):

    def mapper(self, _, line):

        # yield pozwala zwrócić generator
        yield 'chars', len(line)
        yield 'word' , len(line.split())

    def reducer(self, key, values):
        yield key, sum(values)

# Wstawienie klauzuli main.
if __name__ == '__main__':
    MRWordCount.run()