from mrjob.job import MRJob
from mrjob.step import MRStep


class MRSimpleJob(MRJob):

    # Pozwala na łatwą walidację. Usunięcie np. reducer = self.reducer pozwala na
    # sprawdzenie działania samego mappera.
    def steps(self):
        return [
            MRStep(mapper = self.mapper
                  ,reducer = self.reducer)
        ]

    def mapper(self, _, line):
        yield 'line', 1
        yield 'words', len(line.split())    # liczba słów w tekście
        yield 'chars', len(line)            # liczba znaków

    def reducer(self, key, values):
        yield key, sum(values)


# Wstawienie klauzuli main.
if __name__ == '__main__':
    MRSimpleJob.run()