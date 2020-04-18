from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# Stworzenie wyrażenia regularnego.
WORD_RE = re.compile(r'[\w]+')


class MRMostCommonWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words
                   , reducer=self.reducer_count_words)
            , MRStep(mapper=self.mapper_get_keys
                     , reducer=self.reducer_find_most_common_word)
        ]

    # Mapowanie słów w Iliadzie.
    def mapper_get_words(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self, key, values):
        yield key, sum(values)

    def mapper_get_keys(self, key, value):
        yield None, (value, key)

    def reducer_find_most_common_word(self, key, values):
        # Zostaje wygenerowany obiekt, dla którego pierwsza wartość krotki jest maksymalna.
        yield max(values)


if __name__ == '__main__':
    MRMostCommonWord.run()
