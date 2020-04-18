import re

# Budowa wyrażeń regularnych. Poniższy regex będzie szukał znaków, mających
# co najmniej jeden tzw. word character.
WORD_RE = re.compile(r'[\w]+')

words = WORD_RE.findall('Big data, hadoop and map reduce. (hello world)')
print(words)