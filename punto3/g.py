from mrjob.job import MRJob
from mrjob.step import MRStep

class g(MRJob):

    def mapper(self, _, line):
        user_id,movie_id,rating,genre,date = line.split(',')
        yield genre, (movie_id, rating)

    def reducer(self, genre, values):
        moviesCont = 0
        rating = 0

        for value in values:
            print genre, value
            moviesCont += 1
            rating += int(value[1])

        yield 1, (rating, moviesCont, genre)

    def reducer2(self, genre, values):
        for value in values:
            print value
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    g.run()
