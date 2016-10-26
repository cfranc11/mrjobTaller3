from mrjob.job import MRJob
from mrjob.step import MRStep

class c(MRJob):

    def mapper(self, _, line):
        user_id,movie_id,rating,genre,date = line.split(',')
        yield date, 1

    def reducer1(self, date, values):
        moviesCont = 0

        for value in values:
            moviesCont += 1

        yield 1, (moviesCont, date)

    def reducer2(self, date, values):
        max_day = min(values)
        print max_day



    def steps(self):
        return [
           MRStep(mapper=self.mapper, reducer=self.reducer1), MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    c.run()
