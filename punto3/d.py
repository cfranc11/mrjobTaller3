from mrjob.job import MRJob
from mrjob.step import MRStep

class d(MRJob):

    def mapper(self, _, line):
        user_id,movie_id,rating,genre,date = line.split(',')
        yield movie_id, (1, rating)

    def reducer(self, movie_id, values):
        usersCont = 0
        rating = 0

        for value in values:
            rating+=int(value[1])
            usersCont += 1

        yield movie_id, (usersCont, rating/usersCont)

    def steps(self):
        return [
           MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    d.run()
