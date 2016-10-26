from mrjob.job import MRJob
from mrjob.step import MRStep

class promedioUsuarioCalificacion(MRJob):

    def mapper(self, _, line):
        user_id,movie_id,rating,genre,date = line.split(',')
        yield user_id, (1, rating)

    def reducer(self, user_id, values):
        moviesCont = 0
        rating = 0

        for value in values:
            rating+=int(value[1])
            moviesCont += 1

        yield user_id, (moviesCont, rating/moviesCont)

    def steps(self):
        return [
           MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    promedioUsuarioCalificacion.run()
