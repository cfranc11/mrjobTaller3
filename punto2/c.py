from mrjob.job import MRJob
from mrjob.step import MRStep

class subidaEstables(MRJob):

    def mapper1(self, _, line):
        enterprise, stock, date = line.split(',')
        yield enterprise, (date, stock)

    def reducer1(self, enterprise, values):
        stack = []
        for value in values:
            stack.append(value)

        stack.sort()
        yield enterprise, stack

    def reducer2(self, enterprise, values):
        start_day = 0
        stack = []
        # aux = true
        aux = bool(1)
        first = bool(1)

        for value in values:
            for val in value:
                if(first):
                    start_day = int(val[1])
                    first = bool(0)

                #print enterprise, val[1], start_day

                if(int(val[1]) < start_day):
                    aux = bool(0)

        if(aux):
            stack.append(enterprise)
            print stack

    def steps(self):
        return [
           MRStep(mapper=self.mapper1, reducer=self.reducer1), MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    subidaEstables.run()
