

from utils import sc

class Tercer_archivo():

    data_normal = sc.textFile("police_killings.csv", 14, True).map(lambda line: line.split(",")).collect()[1:]
    data = sc.parallelize(data_normal)


    def first_query(self):
        get_rows = self.data.map(lambda row: (row[3], 1))

        total_race = get_rows.reduceByKey(lambda x, y: x + y)

        total_sort = total_race.sortBy(lambda row: row[1], ascending=False)

        print("primera consulta archivo 3")
        print(total_sort.collect()[:3])


    def second_query(self):

        get_rows = self.data.map(lambda row: (row[5].split("/"), 1))
        get_year = get_rows.map(lambda row: (row[0][2], row[1]))
        total_year = get_year.reduceByKey(lambda x, y: x + y)
        total_sort = total_year.sortBy(lambda row: row[1], ascending=False)


        print("segunda consulta archivo 3")
        #print(total_year.collect()[:5])
        print(total_sort.collect())