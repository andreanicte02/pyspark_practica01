

from utils import sc

class Primer_archivo:


    data_normal = sc.textFile("video_games_sales.csv", 14, True).map(lambda line: line.split(",")).collect()[1:]
    data = sc.parallelize(data_normal)

    def first_query(self):

        get_rows = self.data.map(lambda row: (row[4], float(row[10])))
        genre_filters= get_rows.filter( lambda row:  (row[0].lower() == "action") \
                                        or (row[0].lower() == "sports") \
                                        or (row[0].lower() == "fighting") \
                                        or (row[0].lower() == "shooter") \
                                        or (row[0].lower() == "racing") \
                                        or (row[0].lower() == "adventure") \
                                        or (row[0].lower() == "strategy") )

        total = genre_filters.reduceByKey(lambda x, y: x + y)




        print("primera consulta archivo 1")
        #print(genre_filters.collect())
        print(total.collect())

    def second_query(self):

        get_rows = self.data.map(lambda row: (row[5] ,row[4], 1))

        rows_nintendo = get_rows.filter(lambda row: row[0].lower() =="nintendo")

        rows_final = rows_nintendo.map(lambda row: (row[1], row[2]))

        total = rows_final.reduceByKey(lambda x, y: x + y)

        print("segunda consulta archivo 2")
        print(rows_nintendo.collect())
        print(total.collect())


    def third_query(self):

        get_rows = self.data.map(lambda row: (row[2], 1))

        total = get_rows.reduceByKey(lambda x, y: x + y)

        total_ordenado = total.sortBy(lambda row: row[1], ascending=False)

        print("tercera consulta archivo 2")
        print(get_rows.collect())
        print(total_ordenado.collect()[0:5])




