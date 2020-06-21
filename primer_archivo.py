

from utils import sc
from utils import get_ejex_ejey
from utils import graph_js
from utils import write_html

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


        #print(genre_filters.collect())
        print(total.collect())

        xxx = get_ejex_ejey(total)
        print(str(xxx[0]))
        print(str(xxx[1]))

        graph_js("archivo1_reporte1",str(xxx[0]), str(xxx[1]),'bar')
        write_html("archivo1_reporte1", "Ventas globales de la sig categorias")





    def second_query(self):

        get_rows = self.data.map(lambda row: (row[5] ,row[4], 1))

        rows_nintendo = get_rows.filter(lambda row: row[0].lower() =="nintendo")

        rows_final = rows_nintendo.map(lambda row: (row[1], row[2]))

        total = rows_final.reduceByKey(lambda x, y: x + y)

        print("segunda consulta archivo 2")
        #print(rows_nintendo.collect())
        print(total.collect())
        xxx = get_ejex_ejey(total)
        print(str(xxx[0]))
        print(str(xxx[1]))

        graph_js("archivo1_reporte2", str(xxx[0]), str(xxx[1]), 'pie', 'labels', 'values')
        write_html("archivo1_reporte2", "Total de generos publicados por nintendo")

    def third_query(self):

        get_rows = self.data.map(lambda row: (row[2], 1))

        total = get_rows.reduceByKey(lambda x, y: x + y)

        total_ordenado = total.sortBy(lambda row: row[1], ascending=False)

        print("tercera consulta archivo 2")
        #print(get_rows.collect())
        print(total_ordenado.collect()[0:5])

        xxx = get_ejex_ejey(total_ordenado)
        print(str(xxx[0][:5]))
        print(str(xxx[1][:5]))

        graph_js("archivo1_reporte3", str(xxx[0][:5]), str(xxx[1][:5]), 'bar')
        write_html("archivo1_reporte3", "Top 5 de plataformas con mas lanzamientos")





