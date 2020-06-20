
from utils import sc

class Segundo_archivo:


    data_normal = sc.textFile("sales.csv", 14, True).map(lambda line: line.split(",")).collect()[1:]
    data = sc.parallelize(data_normal)

    def first_query(self):

        get_rows = self.data.map(lambda row: (row[0], float(row[13])))
        total = get_rows.reduceByKey(lambda x, y: x + y)
        print("primera consulta archivo 2")
        print(get_rows.collect())
        print(total.collect())



    def second_query(self):
        #use la orden
        get_rows = self.data.map(lambda row: (row[1], row[5].split("/")[2], int(row[8]))) \
            .filter(lambda row: row[0].lower() == 'guatemala')

        total = get_rows.map(lambda row: (row[1], row[2])).reduceByKey(lambda x, y: x+y)

        orden = total.sortBy(lambda row: row[1], ascending = False)

        #ventas_anio = get_rows.filter(lambda row: row[1]=="2019")
        #GUATEMLA
        print("segunda consulta archivo 2")
        print(get_rows.collect())
        print(total.collect())
        print(orden.collect())


    def third_query(self):

        get_rows = self.data.map(lambda row: (row[5].split("/")[2], row[1], float(row[13])))
        year2010 = get_rows.filter(lambda row: row[0]=="2010")

        total = year2010.map(lambda row: (row[1], row[2])).reduceByKey(lambda x, y: x + y)
        total_ordenado = total.sortBy(lambda row: row[0])

        print("tercera consulta archivo 2")
        print(get_rows.collect())
        print(year2010.collect())
        print(total.collect())
        print(total_ordenado.collect())