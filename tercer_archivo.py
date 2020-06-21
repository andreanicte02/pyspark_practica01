

from utils import sc
from utils import get_ejex_ejey
from utils import graph_js
from utils import write_html

class Tercer_archivo():

    data_normal = sc.textFile("police_killings.csv", 14, True).map(lambda line: line.split(",")).collect()[1:]
    data = sc.parallelize(data_normal)


    def first_query(self):
        get_rows = self.data.map(lambda row: (row[3], 1))

        total_race = get_rows.reduceByKey(lambda x, y: x + y)

        total_sort = total_race.sortBy(lambda row: row[1], ascending=False)

        print("primera consulta archivo 3")
        print(total_sort.collect()[:3])

        xxx = get_ejex_ejey(total_sort)
        print(str(xxx[0][:3]))
        print(str(xxx[1][:3]))

        graph_js("archivo3_reporte1", str(xxx[0][:3]), str(xxx[1][:3]), 'bar')
        write_html("archivo3_reporte1", "Top de razas victimas")




    def second_query(self):

        get_rows = self.data.map(lambda row: (row[5].split("/"), 1))
        get_year = get_rows.map(lambda row: (row[0][2], row[1]))
        total_year = get_year.reduceByKey(lambda x, y: x + y)
        total_sort = total_year.sortBy(lambda row: row[1], ascending=False)


        print("segunda consulta archivo 3")
        #print(total_year.collect()[:5])
        print(total_sort.collect())

