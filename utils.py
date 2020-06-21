from pyspark import SparkContext

sc = SparkContext("local[*]", "ss2-practica01")


def write_html(name, extra):
    str = "<!DOCTYPE html>\n"
    str+="<head> "+extra+"  </head>\n"
    str+= "<body>\n"
    str+= "<div id='"+name+"'> </div> \n"
    str += "<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>\n"
    str+= "<script src='"+name+".js'></script> \n"
    str += "</body>\n"
    str += "</html>\n"

    file = open(name+".html", "w")
    file.write(str)
    file.close()


def graph_js(name, x, y, tipo):

    str= "var data = [\n"
    str += "{\n"
    str += "x: " + x +",\n"
    str += "y: " + y + ",\n"
    str += "type: '"+tipo+"' \n"
    str += "}\n"
    str += "];\n"
    str += "Plotly.newPlot('"+name+"', data);"

    file = open(name+".js", "w")
    file.write(str)
    file.close()



def get_ejex_ejey(total):

    d0 = total.map(lambda x: x[0]).collect()
    d1 = total.map(lambda x: x[1]).collect()

    return d0,d1
