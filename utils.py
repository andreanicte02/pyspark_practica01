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


def graph_js(name, x, y, tipo, jsX='x', jsY='y'):

    str= "var data = [\n"
    str += "{\n"
    str += jsX+": " + x +",\n"
    str += jsY+": " + y + ",\n"
    str += "type: '"+tipo+"' \n"
    str += "}\n"
    str += "];\n"
    str += "Plotly.newPlot('"+name+"', data);"

    file = open(name+".js", "w")
    file.write(str)
    file.close()

def graph_js_apilda(name, x, y, tipo, jsX='x', jsY='y', contador=''):

    str = "var data"+ contador +" = \n"
    str += "{\n"
    str += jsX+": " + x +",\n"
    str += jsY+": " + y + ",\n"
    str += "type: '"+tipo+"', \n"
    str += "name:" + "'"+name +"'"
    str += "}\n"
    str += ";\n"

    return str

def write_js_tarea(name, string):
    string += "data = [data1, data2, data3]; \n"
    string += "layout = {barmode: 'stack'}\n;"
    string += "Plotly.newPlot('"+name+"', data,layout);\n"

    file = open(name+".js", "w")
    file.write(string)
    file.close()


def get_ejex_ejey(total):

    d0 = total.map(lambda x: x[0]).collect()
    d1 = total.map(lambda x: x[1]).collect()

    return d0,d1
