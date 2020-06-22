var data1 = 
{
x: ['Baby Food', 'Fruits', 'Beverages', 'Meat', 'Vegetables', 'Snacks', 'Cereal'],
y: [580741832.8799998, 21842304.390000008, 104777619.05000001, 802096002.3300004, 322906986.91999996, 293117623.5, 401032925.69999987],
type: 'bar', 
name:'renueve'}
;
var data2 = 
{
x: ['Baby Food', 'Fruits', 'Beverages', 'Meat', 'Vegetables', 'Snacks', 'Cereal'],
y: [362667905.8200001, 16200294.359999998, 70197692.50999999, 693347533.9300002, 190587643.25999996, 187189548.00000003, 228317773.10999995],
type: 'bar', 
name:'costo'}
;
var data3 = 
{
x: ['Baby Food', 'Fruits', 'Beverages', 'Meat', 'Vegetables', 'Snacks', 'Cereal'],
y: [218073927.06000003, 5642010.029999999, 34579926.54000001, 108748468.39999999, 132319343.65999997, 105928075.5, 172715152.59],
type: 'bar', 
name:'profit'}
;
data = [data1, data2, data3]; 
layout = {barmode: 'stack'}
;Plotly.newPlot('tarea_reporte', data,layout);
