from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('TotalSpent')
sc = SparkContext(conf=conf)

def parse_line(line):
    fields = line.split(',')
    customer = fields[0]
    amount = float(fields[2])
    return (customer, amount)

lines = sc.textFile('customer-orders.csv')
rdd = lines.map(parse_line)
total_spent = rdd.reduceByKey(lambda accum, x: accum + x)

results = total_spent.collect()
for result in results:
    print('{}: {:.2f}'.format(result[0], result[1]))
