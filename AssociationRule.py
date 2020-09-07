from pyspark import SparkConf, SparkContext

source = "C:\\Users\\Lidor\\Desktop\\sparkCourse\\user-ct-test-collection-01.txt"  # todo change to the path file (include the file name!)
CONF = 0.6


def search_query(line):  # todo need to change base on file structure
    details = line.split("\t")
    return details[0], details[1]  # (unique id, query content)


def do_change_info(info):
    x = info[0][0]
    y = info[0][1]
    supp_xuy = info[1]
    return x, (y, supp_xuy)


def calculate_conf(line):
    x = line[0]
    y = line[1][0]
    supp_xuy = line[1][1]
    supp_x = int(rdd_dict[x])
    conf_cal = supp_xuy / supp_x
    return x, y, conf_cal


conf = SparkConf().setMaster("local[*]").setAppName("Association Rule")
try:
    sc = SparkContext(conf=conf)
except:
    sc.stop()
    sc = SparkContext(conf=conf)

# Database
searches = sc.textFile(f"file:///{source}")

# Delete all blank row and mapped the relevant
searches = searches.filter(lambda z: z != "").map(search_query)

searches = searches.distinct().cache()

search_SUPP = searches.map(lambda x: (x[1], 1))
search_SUPP = search_SUPP.reduceByKey(lambda x, y: x + y)  # (Query,SUPP(X))

search_support_XY = searches.join(searches)  # (Tan, (X, Y))
search_support_XY = search_support_XY.filter(lambda z: z[1][0] != z[1][1])  # delete duplicate searches

search_support_XY_SUPP = search_support_XY.map(lambda z: (z[1], 1)).reduceByKey(lambda x, y: x + y)  # ((X, Y), SUPP(XUY))

search_support_XY_SUPP = search_support_XY_SUPP.map(do_change_info)  # (X, (Y,SUPP(XUY)))

rdd_dict = search_SUPP.collectAsMap()  # (X, SUPP_X)

search_support_XY_CONF = search_support_XY_SUPP.map(calculate_conf).cache()  # (X, (Y,SUPP(XUY))) => #(X, Y,CONF(X=>Y))

search_support_XY_CONF_WithFilter = search_support_XY_CONF.filter(lambda z: z[2] > CONF)
print(search_support_XY_CONF_WithFilter.count())
CONF = 0.8
search_support_XY_CONF_WithFilter = search_support_XY_CONF_WithFilter.filter(lambda z: z[2] > CONF)
print(search_support_XY_CONF_WithFilter.count())
CONF = 0.9
search_support_XY_CONF_WithFilter = search_support_XY_CONF_WithFilter.filter(lambda z: z[2] > CONF)
print(search_support_XY_CONF_WithFilter.count())

search_support_XY_CONF_WithFilter = search_support_XY_CONF.filter(lambda z: 0.2 <= z[2] < 1.0)
print(search_support_XY_CONF_WithFilter.take(100))
