from xml.etree import ElementTree

xml = ElementTree.parse("sample_tpcc_config.xml")

root = xml.getroot()

#dbtype
root.find("dbtype").text = "peloton"

#driver
root.find("driver").text = "org.postgresql.Driver"

#DBUrl
root.find("DBUrl").text = "jdbc:postgresql://{0}:{1}/{2}".format("localhost", "15721", "tpcc") #host, port and benchmark name

#dbname = ElementTree.Element("DBName")

#root.append(dbname)

#root.find("DBName").text = 	"tpcc" #benchmark name

#Username
root.find("username").text = "postgres"

#password
root.find("password").text = "postgres"

#isolation
root.find("isolation").text = "TRANSACTION_SERIALIZABLE" #should be the isolation selected in startup

#scalefactor
root.find("scalefactor").text = "1" #should be scale factor

#workload definition
#terminals
root.find("terminals").text = "1" #should be terminals
#works
for work in root.find('works').findall('work'):
	#time
	work.find('time').text ="20" #should be time running
	#rate
	work.find('rate').text ="unlimited"
	#weights
	work.find('weights').text = "45,43,4,4,4" #should be the weight vector 

xml.write('tpcc_config.xml')