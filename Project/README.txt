VM setup:

1)Initialize Kafka and Zookeeper:
	a) sudo systemctl start zookeeper
	b) sudo systemctl status zookeeper
	c) sudo rm /home/kafka/logs/meta.properties
	d) sudo systemctl start kafka
	e) sudo systemctl status kafka
2)Validate that Holidays library and any other necessary libary is installed e.g.:
	a) pip install holidays
	b) pip install kafka-admin-service
3)Assuming all project files are extracted in a "Project" folder that exists in the Desktop (change to where applicable):
	a) cd Desktop/Project/
4)Following the order below create a new terminal instance and run the appropriate file
	a) python3 se1_server.py
		i) wait until the server starts producing results. Verifications at the begining take place 
		   to delete/recreate the topics based on the configuration. Some errors may occur but should be disregarded
	b) python3 investorsDB.py
		i) if executed a second time after the database has been created and populated, 
		   slight chance (1/20) an error will be produced when dropping the DB. 
		   It is because some processes may have not been stopped properly and still access the DB.
		   Please run the command again and no issue will appear. 
	c) python3 se2_server.py
	d) python3 inv1.py
	e) python3 inv2.py
	f) python3 inv3.py
	g) python3 app1.py
	h) python3 app2.py *
		i) when executed, app2.py will produce a series of warnings. Please ignore.

*app2.py should be executed after some data in the database have been populated else some functions may not produce results.
Average time to populate the database with evaluations up to 2023: 4:30 hours
Q4 of app2.py currently is set up with year range 2000-2002

5) Two .txt files will be produced at the active directory that contain the results of the analysis made by app2.py. 
   If the app2.py is run a second time without removing the .txt files first, the results will be appended at the end of the text files
