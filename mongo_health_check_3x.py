import os
import sys
from bson import ObjectId
from bson.json_util import dumps
from pprint import pprint
from colorama import Fore, Back, Style 

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(-1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
infoworks_temp_dir = os.path.abspath(os.path.join(iw_home, 'temp'))
sys.path.insert(0, infoworks_python_dir)
sys.path.insert(0, infoworks_temp_dir)


from infoworks.core.mongo_utils import mongodb

def entity_total_jobs_count():
	try:
		total_jobs_count=mongodb.jobs.find().count()
		if not total_jobs_count:
			print ('Error finding total job count')
			return
		print(Fore.WHITE+"Total jobs count(source_crawl,fetch_metadata,pipeline_build,test_connection etc): ",total_jobs_count)
		print(Style.RESET_ALL) 
		month_wise_data=mongodb.jobs.aggregate([{'$project':{"month":{'$month':"$createdAt"},"year":{'$year':"$createdAt"}}},{'$group':{"_id":{"month" : "$month" ,"year" : "$year" },"count": { '$sum': 1 }}}])
		print("\n Month wise total jobs run data\n")
		with open(infoworks_temp_dir+"/total_jobs_monthwise_count.json","w") as f:
			f.write("[")
			for document in month_wise_data:
				print(dumps(document)+",")
				f.write(dumps(document)+",")
		if(os.system("sed -i '$ s/.$/]/' /opt/infoworks/temp/total_jobs_monthwise_count.json")==0):
			print(Fore.GREEN+"Successfully created  total_jobs_monthwise_count.json >>>>  "+infoworks_temp_dir+"/total_jobs_monthwise_count.json")
			print(Style.RESET_ALL) 
					
	except Exception as e:
		print('Error: ' + str(e))
		import traceback
		traceback.print_exc()
		print(Fore.RED+'Failed to find the total jobs count')
		print(Style.RESET_ALL) 
		

def entity_job_metrics(entitytype):

	try:
		total_entityjobtype_jobs_completed_count=mongodb.jobs.find({"entityType":entitytype,"status":"completed"}).count()
		total_entityjobtype_jobs_failed_count=mongodb.jobs.find({"entityType":entitytype,"status":"failed"}).count()
		total_entityjobtype_jobs_count=mongodb.jobs.find({"entityType":entitytype}).count()
		if not total_entityjobtype_jobs_count:
			print ('Error finding total '+entitytype+' job count')
			return
		print("Total number of "+entitytype+" jobs:",total_entityjobtype_jobs_count)
		print("Total number of "+entitytype+" completed jobs:",total_entityjobtype_jobs_completed_count)
		print("Total number of "+entitytype+" failed jobs:",total_entityjobtype_jobs_failed_count)
		print("\n")
	except Exception as e:
		print('Error: ' + str(e))
		import traceback
		traceback.print_exc()
		print('Failed to find the total jobs count')
	
def get_month_wise_entity_jobtype_metrics(entitytype):
	try:
		#get month wise specific job type data
		month_wise_specific_jobtype_data=mongodb.jobs.aggregate([{'$match':{"jobType":{'$regex' : entitytype}}},{'$project':{"month":{'$month':"$createdAt"},"year":{'$year':"$createdAt"}}},{'$group':{"_id":{"month" : "$month" ,"year" : "$year" },"count": { '$sum': 1 }}}])	
		print("\nMonth wise "+entitytype+" job count\n")
		with open(infoworks_temp_dir+"/total_jobs_monthwise_"+entitytype+"_count.json","w") as f:
			f.write("[")
			for document in month_wise_specific_jobtype_data:
				print(dumps(document)+",")
				f.write(dumps(document)+",")
		if(os.system("sed -i '$ s/.$/]/' "+infoworks_temp_dir+"/total_jobs_monthwise_"+entitytype+"_count.json")==0):
			print(Fore.GREEN+"Successfully created  total_jobs_monthwise_"+entitytype+"_count.json >>>>  "+infoworks_temp_dir+"/total_jobs_monthwise_"+entitytype+"_count.json")
			print(Style.RESET_ALL) 		
		max_failures_error_reason=mongodb.jobs.aggregate([{"$match":{"status":"failed","entityType":entitytype
		}},{'$group':{"_id":"$errorName","count": { '$sum': 1 }}},{"$sort":{"count":-1}},{ "$limit" : 5 }, {"$addFields": {"error_log_details":"$errorLog"}}])
		print("\nThe reason for max failures was :\n")
		for document in max_failures_error_reason:
			print(dumps(document))
		
	except Exception as e:
		print('Error: ' + str(e))
		import traceback
		traceback.print_exc()
		print(Fore.RED+'Failed to find month wise jobs data')
		print(Style.RESET_ALL) 					

if __name__ == "__main__":
	entity_total_jobs_count()
	entity_job_metrics("source")
	entity_job_metrics("pipeline")
	#get month wise ingestion job metrics
	get_month_wise_entity_jobtype_metrics("source")
	#get_month_wise_entity_jobtype_metrics("source_structured_crawl")
	#get month wise pipeline build job metrics
	get_month_wise_entity_jobtype_metrics("pipeline")
	#get month wise export job metrics
	get_month_wise_entity_jobtype_metrics("export")
	print("Hi")
	
