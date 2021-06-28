from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import boto3
import os

def source_dataset():

	source_dataset_url = 'https://www.clinicaltrials.gov/ct2/results/download_fields?down_count=10000&down_flds=all&down_fmt=csv&cond=COVID-19&flds=a&flds=b&flds=y'

	# throws error occured if there was a problem accessing data
	# otherwise downloads and uploads to s3

	try:
		response = urlopen(source_dataset_url)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, new_filename)

	except URLError as e:
		raise Exception('URLError: ', e.reason, new_filename)

	else:
		data = response.read().decode().splitlines()
		data_set_name = os.environ['DATASET_NAME']

		filename = data_set_name #'-' + endpoint.replace('/', '-')
		file_location = '/tmp/' + filename

		with open(file_location, 'w', encoding='utf-8') as f:
			f.write(data[0].lower().replace(' ', '_').replace('/', '_') + '\n')
			f.write('\n'.join(line for line in data[1:]))
			#f.write(response.read())

		# uploading new s3 dataset
		s3_bucket = os.environ['ASSET_BUCKET']
		new_s3_key = data_set_name + '/dataset/'
		s3 = boto3.client("s3")

		s3.upload_file(file_location, s3_bucket, new_s3_key + filename)

		#s3.upload_file('/tmp/' + new_filename, s3_bucket, new_s3_key)
		return [{'Bucket': s3_bucket, 'Key': new_s3_key + filename}]
