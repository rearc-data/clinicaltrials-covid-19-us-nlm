from urllib.request import urlopen
import boto3

def source_dataset(new_filename, s3_bucket, new_s3_key):

	source_dataset_url = 'https://www.clinicaltrials.gov/ct2/results/download_fields?down_count=10000&down_flds=all&down_fmt=csv&cond=COVID-19&flds=a&flds=b&flds=y'

	data = urlopen(source_dataset_url).read().decode().splitlines()

	with open('/tmp/' + new_filename, 'w', encoding='utf-8') as f:
		f.write(data[0].lower().replace(' ', '_').replace('/', '_') + '\n')
		f.write('\n'.join(line for line in data[1:]))

	# uploading new s3 dataset
	s3 = boto3.client("s3")

	s3.upload_file('/tmp/' + new_filename, s3_bucket, new_s3_key)
	return [{'Bucket': s3_bucket, 'Key': new_s3_key}]