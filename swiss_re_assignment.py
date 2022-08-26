import argparse
import os
import pandas as pd
import json
import numpy as np

def get_data(input_file):
	names_of_columns = [
		"timestamp", "response_header_size",
		"client_ip", "http_response_code",
		"response_size", "http_request_method",
		"url", "username",
		"type_of_access/destination_ip", "response_type"
	]

	try:
		df = pd.read_csv(input_file, compression='gzip', delim_whitespace=True, header=None, names=names_of_columns, skip_blank_lines=True) 
		#error_bad_lines=False if the requirement is results over making sure the input data is correct
	except Exception as e:
		return e
	return df

def get_most_freq_ip(df, ip_type="client_ip"):
	if ip_type == "destination_ip":
		df[["type_of_access","destination_ip"]] = df["type_of_access/destination_ip"].str.split("/", n=-1, expand=True)

	if ip_type in df.columns:
		return df[ip_type].value_counts().to_dict()
	return "failed"

def get_least_freq_ip(df, ip_type="client_ip"):
	if ip_type == "destination_ip":
		df[["type_of_access","destination_ip"]] = df["type_of_access/destination_ip"].str.split("/", n=-1, expand=True)
	if ip_type in df.columns:
		return df[ip_type].value_counts(ascending=True).to_dict()
	return "failed"

def get_events_per_sec(df):
	if "timestamp" in df:
		df["rounded_timestamp"] = df["timestamp"].astype(np.uint32)
		new_df = df.groupby("rounded_timestamp")["rounded_timestamp"].count()
		return float(new_df.mean())
	return "failed"

def total_bytes_exchanged(df):
	if "response_size" in df and "response_header_size" in df:
		return int(df["response_size"].sum() + df["response_header_size"].sum())
	return "failed"

def main(input_files, output_file=None, most_freq_ip=False, least_freq_ip=False, events_per_sec=False, bytes_exchanged=False):
	results = {}
	err_occured = False

	if input_files is str:
		if os.path.isdir(input_files):
			input_files = os.listdir(input_files)
		else:
			input_files = [input_files]

	elif type(input_files) is not list:
		print(f"input file format not supported \"{input_files}\"")
		return 1

	for file in input_files:
		result = {}

		df = get_data(file)

		if type(df) is not pd.DataFrame:
			print(f"loading file \"{file}\" failed with error:\n{df}")
			err_occured = True
			continue

		result["most_freq_client_ip"] = get_most_freq_ip(df, "client_ip")
		result["least_freq_client_ip"] = get_least_freq_ip(df, "client_ip")

		result["most_freq_dest_ip"] = get_most_freq_ip(df, "destination_ip")
		result["least_freq_dest_ip"] = get_least_freq_ip(df, "destination_ip")

		result["total_bytes_exchanged"] = total_bytes_exchanged(df)
		result["avg_events_per_sec"] = get_events_per_sec(df)

		results[file] = result

	if output_file:
		with open(output_file, "w") as f:
			json.dump(results, f)
	else:
		print(results)

	return 1 if err_occured else 0


if __name__ == '__main__':
	#PARSE ARGUMENTS
	parser = argparse.ArgumentParser(description='Parse log files and output selected information.')
	parser.add_argument('input_files', metavar='input_file', type=str, nargs='+',
	                    help='names of input files with data or directory with input files')
	parser.add_argument('-o', '--output', metavar='output_file', type=str, nargs=1,
	                    help='name of output file')

	parser.add_argument('-m', '--most_freq_ip', action="store_true",
	                    help='write in order most frequent IPs (both client and destination)')
	parser.add_argument('-l', '--least_freq_ip', action="store_true", 
	                    help='write in order least frequent IPs (both client and destination)')
	parser.add_argument('-e', '--events_per_sec', action="store_true",
	                    help='average events per second')
	parser.add_argument('-b', '--bytes_exchanged', action="store_true",
	                    help='total amount of bytes exchanged')

	args = parser.parse_args()

	input_files = args.input_files
	output_file = args.output[0] if args.output is not None else None

	mfip, lfip, eps, be = args.most_freq_ip, args.least_freq_ip, args.events_per_sec, args.bytes_exchanged
	
	main(input_files, output_file, mfip, lfip, eps, be)
