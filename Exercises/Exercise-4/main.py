import boto3
import json
import os
import csv
from pandas import json_normalize


def make_json_file_list():
    # 현재 파이썬 파일의 디렉토리 위치를 가져오기
    current_directory = os.path.dirname(os.path.abspath(__file__))
    directory_path = current_directory
    
    json_files = []
    
    # json만 찾아서 파일 리스트 만들기
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
                
    return json_files


def save_csv(json_file, csv_writer):
    with open(json_file, 'r') as file:
        data = json.load(file)
        flattened_data = json_normalize(data).to_dict(orient='records')
        
        for record in flattened_data:
            csv_writer.writerow(record)


def main():
    # your code here
    json_files = make_json_file_list()
    
    csv_filename = 'data.csv'
    
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = set()
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # CSV 파일 헤더 쓰기
        writer.writeheader()
        
        for json_file in json_files:
            save_csv(json_file, writer)
            
    
if __name__ == "__main__":
    main()
