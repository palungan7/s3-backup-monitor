import os
import boto3
from flask import Flask, jsonify, render_template
from botocore.exceptions import NoCredentialsError
from datetime import datetime, timezone

app = Flask(__name__)

S3_ENDPOINT = "https://s3member.pajakku.com"
S3_ACCESS_KEY = "optek"
S3_SECRET_KEY = "Cr0tY93nak3n4k!"

s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name='us-east-1'
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/buckets')
def get_buckets():
    try:
        response = s3_client.list_buckets()
        buckets = [{'name': b['Name']} for b in response.get('Buckets', [])]
        return jsonify({'buckets': buckets})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bucket/<bucket_name>')
def get_bucket_detail(bucket_name):
    from flask import request
    try:
        mode = request.args.get('mode', 'fast')
        paginator = s3_client.get_paginator('list_objects_v2')
        latest_obj = None
        total_size = 0
        
        if mode == 'fast':
            # 1. Dapatkan folder root
            root_folders = []
            try:
                resp = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
                for p in resp.get('CommonPrefixes', []):
                    root_folders.append(p.get('Prefix'))
            except:
                pass
                
            # Targetkan folder yang relevan dengan Barman dan Duplicacy
            prefixes_to_scan = ['snapshots/']
            for rf in root_folders:
                prefixes_to_scan.append(rf + 'wals/')
                
            # Fallback root prefix jika tidak ada folder tsb
            prefixes_to_scan.append('') 
            
            for prefix in prefixes_to_scan:
                page_count = 0
                for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                    page_count += 1
                    contents = page.get('Contents', [])
                    if contents:
                        page_latest = max(contents, key=lambda obj: obj['LastModified'])
                        if not latest_obj or page_latest['LastModified'] > latest_obj['LastModified']:
                            latest_obj = page_latest
                    
                    if page_count >= 10:
                        break
        else:
            # Mode DEEP: Kalkulasi seluruh storage
            for page in paginator.paginate(Bucket=bucket_name):
                contents = page.get('Contents', [])
                if contents:
                    for obj in contents:
                        total_size += obj['Size']
                        
                    page_latest = max(contents, key=lambda obj: obj['LastModified'])
                    if not latest_obj or page_latest['LastModified'] > latest_obj['LastModified']:
                        latest_obj = page_latest
                
        if latest_obj:
            last_modified = latest_obj['LastModified']
            
            now = datetime.now(timezone.utc)
            delta = now - last_modified
            hours_ago = delta.total_seconds() / 3600
            
            # 48 hours threshold
            status = 'healthy' if hours_ago < 48 else 'stale'
            
            return jsonify({
                'name': bucket_name,
                'last_update': last_modified.isoformat(),
                'hours_ago': round(hours_ago, 2),
                'status': status,
                'latest_file': latest_obj['Key'],
                'size': total_size if mode == 'deep' else None
            })
        else:
            return jsonify({
                'name': bucket_name,
                'last_update': None,
                'hours_ago': None,
                'status': 'empty',
                'latest_file': None,
                'size': 0
            })
    except Exception as e:
        return jsonify({
            'name': bucket_name,
            'error': str(e),
            'status': 'error'
        })

if __name__ == '__main__':
    app.run(debug=True, port=3000)
