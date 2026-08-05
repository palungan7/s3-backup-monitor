import os
import boto3
import json
import threading
from flask import Flask, jsonify, request, render_template
from botocore.exceptions import NoCredentialsError
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

CACHE_FILE = 'cache.json'
CONFIG_FILE = 'config.json'

DEFAULT_CONFIG = {
    'S3_ENDPOINT': 'https://s3member.pajakku.com',
    'S3_ACCESS_KEY': 'optek',
    'S3_SECRET_KEY': 'Cr0tY93nak3n4k!',
    'SCAN_HOUR': 0,
    'SCAN_MINUTE': 0
}

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                conf = json.load(f)
                # Merge with defaults to ensure all keys exist
                for k, v in DEFAULT_CONFIG.items():
                    if k not in conf:
                        conf[k] = v
                return conf
        except Exception as e:
            print(f"Error loading config: {e}")
            return DEFAULT_CONFIG.copy()
    else:
        # Save default config if not exist
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()

def save_config(config_data):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config_data, f)

def get_s3_client():
    conf = load_config()
    return boto3.client(
        's3',
        endpoint_url=conf.get('S3_ENDPOINT'),
        aws_access_key_id=conf.get('S3_ACCESS_KEY'),
        aws_secret_access_key=conf.get('S3_SECRET_KEY'),
        region_name='us-east-1'
    )

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cache(data):
    with open(CACHE_FILE, 'w') as f:
        json.dump(data, f)

def calculate_all_storage():
    print(f"[{datetime.now()}] Starting background deep scan...")
    try:
        client = get_s3_client()
        response = client.list_buckets()
        buckets = response.get('Buckets', [])
        
        cache_data = load_cache()
        paginator = client.get_paginator('list_objects_v2')
        
        for b in buckets:
            bucket_name = b['Name']
            total_size = 0
            try:
                for page in paginator.paginate(Bucket=bucket_name):
                    contents = page.get('Contents', [])
                    for obj in contents:
                        total_size += obj['Size']
                cache_data[bucket_name] = total_size
            except Exception as e:
                print(f"Error deep scanning {bucket_name}: {e}")
                
        save_cache(cache_data)
        print(f"[{datetime.now()}] Finished background deep scan. Cache updated.")
    except Exception as e:
        print(f"[{datetime.now()}] Failed background deep scan: {e}")

# Initialize Background Scheduler
scheduler = BackgroundScheduler()
conf = load_config()
scheduler.add_job(
    func=calculate_all_storage, 
    trigger="cron", 
    hour=conf.get('SCAN_HOUR', 0), 
    minute=conf.get('SCAN_MINUTE', 0),
    id='deep_scan_job'
)
scheduler.start()

if not os.path.exists(CACHE_FILE):
    threading.Thread(target=calculate_all_storage, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/config', methods=['GET', 'POST'])
def handle_config():
    if request.method == 'POST':
        data = request.json
        # Basic validation
        if not all(k in data for k in ['S3_ENDPOINT', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'SCAN_HOUR', 'SCAN_MINUTE']):
            return jsonify({'error': 'Missing required fields'}), 400
        
        try:
            data['SCAN_HOUR'] = int(data['SCAN_HOUR'])
            data['SCAN_MINUTE'] = int(data['SCAN_MINUTE'])
        except:
            return jsonify({'error': 'SCAN_HOUR and SCAN_MINUTE must be integers'}), 400

        save_config(data)
        
        # Reschedule job
        scheduler.reschedule_job(
            'deep_scan_job', 
            trigger='cron', 
            hour=data['SCAN_HOUR'], 
            minute=data['SCAN_MINUTE']
        )
        return jsonify({'message': 'Configuration saved successfully', 'config': data})
    else:
        return jsonify(load_config())

@app.route('/api/cache')
def get_cache():
    return jsonify(load_cache())

@app.route('/api/buckets')
def get_buckets():
    try:
        client = get_s3_client()
        response = client.list_buckets()
        buckets = [{'name': b['Name']} for b in response.get('Buckets', [])]
        return jsonify({'buckets': buckets})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bucket/<bucket_name>')
def get_bucket_detail(bucket_name):
    try:
        mode = request.args.get('mode', 'fast')
        client = get_s3_client()
        paginator = client.get_paginator('list_objects_v2')
        latest_obj = None
        total_size = 0
        
        if mode == 'fast':
            root_folders = []
            try:
                resp = client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
                for p in resp.get('CommonPrefixes', []):
                    root_folders.append(p.get('Prefix'))
            except:
                pass
                
            prefixes_to_scan = ['snapshots/']
            for rf in root_folders:
                prefixes_to_scan.append(rf + 'wals/')
                
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
            status = 'healthy' if hours_ago < 48 else 'stale'
            
            # If manual deep scan is requested, update cache
            if mode == 'deep':
                cache_data = load_cache()
                cache_data[bucket_name] = total_size
                save_cache(cache_data)
                
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
        return jsonify({'name': bucket_name, 'error': str(e), 'status': 'error'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
