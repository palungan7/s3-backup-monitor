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

cache_lock = threading.Lock()

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
                for k, v in DEFAULT_CONFIG.items():
                    if k not in conf:
                        conf[k] = v
                return conf
        except Exception as e:
            print(f"Error loading config: {e}")
            return DEFAULT_CONFIG.copy()
    else:
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
                data = json.load(f)
                # Ensure the new format is respected
                if 'buckets' not in data:
                    return {'timestamp': datetime.now(timezone.utc).isoformat(), 'buckets': {}}
                return data
        except:
            return {'timestamp': datetime.now(timezone.utc).isoformat(), 'buckets': {}}
    return {'timestamp': datetime.now(timezone.utc).isoformat(), 'buckets': {}}

def save_cache(data):
    with open(CACHE_FILE, 'w') as f:
        json.dump(data, f)

def update_bucket_in_cache(bucket_name, data_dict):
    with cache_lock:
        cache = load_cache()
        if bucket_name not in cache['buckets']:
            cache['buckets'][bucket_name] = {}
        
        for k, v in data_dict.items():
            if v is not None:
                cache['buckets'][bucket_name][k] = v
                
        cache['timestamp'] = datetime.now(timezone.utc).isoformat()
        save_cache(cache)
        # Salin return value agar tidak mengakses dict yang mungkin berubah
        return cache['buckets'][bucket_name].copy()

def calculate_all_storage():
    print(f"[{datetime.now()}] Starting background deep scan...")
    try:
        client = get_s3_client()
        response = client.list_buckets()
        buckets = response.get('Buckets', [])
        
        paginator = client.get_paginator('list_objects_v2')
        # Buat temporary dictionary agar tidak mengunci cache selama scan berjam-jam
        temp_cache = {'buckets': {}}
        
        for b in buckets:
            bucket_name = b['Name']
            total_size = 0
            latest_obj = None
            try:
                for page in paginator.paginate(Bucket=bucket_name):
                    contents = page.get('Contents', [])
                    for obj in contents:
                        total_size += obj['Size']
                    if contents:
                        page_latest = max(contents, key=lambda obj: obj['LastModified'])
                        if not latest_obj or page_latest['LastModified'] > latest_obj['LastModified']:
                            latest_obj = page_latest
                
                # Update logic
                bucket_data = {'size': total_size}
                if latest_obj:
                    last_modified = latest_obj['LastModified']
                    now = datetime.now(timezone.utc)
                    delta = now - last_modified
                    hours_ago = delta.total_seconds() / 3600
                    status = 'healthy' if hours_ago < 48 else 'stale'
                    
                    bucket_data['last_update'] = last_modified.isoformat()
                    bucket_data['latest_file'] = latest_obj['Key']
                    bucket_data['status'] = status
                else:
                    bucket_data['status'] = 'empty'
                
                if bucket_name not in temp_cache['buckets']:
                    temp_cache['buckets'][bucket_name] = {}
                temp_cache['buckets'][bucket_name].update(bucket_data)
                
            except Exception as e:
                print(f"Error deep scanning {bucket_name}: {e}")
                
        with cache_lock:
            # Load cache terbaru, lalu merge dengan hasil scan
            final_cache = load_cache()
            for b_name, b_data in temp_cache['buckets'].items():
                if b_name not in final_cache['buckets']:
                    final_cache['buckets'][b_name] = {}
                final_cache['buckets'][b_name].update(b_data)
            
            final_cache['timestamp'] = datetime.now(timezone.utc).isoformat()
            save_cache(final_cache)
            
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
        if not all(k in data for k in ['S3_ENDPOINT', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'SCAN_HOUR', 'SCAN_MINUTE']):
            return jsonify({'error': 'Missing required fields'}), 400
        
        try:
            data['SCAN_HOUR'] = int(data['SCAN_HOUR'])
            data['SCAN_MINUTE'] = int(data['SCAN_MINUTE'])
        except:
            return jsonify({'error': 'SCAN_HOUR and SCAN_MINUTE must be integers'}), 400

        save_config(data)
        scheduler.reschedule_job(
            'deep_scan_job', 
            trigger='cron', 
            hour=data['SCAN_HOUR'], 
            minute=data['SCAN_MINUTE']
        )
        return jsonify({'message': 'Configuration saved successfully', 'config': data})
    else:
        return jsonify(load_config())

@app.route('/api/dashboard')
def get_dashboard():
    # Return the entire cache as the single source of truth
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
            
            bucket_data = {
                'name': bucket_name,
                'last_update': last_modified.isoformat(),
                'status': status,
                'latest_file': latest_obj['Key']
            }
            if mode == 'deep':
                bucket_data['size'] = total_size
            
            # Save to the backend cache immediately
            merged_data = update_bucket_in_cache(bucket_name, bucket_data)
            merged_data['name'] = bucket_name # ensure name is returned
            
            return jsonify(merged_data)
        else:
            bucket_data = {
                'name': bucket_name,
                'status': 'empty',
                'size': 0 if mode == 'deep' else None
            }
            merged_data = update_bucket_in_cache(bucket_name, bucket_data)
            merged_data['name'] = bucket_name
            return jsonify(merged_data)
    except Exception as e:
        bucket_data = {'name': bucket_name, 'error': str(e), 'status': 'error'}
        merged_data = update_bucket_in_cache(bucket_name, bucket_data)
        merged_data['name'] = bucket_name
        return jsonify(merged_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
